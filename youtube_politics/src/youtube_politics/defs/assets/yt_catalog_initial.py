"""Load the hand-maintained channel catalog into Snowflake.

Reads catalog/channels.yaml, looks up each channel on the YouTube Data API, and
appends the raw API response to RAW.YT_CATALOG_INITIAL. Only handles not loaded
within the last refresh_days are fetched, so routine runs cost little quota. This is the "EL" of
ELT: the data lands untouched as JSON, and dbt does the shaping later. That way
dbt never needs YouTube access, and re-modeling never costs API quota.
"""

from pathlib import Path

import dagster as dg
import yaml
from dagster_snowflake import SnowflakeResource

from youtube_politics.defs.resources import YouTubeResource
from youtube_politics.lib.snowflake_raw import load_raw, table_exists

# parents[2] walks up from defs/assets/ to the youtube_politics package folder.
# Building the path from __file__ means it works whatever directory dg runs in.
CATALOG_PATH = Path(__file__).resolve().parents[2] / "catalog" / "channels.yaml"

# Every part channels.list exposes to a plain API key (see .dev/pull_channel_sample.py).
# Requesting more parts costs no extra quota, so grab everything while we're here.
PARTS = [
    "id",
    "snippet",
    "contentDetails",  # relatedPlaylists.uploads: needed later to list a channel's videos
    "statistics",
    "status",
    "topicDetails",
    "brandingSettings",
    "localizations",
]

TABLE = "YT_CATALOG_INITIAL"


def load_catalog() -> list[dict]:
    """Read channels.yaml and return its list of entries, checking each has a handle."""
    with CATALOG_PATH.open() as f:
        # safe_load only builds plain types (dicts, lists, strings); plain
        # yaml.load can construct arbitrary Python objects, so avoid it.
        entries = yaml.safe_load(f)["channels"]
    for entry in entries:
        if not entry.get("handle"):
            raise ValueError(f"Catalog entry without a handle in {CATALOG_PATH}: {entry}")
    return entries


def current_channels(conn) -> list[tuple[str, str, str]]:
    """Latest stored row for every handle currently in channels.yaml.

    Returns (channel_id, handle, uploads_playlist_id) tuples. Downstream assets
    use this rather than "the catalog's latest run", because a run only
    re-fetches handles that are due (see refresh_days), so the latest run may
    hold just a few channels. Handles removed from the YAML drop out here even
    though their old rows stay in the table.
    """
    handles = [e["handle"].lower() for e in load_catalog()]
    # One %s per handle, e.g. "%s, %s, %s"; the connector fills them in safely.
    placeholders = ", ".join(["%s"] * len(handles))
    cur = conn.cursor()
    cur.execute(
        f"""
        select channel_id,
               handle,
               raw_data:contentDetails:relatedPlaylists:uploads::string
        from {TABLE}
        where lower(handle) in ({placeholders})
        -- QUALIFY filters on a window function, like HAVING does for GROUP BY:
        -- number each handle's rows newest-first and keep only row 1.
        qualify row_number() over (partition by lower(handle) order by loaded_at desc) = 1
        """,
        handles,
    )
    return cur.fetchall()


class CatalogConfig(dg.Config):
    """Run config, editable per run in the UI's Launchpad (shift+click Materialize)."""

    # Only call the API for handles whose last load is older than this, or that
    # have never been loaded (new YAML entries, renamed handles). Channel
    # metadata and subscriber counts change slowly, so weekly is plenty and
    # most runs cost ~0 units. Set to 0 to force a refresh of every handle.
    refresh_days: int = 7


@dg.asset(
    # Groups and kinds are only for organizing/labeling the asset in the UI.
    group_name="youtube_raw",
    kinds={"youtube", "snowflake"},
    description="Raw channels.list snapshots for channels in catalog/channels.yaml, refreshed every refresh_days.",
)
def yt_catalog_initial(
    context: dg.AssetExecutionContext,
    config: CatalogConfig,
    # Dagster matches these parameter names to the resource keys in resources.py
    # and passes the configured resources in. The type hints are what tell
    # Dagster these are resources rather than upstream assets.
    youtube: YouTubeResource,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    entries = load_catalog()

    # Step 1: ask Snowflake which handles were loaded recently enough to skip.
    # The threshold is 4 hours short of a whole number of days, so a run
    # scheduled at the same time each week isn't pushed to the next day by a
    # few minutes of drift (the same trick yt_videos uses).
    recent: set[str] = set()
    with snowflake.get_connection() as conn:
        if config.refresh_days > 0 and table_exists(conn, TABLE):
            cur = conn.cursor()
            cur.execute(
                f"""
                select lower(handle)
                from {TABLE}
                group by lower(handle)
                having max(loaded_at) >= dateadd(hour, -%s, current_timestamp())
                """,
                (config.refresh_days * 24 - 4,),
            )
            recent = {row[0] for row in cur.fetchall()}

    # Handles are case-insensitive on YouTube, so compare lowercased.
    due = [e for e in entries if e["handle"].lower() not in recent]
    context.log.info(
        f"{len(due)} of {len(entries)} handles due for refresh "
        f"({len(entries) - len(due)} loaded within {config.refresh_days} days)"
    )

    # Step 2: look up every due channel before touching Snowflake again, so a
    # typo in the YAML fails the run without leaving a half-loaded batch behind.
    client = youtube.get_client()
    rows = []
    missing = []
    empty = []  # handles that exist but have no public videos (likely wrong channel)
    for entry in due:
        handle = entry["handle"]
        # forHandle takes one handle per call (1 quota unit each). Unlike the
        # id parameter, it can't batch 50 at once.
        response = client.channels().list(part=",".join(PARTS), forHandle=handle).execute()
        # The API omits "items" entirely when nothing matches.
        items = response.get("items", [])
        if not items:
            missing.append(handle)
            continue
        # A handle can exist but belong to the wrong channel (e.g. @CharlieKirkShow
        # was an unrelated, empty channel called "Charlie"). Every channel we
        # track posts videos, so zero public videos is treated as a bad handle.
        if int(items[0]["statistics"].get("videoCount", 0)) == 0:
            title = items[0]["snippet"]["title"]
            empty.append(f'{handle} ("{title}")')
            continue
        # Keys match the columns passed to load_raw below.
        rows.append(
            {
                "handle": handle,
                "channel_id": items[0]["id"],
                "catalog_entry": entry,
                "raw_data": items[0],
            }
        )
        context.log.info(f"Fetched {handle} -> {items[0]['id']}")

    # Report both kinds of bad handle together, so one run lists every fix.
    problems = []
    if missing:
        problems.append(f"No YouTube channel found for: {', '.join(missing)}.")
    if empty:
        problems.append(f"Channel has no public videos (wrong channel?): {', '.join(empty)}.")
    if problems:
        raise dg.Failure(description=" ".join(problems) + f" Check the handles in {CATALOG_PATH.name}.")

    # Step 3: append the batch to Snowflake.
    # VARIANT is Snowflake's semi-structured type: it stores JSON as-is and
    # lets dbt query into it later, e.g. raw_data:snippet:title.
    # Appending (rather than replacing) keeps a history of every load;
    # downstream assets and dbt pick the latest row per channel by loaded_at.
    # load_raw does nothing when rows is empty (every handle was recent).
    with snowflake.get_connection() as conn:
        load_raw(
            conn,
            TABLE,
            columns={
                "handle": "varchar",
                "channel_id": "varchar",
                "catalog_entry": "variant",  # the YAML entry, incl. your custom keys
                "raw_data": "variant",  # the channels.list item, untouched
            },
            rows=rows,
            run_id=context.run_id,
        )

    # Metadata shows up on the asset's page in the UI for each materialization.
    return dg.MaterializeResult(
        metadata={
            "channels_loaded": len(rows),
            "channels_skipped_recent": len(entries) - len(due),
            # One channels.list call per due handle, found or not (1 unit each).
            # Same key as the other YouTube assets, so quota is comparable
            # across them in the UI's metadata plots.
            "quota_units_used": len(due),
            "refresh_days": config.refresh_days,
            "table": f"{snowflake.database}.{snowflake.schema_}.{TABLE}",
            "catalog_file": str(CATALOG_PATH),
        }
    )
