"""List every catalog channel's uploads published since the analysis start date.

Uses each channel's uploads playlist (from yt_catalog_initial), which lists
every *public* upload, newest first, at 50 videos per call (1 quota unit).
Appends one row per upload to RAW.YT_UPLOADS. Full video details and stats
come from yt_videos; this asset only answers "which videos exist?".

Two modes per channel, decided automatically:
  - BACKFILL: the channel has never been fully pulled back to published_after.
    Page through the whole uploads playlist until videos get older than that.
    A big network can take ~1,000+ units, so backfill can span several days.
  - INCREMENTAL: the backfill is done. Page only until reaching videos we've
    already stored (usually the first page, 1 unit).

Backfill completion is recorded per channel in RAW.YT_UPLOADS_BACKFILL as soon
as that channel finishes, so a run that stops early (unit budget reached, or a
crash) picks up only the remaining channels next time.

Known limit: the uploads list stops at a channel's ~20,000 most recent videos.
For big networks that's well short of 2020; such channels are recorded with
truncated = true and the oldest date actually reached.
"""

from datetime import datetime

import dagster as dg
from dagster_snowflake import SnowflakeResource
from googleapiclient.errors import HttpError

from youtube_politics.defs.assets.yt_catalog_initial import current_channels
from youtube_politics.defs.resources import YouTubeResource
from youtube_politics.lib.snowflake_raw import load_raw, table_exists

# playlistItems.list parts available to an API key.
#   snippet:        title, description, position, date added to the playlist
#   contentDetails: videoId, videoPublishedAt
#   status:         privacyStatus
PARTS = ["id", "snippet", "contentDetails", "status"]

TABLE = "YT_UPLOADS"
STATE_TABLE = "YT_UPLOADS_BACKFILL"

# The API only ever lists a channel's ~20,000 most recent uploads, however far
# you page. Big news networks pass that within a year or two (observed:
# ABC News, CBS, AP, BBC all stopped at exactly 20,000).
UPLOADS_LIST_CAP = 20_000


class UploadsConfig(dg.Config):
    """Run config, editable per run in the UI's Launchpad (shift+click Materialize)."""

    # Start of the analysis window (ISO date, e.g. "2020-01-01"). Uploads older
    # than this are never stored. Moving it earlier later is fine: channels
    # whose backfill only reached a later date are treated as not backfilled
    # and redone.
    #
    # Deliberately no default: the window is an analysis decision, so it's set
    # by the job that runs this asset (see defs/jobs.py), not baked in here.
    # Materializing the asset on its own makes the Launchpad ask for it.
    published_after: str

    # Quota budget for this asset per run. The daily limit is 10,000 units
    # shared by ALL assets (and resets at midnight Pacific), so this leaves
    # room for yt_videos. The budget is checked before starting each channel,
    # so a run can overshoot by up to one channel's pages.
    max_units: int = 4000


@dg.asset(
    group_name="youtube_raw",
    kinds={"youtube", "snowflake"},
    deps=["yt_catalog_initial"],
    description="Every public upload since published_after for each catalog channel (uploads playlist items).",
)
def yt_uploads(
    context: dg.AssetExecutionContext,
    config: UploadsConfig,
    youtube: YouTubeResource,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    # fromisoformat needs a timezone to compare against the API's "...Z"
    # timestamps, so treat the configured date as midnight UTC.
    cutoff = datetime.fromisoformat(config.published_after + "T00:00:00+00:00")

    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        # Latest stored row per handle in channels.yaml (not just the catalog's
        # latest run, which only holds the handles that were due a refresh).
        channels = current_channels(conn)

        # Channels whose backfill already reached back to (at least) the
        # configured date. Empty on the very first run.
        done = set()
        if table_exists(conn, STATE_TABLE):
            cur.execute(
                f"select distinct channel_id from {STATE_TABLE} where published_after <= %s",
                (config.published_after,),
            )
            done = {row[0] for row in cur.fetchall()}

        # Every video ID already stored, per channel, so incremental runs know
        # where to stop. A set of strings: a few MB even for ~1M videos.
        known: dict[str, set[str]] = {}
        if table_exists(conn, TABLE):
            cur.execute(f"select distinct channel_id, video_id from {TABLE}")
            for channel_id, video_id in cur.fetchall():
                known.setdefault(channel_id, set()).add(video_id)

    # Cheap incremental channels first, so routine updates always happen even
    # while a long backfill is still eating the budget. sorted() on a bool key
    # puts False (= already done) before True (= still needs backfill).
    channels.sort(key=lambda c: c[0] not in done)

    client = youtube.get_client()
    units = 0
    new_rows_total = 0
    finished_backfills = 0
    truncated = []
    skipped = []

    for channel_id, handle, uploads_id in channels:
        if units >= config.max_units:
            # Out of budget: leave the rest for the next run.
            skipped.append(handle)
            continue

        mode = "incremental" if channel_id in done else "backfill"
        seen = known.get(channel_id, set())
        rows = []
        items_read = 0  # every item the API returned, kept or not (for the cap check)
        oldest = None  # oldest publish date reached, for the backfill record
        reached_cutoff = False  # True once we've passed published_after
        read_whole_playlist = False

        try:
            for page in youtube.pages(
                client.playlistItems(), part=",".join(PARTS), playlistId=uploads_id, maxResults=50
            ):
                units += 1
                items_read += len(page)
                page_has_known = False
                for item in page:
                    video_id = item["contentDetails"]["videoId"]
                    # videoPublishedAt is missing on private-video placeholders;
                    # those can't be fetched by yt_videos anyway, so skip them.
                    published = item["contentDetails"].get("videoPublishedAt")
                    if published is None:
                        continue
                    if datetime.fromisoformat(published.replace("Z", "+00:00")) < cutoff:
                        # Newest-first, so everything after this is older still.
                        reached_cutoff = True
                        break
                    oldest = published
                    if video_id in seen:
                        page_has_known = True
                        continue  # already stored; don't duplicate it
                    rows.append(
                        {
                            "channel_id": channel_id,
                            "video_id": video_id,
                            "published_at": published,
                            "raw_data": item,
                        }
                    )
                if reached_cutoff:
                    break
                # Incremental: once a page contains videos we already have, the rest
                # of the playlist is already stored. Finishing the page (rather than
                # stopping at the first known ID) tolerates small reorderings, e.g.
                # a premiere that went live after videos uploaded around it.
                if mode == "incremental" and page_has_known:
                    break
            else:
                # for/else: runs only if the loop wasn't broken, i.e. we read
                # every page the API would give us.
                read_whole_playlist = True
        except HttpError as e:
            # A channel with no public uploads has an uploads playlist ID that
            # returns 404 playlistNotFound. In a hand-curated catalog that almost
            # always means the handle points at the wrong channel, so fail loudly
            # with a readable message instead of the raw HttpError. (Normally
            # yt_catalog_initial catches this first via videoCount == 0.)
            # Channels finished earlier in this run are already recorded, so
            # the next run won't redo them.
            if e.resp.status == 404:
                raise dg.Failure(
                    description=f"{handle} ({channel_id}) has no uploads playlist: the channel has "
                    "no public uploads. Check the handle in channels.yaml."
                ) from e
            raise

        # The uploads playlist only ever returns the most recent ~20,000 items.
        # If we read to its end and got that many, older uploads exist that the
        # API won't list: the channel's history is truncated before `oldest`.
        is_truncated = read_whole_playlist and items_read >= UPLOADS_LIST_CAP
        if is_truncated:
            truncated.append(handle)
            context.log.warning(
                f"{handle}: uploads list capped at {UPLOADS_LIST_CAP:,} items; "
                f"only reaches back to {oldest}, not {config.published_after}"
            )

        with snowflake.get_connection() as conn:
            if rows:
                load_raw(
                    conn,
                    TABLE,
                    columns={
                        "channel_id": "varchar",
                        "video_id": "varchar",
                        "published_at": "timestamp_tz",
                        "raw_data": "variant",
                    },
                    rows=rows,
                    run_id=context.run_id,
                )
            # Record a finished backfill right away, per channel, so a crash
            # later in the run doesn't make the next run redo this channel.
            # It's written after the uploads above, so "done" is never recorded
            # for uploads that failed to load.
            if mode == "backfill" and (reached_cutoff or read_whole_playlist):
                load_raw(
                    conn,
                    STATE_TABLE,
                    columns={
                        "channel_id": "varchar",
                        "published_after": "date",
                        # Oldest upload actually reached, and whether the 20k
                        # cap cut the history short: tells dbt/analysis where
                        # each channel's coverage really starts.
                        "oldest_published_at": "timestamp_tz",
                        "truncated": "boolean",
                    },
                    rows=[
                        {
                            "channel_id": channel_id,
                            "published_after": config.published_after,
                            "oldest_published_at": oldest,
                            "truncated": is_truncated,
                        }
                    ],
                    run_id=context.run_id,
                )
                finished_backfills += 1
        new_rows_total += len(rows)
        context.log.info(f"{handle} [{mode}]: {len(rows)} new uploads ({units} units used so far)")

    if skipped:
        context.log.warning(
            f"Unit budget ({config.max_units}) reached; {len(skipped)} channels left for the next run: "
            + ", ".join(skipped)
        )

    return dg.MaterializeResult(
        metadata={
            "quota_units_used": units,
            "new_uploads": new_rows_total,
            "backfills_completed": finished_backfills,
            "channels_backfilled_total": len(done) + finished_backfills,
            "channels_total": len(channels),
            "channels_skipped_for_budget": len(skipped),
            # Channels whose history stops short of published_after because of
            # the API's 20k cap on the uploads list.
            "channels_truncated": truncated,
            "published_after": config.published_after,
        }
    )
