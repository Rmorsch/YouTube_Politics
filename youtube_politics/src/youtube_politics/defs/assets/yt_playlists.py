"""Crawl each catalog channel's own (curated) playlists, once per channel.

Why: yt_uploads reads the auto-generated uploads playlists, which only ever
list a channel's ~20,000 most recent videos. Big networks pass that within a
couple of years, so their history before ~2023 is invisible there. But
channels also curate their own playlists (per show, per topic, per event),
and each of those is a separate list that isn't subject to that cap.

Reading them is cheap (playlists.list and playlistItems.list are 1 unit per 50
items), so this runs for EVERY channel the first time it appears in the
catalog. Measured on PBS NewsHour (.dev/probe_playlist_coverage.py): ~640
units for its 273 playlists, recovering ~1,600+ videos from 2020-2023 that
the uploads list can't reach. Expect partial, event-heavy coverage (hearings,
conventions, debates), not a complete history. That's what yt_search_backfill
is for.

For channels under the cap, the uploads list already has every video, so
this adds no new videos. What it does add is playlist membership ("which show
or series is this video part of?"), useful for analysis either way.

Writes three tables:
  YT_PLAYLISTS        one row per playlist (raw playlists.list item: title...)
  YT_PLAYLIST_ITEMS   one row per (playlist, video) published since published_after
  YT_PLAYLISTS_CRAWL  one row per finished channel crawl (the progress record)

yt_videos picks up video IDs from YT_PLAYLIST_ITEMS as well as YT_UPLOADS.
"""

from datetime import datetime

import dagster as dg
from dagster_snowflake import SnowflakeResource
from googleapiclient.errors import HttpError

from youtube_politics.defs.assets.yt_catalog_initial import current_channels
from youtube_politics.defs.resources import YouTubeResource
from youtube_politics.lib.snowflake_raw import load_raw, table_exists

PLAYLISTS_TABLE = "YT_PLAYLISTS"
ITEMS_TABLE = "YT_PLAYLIST_ITEMS"
STATE_TABLE = "YT_PLAYLISTS_CRAWL"

# Every playlists.list part an API key can read, minus "player" (embed HTML).
PLAYLIST_PARTS = ["id", "snippet", "contentDetails", "status", "localizations"]
# Same parts yt_uploads requests for playlistItems.list.
ITEM_PARTS = ["id", "snippet", "contentDetails", "status"]


class PlaylistsConfig(dg.Config):
    """Run config, editable per run in the UI's Launchpad (shift+click Materialize)."""

    # Start of the analysis window (ISO date). Playlist items for videos
    # published before this aren't stored. Set by the job (see defs/jobs.py),
    # like yt_uploads, so there's no default here.
    published_after: str

    # Quota budget per run. Checked before starting each channel, so a run can
    # overshoot by one channel's crawl (~650 units for a big network). The
    # first pass over the whole catalog takes several runs; after that, only
    # newly added channels cost anything.
    max_units: int = 1000

    # Re-crawl a channel this many days after its last crawl, to pick up
    # playlists created since. 0 = never (crawl each channel only once).
    # Recent videos are covered by yt_uploads anyway, so re-crawling mainly
    # refreshes playlist membership.
    recrawl_days: int = 0


@dg.asset(
    group_name="youtube_raw",
    kinds={"youtube", "snowflake"},
    deps=["yt_catalog_initial"],
    description="Each catalog channel's curated playlists and their videos since published_after "
    "(crawled once per channel). Recovers some history past the uploads list's 20k cap.",
)
def yt_playlists(
    context: dg.AssetExecutionContext,
    config: PlaylistsConfig,
    youtube: YouTubeResource,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    cutoff = datetime.fromisoformat(config.published_after + "T00:00:00+00:00")

    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        channels = current_channels(conn)

        # Channels crawled recently enough to skip. With recrawl_days = 0 that's
        # every channel ever crawled; otherwise only those crawled within the
        # window. (No crawl table yet = first run = nothing to skip.)
        crawled: set[str] = set()
        if table_exists(conn, STATE_TABLE):
            if config.recrawl_days > 0:
                cur.execute(
                    f"""
                    select channel_id from {STATE_TABLE}
                    group by channel_id
                    having max(loaded_at) >= dateadd(day, -%s, current_timestamp())
                    """,
                    (config.recrawl_days,),
                )
            else:
                cur.execute(f"select distinct channel_id from {STATE_TABLE}")
            crawled = {row[0] for row in cur.fetchall()}

        # (playlist_id, video_id) pairs already stored, so a crawl that was cut
        # short by the daily quota (and saved its partial results) doesn't
        # store them twice when it's redone, and re-crawls only add new items.
        known: set[tuple[str, str]] = set()
        if table_exists(conn, ITEMS_TABLE):
            cur.execute(f"select distinct playlist_id, video_id from {ITEMS_TABLE}")
            known = {(row[0], row[1]) for row in cur.fetchall()}

    due = [c for c in channels if c[0] not in crawled]
    context.log.info(f"{len(due)} of {len(channels)} channels due for a playlist crawl")

    client = youtube.get_client()
    units = 0
    channels_done = 0
    items_total = 0
    gap_videos_total = 0  # stored videos older than what yt_uploads reached, see below
    skipped = []

    for channel_id, handle, _uploads_id, _video_count in due:
        if units >= config.max_units:
            skipped.append(handle)
            continue

        playlist_rows = []
        item_rows = []
        quota_exhausted = False
        oldest_seen = None
        try:
            # Step 1: every public playlist on the channel. This only lists the
            # channel's curated playlists, not the auto-generated UU... ones.
            playlists = []
            for page in youtube.pages(
                client.playlists(), part=",".join(PLAYLIST_PARTS), channelId=channel_id, maxResults=50
            ):
                units += 1
                playlists.extend(page)
            playlist_rows = [
                {"channel_id": channel_id, "playlist_id": p["id"], "raw_data": p} for p in playlists
            ]

            # Step 2: every item of every playlist. Unlike the uploads list,
            # curated playlists are in whatever order the channel arranged them,
            # not newest first, so there's no early stop: each is read in full.
            for p in playlists:
                for page in youtube.pages(
                    client.playlistItems(), part=",".join(ITEM_PARTS), playlistId=p["id"], maxResults=50
                ):
                    units += 1
                    for item in page:
                        video_id = item["contentDetails"]["videoId"]
                        published = item["contentDetails"].get("videoPublishedAt")
                        # Private/deleted placeholders have no publish date and
                        # can't be fetched by yt_videos; skip them.
                        if published is None:
                            continue
                        if datetime.fromisoformat(published.replace("Z", "+00:00")) < cutoff:
                            continue  # before the analysis window
                        # Curated playlists can include OTHER channels' videos
                        # (e.g. a debate playlist mixing several networks' feeds).
                        # Those belong to channels we may not track, so only keep
                        # the channel's own videos.
                        if item["snippet"].get("videoOwnerChannelId") != channel_id:
                            continue
                        if (p["id"], video_id) in known:
                            continue
                        known.add((p["id"], video_id))
                        item_rows.append(
                            {
                                "channel_id": channel_id,
                                "playlist_id": p["id"],
                                "video_id": video_id,
                                "published_at": published,
                                "raw_data": item,
                            }
                        )
                        if oldest_seen is None or published < oldest_seen:
                            oldest_seen = published
        except HttpError as e:
            # Daily quota used up: save what this channel produced so far (the
            # `known` check above stops duplicates on the retry), but don't
            # record the crawl as finished, so the next run redoes it.
            if e.resp.status == 403 and b"quotaExceeded" in e.content:
                quota_exhausted = True
            else:
                raise

        with snowflake.get_connection() as conn:
            # Playlist metadata is only saved on a finished crawl. A partial
            # crawl is redone in full, which re-lists the playlists anyway.
            if playlist_rows and not quota_exhausted:
                load_raw(
                    conn,
                    PLAYLISTS_TABLE,
                    columns={"channel_id": "varchar", "playlist_id": "varchar", "raw_data": "variant"},
                    rows=playlist_rows,
                    run_id=context.run_id,
                )
            if item_rows:
                load_raw(
                    conn,
                    ITEMS_TABLE,
                    columns={
                        "channel_id": "varchar",
                        "playlist_id": "varchar",
                        "video_id": "varchar",
                        "published_at": "timestamp_tz",
                        "raw_data": "variant",
                    },
                    rows=item_rows,
                    run_id=context.run_id,
                )
            if not quota_exhausted:
                # Written last, so a crawl is never marked done unless its items
                # were saved first.
                load_raw(
                    conn,
                    STATE_TABLE,
                    columns={
                        "channel_id": "varchar",
                        "published_after": "date",
                        "playlists": "int",
                        "items_stored": "int",
                        "oldest_published_at": "timestamp_tz",
                    },
                    rows=[
                        {
                            "channel_id": channel_id,
                            "published_after": config.published_after,
                            "playlists": len(playlist_rows),
                            "items_stored": len(item_rows),
                            "oldest_published_at": oldest_seen,
                        }
                    ],
                    run_id=context.run_id,
                )
                channels_done += 1

            # How many stored videos fall before what yt_uploads reached for
            # this channel: the videos this crawl actually recovered. Only
            # meaningful for truncated channels; for the rest it's 0.
            gap = 0
            if item_rows and table_exists(conn, "YT_UPLOADS_BACKFILL"):
                cur = conn.cursor()
                cur.execute(
                    "select min(oldest_published_at) from YT_UPLOADS_BACKFILL "
                    "where channel_id = %s and truncated",
                    (channel_id,),
                )
                # timestamp_tz comes back as a timezone-aware datetime, so it
                # compares directly with the parsed API timestamps.
                reached = cur.fetchone()[0]
                if reached is not None:
                    gap = len(
                        {
                            r["video_id"]
                            for r in item_rows
                            if datetime.fromisoformat(r["published_at"].replace("Z", "+00:00")) < reached
                        }
                    )
        gap_videos_total += gap
        items_total += len(item_rows)
        context.log.info(
            f"{handle}: {len(playlist_rows)} playlists, {len(item_rows)} items stored"
            + (f", {gap} videos older than its uploads list reaches" if gap else "")
            + f" ({units} units used so far)"
        )

        if quota_exhausted:
            raise dg.Failure(
                description=f"YouTube daily quota exhausted during {handle} after {units} units this run. "
                f"Partial items saved; {handle} is re-crawled next run. Quota resets at midnight Pacific."
            )

    if skipped:
        context.log.warning(
            f"Unit budget ({config.max_units}) reached; {len(skipped)} channels left for the next run: "
            + ", ".join(skipped)
        )

    return dg.MaterializeResult(
        metadata={
            "quota_units_used": units,
            "channels_crawled": channels_done,
            "channels_skipped_for_budget": len(skipped),
            "items_stored": items_total,
            "videos_recovered_past_cap": gap_videos_total,
            "published_after": config.published_after,
        }
    )
