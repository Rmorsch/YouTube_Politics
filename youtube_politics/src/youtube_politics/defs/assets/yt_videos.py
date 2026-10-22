"""Snapshot full public details + stats for uploaded videos, refreshing by age.

Picks video IDs from RAW.YT_UPLOADS (yt_uploads), calls videos.list for them
in batches of 50 (1 quota unit per batch), and appends one row per video to
RAW.YT_VIDEOS. Each row is a snapshot, so dbt can split them into
  - videos           (latest row per video: title, duration, live stream times...)
  - video_snapshots  (every row: view/like/comment counts and privacy over time)

Which videos get fetched, in priority order:
  1. never fetched before (new uploads, or the backfill)
  2. younger than daily_refresh_days and not fetched in ~a day
  3. younger than weekly_refresh_days and not fetched in ~a week
Older videos aren't refreshed: views have mostly settled by then, and
re-fetching hundreds of thousands of old videos daily would blow the quota.
"""

import dagster as dg
from dagster_snowflake import SnowflakeResource

from youtube_politics.defs.resources import YouTubeResource
from youtube_politics.lib.snowflake_raw import load_raw, table_exists

# Every videos.list part available to an API key (see .dev/pull_youtube_sample.py),
# minus "player" (embed HTML). fileDetails/processingDetails/suggestions are
# owner-only and need OAuth.
PARTS = [
    "id",
    "snippet",
    "contentDetails",
    "statistics",
    "status",
    "topicDetails",
    "recordingDetails",
    "liveStreamingDetails",  # only present on streams/premieres: the key streamer field
    "localizations",
    "paidProductPlacementDetails",
]

TABLE = "YT_VIDEOS"

# videos.list accepts up to 50 comma-separated IDs per call, for 1 quota unit.
BATCH_SIZE = 50

# Write to Snowflake every this many videos, so memory stays flat during a
# big backfill (a video row is a few KB of JSON) and a crash mid-run keeps
# everything fetched up to the last chunk.
CHUNK_SIZE = 5000


class VideosConfig(dg.Config):
    """Run config, editable per run in the UI's Launchpad (shift+click Materialize)."""

    # Quota budget for this asset per run (1 unit = 50 videos). Together with
    # yt_uploads' max_units this should stay under the 10,000/day limit.
    max_units: int = 5000

    # Refresh tiers, by video age (days since publish).
    daily_refresh_days: int = 14
    weekly_refresh_days: int = 90


@dg.asset(
    group_name="youtube_raw",
    kinds={"youtube", "snowflake"},
    deps=["yt_uploads"],
    description="Snapshots of videos.list (details + stats) for new and recently published uploads.",
)
def yt_videos(
    context: dg.AssetExecutionContext,
    config: VideosConfig,
    youtube: YouTubeResource,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    max_videos = config.max_units * BATCH_SIZE

    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        # Before the first yt_videos run there's no YT_VIDEOS table to compare
        # against, so every upload counts as never fetched.
        if table_exists(conn, TABLE):
            last_fetch = f"(select video_id, max(loaded_at) as last_loaded from {TABLE} group by video_id)"
        else:
            last_fetch = "(select null::varchar as video_id, null::timestamp_tz as last_loaded where false)"

        # One row per upload that's due, with its priority tier.
        # - LEFT JOIN keeps uploads with no YT_VIDEOS row (f.video_id is null).
        # - The -20 hour / -6 day thresholds are slightly under a day/week so a
        #   run scheduled at the same time each day doesn't miss videos fetched
        #   a few minutes "less than a day" ago.
        # - %(name)s placeholders are filled in safely by the connector.
        cur.execute(
            f"""
            with uploads as (
                select video_id, max(published_at) as published_at
                from YT_UPLOADS
                group by video_id
            ),
            due as (
                select u.video_id,
                       u.published_at,
                       case
                           when f.video_id is null then 1
                           when u.published_at >= dateadd(day, -%(daily)s, current_timestamp())
                                and f.last_loaded < dateadd(hour, -20, current_timestamp()) then 2
                           when u.published_at >= dateadd(day, -%(weekly)s, current_timestamp())
                                and f.last_loaded < dateadd(day, -6, current_timestamp()) then 3
                       end as tier
                from uploads u
                left join {last_fetch} f on f.video_id = u.video_id
            )
            select video_id, tier
            from due
            where tier is not null
            -- Highest priority first; within a tier, newest first.
            order by tier, published_at desc
            limit %(limit)s
            """,
            {
                "daily": config.daily_refresh_days,
                "weekly": config.weekly_refresh_days,
                "limit": max_videos,
            },
        )
        due = cur.fetchall()

    video_ids = [row[0] for row in due]
    tier_counts = {"new": 0, "daily_refresh": 0, "weekly_refresh": 0}
    for _, tier in due:
        tier_counts[["new", "daily_refresh", "weekly_refresh"][tier - 1]] += 1
    context.log.info(f"{len(video_ids)} videos due: {tier_counts}")

    client = youtube.get_client()
    units = 0
    loaded = 0
    pending: list[dict] = []

    def flush():
        """Write the pending chunk to Snowflake and clear it."""
        nonlocal loaded  # lets this inner function update the outer variable
        if pending:
            with snowflake.get_connection() as conn:
                load_raw(
                    conn,
                    TABLE,
                    columns={"video_id": "varchar", "raw_data": "variant"},
                    rows=pending,
                    run_id=context.run_id,
                )
            loaded += len(pending)
            pending.clear()

    # range(0, n, 50) gives 0, 50, 100, ...; each slice is one API call.
    for start in range(0, len(video_ids), BATCH_SIZE):
        batch = video_ids[start : start + BATCH_SIZE]
        response = client.videos().list(part=",".join(PARTS), id=",".join(batch)).execute()
        units += 1
        pending.extend({"video_id": v["id"], "raw_data": v} for v in response.get("items", []))
        if len(pending) >= CHUNK_SIZE:
            flush()
            context.log.info(f"{loaded} videos loaded ({units} units used)")
    flush()

    # Videos made private or deleted since they were uploaded come back empty
    # from videos.list, so they're simply absent from this batch. Unlisted
    # videos DO come back, because anyone with the ID can view them.
    unavailable = len(video_ids) - loaded
    context.log.info(f"Fetched {loaded} of {len(video_ids)} videos ({unavailable} private/deleted)")

    return dg.MaterializeResult(
        metadata={
            "quota_units_used": units,
            "videos_loaded": loaded,
            "unavailable": unavailable,
            # True means more videos were due than the budget allowed; the
            # remainder (lowest priority first) waits for the next run.
            "budget_limited": len(video_ids) == max_videos,
            **tier_counts,
        }
    )
