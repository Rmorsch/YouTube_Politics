"""Slowly fill truncated channels' history with search.list, window by window.

The heavy lift. yt_uploads can only list a channel's ~20,000 most recent
videos; for big networks (truncated = true in YT_UPLOADS_BACKFILL) that leaves
a gap between published_after and the oldest upload it reached. search.list
can filter one channel's videos by publish date, so walking the gap in small
date windows lists (most of) what's in it.

Cost, and why this runs slowly on its own job:
  search.list = 100 quota units per page of up to 50 results, vs 1 unit for
  playlistItems. A network posting ~25 videos/day has ~30,000 videos in a
  2020-2023 gap: ~650 pages, ~65,000 units, i.e. a week of the whole daily
  quota per network. So this asset spends a fixed budget per run and resumes
  where it stopped; finishing every network takes weeks to months.

How it walks:
  - One channel at a time (finish one before starting the next), so finished
    channels are complete and usable rather than every channel half done.
  - Newest window first, moving back towards published_after, so the period
    closest to the uploads list's coverage fills in first.
  - Each finished window is recorded in YT_SEARCH_BACKFILL together with its
    results, so a stopped run loses at most the window in progress.

Caveats:
  - A single search query returns at most ~500 results however you page, so
    windows must stay well under 500 videos. If a window comes back with that
    many, the asset logs a warning: lower window_days.
  - Search is not guaranteed to be exhaustive (YouTube can leave videos out of
    search results), so treat the result as near-complete, not complete.
  - Search results only carry snippet (title, publish date...). yt_videos
    fetches the full details for these IDs like any other video.

Writes YT_SEARCH_RESULTS (one row per video found) and YT_SEARCH_BACKFILL
(one row per finished window). yt_videos reads video IDs from
YT_SEARCH_RESULTS too.
"""

from datetime import datetime, timedelta, timezone

import dagster as dg
from dagster_snowflake import SnowflakeResource
from googleapiclient.errors import HttpError

from youtube_politics.defs.assets.yt_catalog_initial import current_channels
from youtube_politics.defs.resources import YouTubeResource
from youtube_politics.lib.snowflake_raw import load_raw, table_exists

RESULTS_TABLE = "YT_SEARCH_RESULTS"
STATE_TABLE = "YT_SEARCH_BACKFILL"
UPLOADS_STATE_TABLE = "YT_UPLOADS_BACKFILL"

UNITS_PER_SEARCH = 100
# search.list stops returning results after about this many per query.
SEARCH_RESULT_LIMIT = 500


class SearchBackfillConfig(dg.Config):
    """Run config, editable per run in the UI's Launchpad (shift+click Materialize)."""

    # Start of the analysis window (ISO date): the walk stops here. Set by the job.
    published_after: str

    # Quota budget per run. 100 units per search page, so the default is 40
    # pages (up to ~2,000 videos). This shares the 10,000/day limit with every
    # other asset; raise it on days the main job doesn't need much.
    max_units: int = 4000

    # Width of each search window, in days. Smaller windows waste more on
    # partly filled last pages; larger ones risk passing the ~500 results a
    # query can return. 7 days fits networks posting up to ~70 videos/day.
    window_days: int = 7

    # Limit to these handles (e.g. ["@PBSNewsHour"]). Empty = every truncated
    # channel, in catalog order.
    handles: list[str] = []


@dg.asset(
    group_name="youtube_raw",
    kinds={"youtube", "snowflake"},
    deps=["yt_uploads"],
    description="search.list backfill of the history the uploads list can't reach for truncated "
    "channels, walked back to published_after in date windows over many runs.",
)
def yt_search_backfill(
    context: dg.AssetExecutionContext,
    config: SearchBackfillConfig,
    youtube: YouTubeResource,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    floor = datetime.fromisoformat(config.published_after + "T00:00:00+00:00")
    wanted = {h.lower() for h in config.handles}

    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        channels = current_channels(conn)

        # Where each truncated channel's uploads coverage starts: the gap to
        # fill runs from published_after up to here.
        # The 20k cap is one window shared by all formats: the API shows the
        # channel's 20,000 most recent uploads, and the long/short/live lists
        # split that window between them (their itemCounts add up to ~20,000).
        # So the window starts at the OLDEST video any format list reached,
        # i.e. MIN over the channel's rows. A format whose own oldest video is
        # newer (BBC's live list starts 2026-06, its long list 2021-08) simply
        # had no videos of that format in between; it isn't missing any.
        # Rows from before playlist_id existed (read from the all-uploads UU
        # list) count too: their videos are in YT_UPLOADS, and UU can reach
        # further back than the format lists (CNN: UU got to 2024-11-28, the
        # format lists only to 2025-01-07). Ignoring them meant 200 units spent
        # on a first test window that found 0 new videos.
        gap_end: dict[str, datetime] = {}
        if table_exists(conn, UPLOADS_STATE_TABLE):
            cur.execute(
                f"""
                select channel_id, min(oldest_published_at)
                from {UPLOADS_STATE_TABLE}
                where truncated
                group by channel_id
                """
            )
            gap_end = {row[0]: row[1] for row in cur.fetchall()}

        # Progress: the oldest window start finished so far, per channel. The
        # walk resumes from there, moving further back.
        resume_from: dict[str, datetime] = {}
        if table_exists(conn, STATE_TABLE):
            cur.execute(f"select channel_id, min(window_start) from {STATE_TABLE} group by channel_id")
            resume_from = {row[0]: row[1] for row in cur.fetchall()}

        # Every video ID already known for these channels from any source, so
        # results aren't stored twice. (Searches still return them and still
        # cost units: there's no way to exclude IDs from a search.)
        known: set[str] = set()
        for table in ("YT_UPLOADS", "YT_PLAYLIST_ITEMS", RESULTS_TABLE):
            if gap_end and table_exists(conn, table):
                cur.execute(
                    f"select distinct video_id from {table} "
                    f"where channel_id in ({', '.join(['%s'] * len(gap_end))})",
                    list(gap_end),
                )
                known.update(row[0] for row in cur.fetchall())

    # Truncated channels still in the catalog (and in `handles`, if given)
    # whose walk hasn't reached published_after yet.
    todo = []
    for channel_id, handle, _uploads_id, _video_count in channels:
        if channel_id not in gap_end:
            continue
        if wanted and handle.lower() not in wanted:
            continue
        # Start from whichever is further back: where the last finished window
        # began, or where uploads coverage starts. (Uploads coverage can move
        # back after windows were searched, e.g. once older records count.)
        position = min(resume_from.get(channel_id, gap_end[channel_id]), gap_end[channel_id])
        if position > floor:
            todo.append((channel_id, handle, position))
    context.log.info(
        f"{len(todo)} truncated channels with history left to search: "
        + ", ".join(f"{h} (back to {p.date()})" for _, h, p in todo)
    )

    client = youtube.get_client()
    units = 0
    windows_done = 0
    new_total = 0
    full_windows = []  # windows that hit the ~500 result limit
    quota_exhausted = False
    finished_channels = []

    for channel_id, handle, position in todo:
        if quota_exhausted:
            break
        # The window walks backwards: [window_start, window_end) with
        # window_end = where the previous window started.
        window_end = position
        while window_end > floor:
            # Budget is checked before each window, like the other assets check
            # before each channel: a started window is always finished (a
            # half-read one would have to be redone, wasting its units), so a
            # run can overshoot by one window's pages (typically 2-5, i.e.
            # 200-500 units).
            if units + UNITS_PER_SEARCH > config.max_units:
                break
            window_start = max(floor, window_end - timedelta(days=config.window_days))

            rows = []
            found = 0
            pages = 0
            try:
                for page in youtube.pages(
                    client.search(),
                    part="snippet",
                    channelId=channel_id,
                    type="video",  # not playlists or the channel itself
                    order="date",
                    # RFC 3339 with a Z, as the API requires.
                    publishedAfter=window_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    publishedBefore=window_end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    maxResults=50,
                ):
                    units += UNITS_PER_SEARCH
                    pages += 1
                    for item in page:
                        found += 1
                        video_id = item["id"]["videoId"]
                        if video_id in known:
                            continue
                        known.add(video_id)
                        rows.append(
                            {
                                "channel_id": channel_id,
                                "video_id": video_id,
                                "published_at": item["snippet"]["publishedAt"],
                                "raw_data": item,
                            }
                        )
            except HttpError as e:
                if e.resp.status == 403 and b"quotaExceeded" in e.content:
                    quota_exhausted = True
                    break
                raise

            if found >= SEARCH_RESULT_LIMIT * 0.9:
                full_windows.append(f"{handle} {window_start.date()}..{window_end.date()}")

            # Results and the progress record go in together, results first, so
            # a window is never marked done without its videos saved.
            with snowflake.get_connection() as conn:
                if rows:
                    load_raw(
                        conn,
                        RESULTS_TABLE,
                        columns={
                            "channel_id": "varchar",
                            "video_id": "varchar",
                            "published_at": "timestamp_tz",
                            "raw_data": "variant",
                        },
                        rows=rows,
                        run_id=context.run_id,
                    )
                load_raw(
                    conn,
                    STATE_TABLE,
                    columns={
                        "channel_id": "varchar",
                        "window_start": "timestamp_tz",
                        "window_end": "timestamp_tz",
                        "results": "int",  # everything search returned
                        "new_videos": "int",  # of which not already known
                        "pages": "int",
                    },
                    rows=[
                        {
                            "channel_id": channel_id,
                            "window_start": window_start.isoformat(),
                            "window_end": window_end.isoformat(),
                            "results": found,
                            "new_videos": len(rows),
                            "pages": pages,
                        }
                    ],
                    run_id=context.run_id,
                )
            windows_done += 1
            new_total += len(rows)
            context.log.info(
                f"{handle} {window_start.date()}..{window_end.date()}: {found} results, "
                f"{len(rows)} new ({units} units used so far)"
            )
            window_end = window_start

        if window_end <= floor:
            finished_channels.append(handle)
            context.log.info(f"{handle}: search backfill complete back to {config.published_after}")
        else:
            # Budget or quota ran out inside this channel; the rest waits.
            break

    if full_windows:
        context.log.warning(
            f"{len(full_windows)} windows returned ~{SEARCH_RESULT_LIMIT} results, the most one search "
            f"can return, so some videos may be missing. Lower window_days: {', '.join(full_windows)}"
        )

    metadata = {
        "quota_units_used": units,
        "windows_completed": windows_done,
        "new_videos": new_total,
        "channels_finished": finished_channels,
        "windows_at_result_limit": full_windows,
        "published_after": config.published_after,
    }
    if quota_exhausted:
        raise dg.Failure(
            description=f"YouTube daily quota exhausted after {units} units this run; "
            f"{windows_done} windows saved. Resumes next run. Quota resets at midnight Pacific.",
            metadata=metadata,
        )
    return dg.MaterializeResult(metadata=metadata)

