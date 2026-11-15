"""List every catalog channel's uploads published since the analysis start date.

Each channel's uploads are read from three playlists YouTube generates
automatically, one per format, which together hold every *public* upload:
    UULF...  long-form videos   (the channel page's "Videos" tab)
    UUSH...  Shorts             ("Shorts" tab)
    UULV...  past live streams  ("Live" tab)
They share the channel ID's suffix, like the all-uploads playlist UU... does.
Reading them instead of UU costs the same per video (50 per call, 1 quota
unit) and records each video's format in playlist_type, which the API offers
nowhere else (videos.list has no "is a Short" field). These playlists are
undocumented, but verified: for a channel under the cap the three totals add
up exactly to UU's.

Appends one row per (upload, playlist) to RAW.YT_UPLOADS. Full video details
and stats come from yt_videos; this asset only answers "which videos exist?".

Two modes per playlist, decided automatically:
  - BACKFILL: the playlist has never been fully pulled back to published_after.
    Page through it until videos get older than that. A big network can take
    ~400 units across its three playlists, so backfill can span several days.
  - INCREMENTAL: the backfill is done. Page only until reaching videos we've
    already stored (usually the first page, so ~3 units per channel).

Backfill completion is recorded per playlist in RAW.YT_UPLOADS_BACKFILL once
the channel's three playlists have been read, so a run that stops early (unit
budget reached, or quota exhausted) picks up only the unfinished ones.

History note: rows loaded before playlist_id existed came from UU and have
playlist_id NULL. The first run of this version re-backfills every channel
(old backfill records have no playlist_id either), which adds a second row,
with its format, for each of those videos. Downstream, prefer rows where
playlist_id is not null. yt_videos is unaffected: it groups by video_id.

Known limit: the API only ever lists a channel's ~20,000 most recent uploads.
The three format playlists split that same window rather than each getting
their own (their itemCounts add up to ~20,000: CNN 6,349 + 13,497 + 156), so
they don't get around it. For big networks that's well short of 2020; all of
such a channel's format playlists are recorded with truncated = true.

Reading YT_UPLOADS_BACKFILL for a truncated channel:
  - truncated is true for every format, correctly: none of them can show
    videos older than the window.
  - oldest_published_at is the oldest video OF THAT FORMAT, not where
    coverage starts. The channel's coverage starts at the MIN across its
    formats. Example, BBC News: long reaches 2021-08-18, short 2022-11-10,
    live 2026-06-08. Coverage of all three formats starts 2021-08-18; BBC
    just had no public live streams between then and 2026-06.
  yt_playlists and yt_search_backfill fill in what's before that date.
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

# Format -> playlist ID prefix. A channel ID "UCxyz" has uploads playlist
# "UUxyz", and these format playlists "UULFxyz", "UUSHxyz", "UULVxyz".
# The keys are what's stored in the playlist_type column.
FORMAT_PLAYLISTS = {"long": "UULF", "short": "UUSH", "live": "UULV"}

# The API only ever lists a channel's ~20,000 most recent uploads, however far
# you page. Big news networks pass that within a year or two (observed:
# ABC News, CBS, AP, BBC, PBS all stopped at exactly 20,000).
UPLOADS_LIST_CAP = 20_000


class UploadsConfig(dg.Config):
    """Run config, editable per run in the UI's Launchpad (shift+click Materialize)."""

    # Start of the analysis window (ISO date, e.g. "2020-01-01"). Uploads older
    # than this are never stored. Moving it earlier later is fine: playlists
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
    # so a run can overshoot by up to one channel's pages (~400 for a network).
    max_units: int = 4000


@dg.asset(
    group_name="youtube_raw",
    kinds={"youtube", "snowflake"},
    deps=["yt_catalog_initial"],
    description="Every public upload since published_after for each catalog channel, "
    "with its format (long / short / live) from the playlist it came from.",
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

        # The queries below filter on playlist_id, which tables created before
        # it existed don't have yet. load_raw would add it, but only on the
        # first load, after these reads; adding it now (a no-op once it
        # exists) lets the reads work on the first run of this version.
        for table in (TABLE, STATE_TABLE):
            if table_exists(conn, table):
                cur.execute(f"alter table {table} add column if not exists playlist_id varchar")

        # (channel_id, playlist_id) pairs whose backfill already reached back to
        # (at least) the configured date. Rows from before playlist_id existed
        # (NULL) don't count, so those channels are re-read once per format.
        done: set[tuple[str, str]] = set()
        if table_exists(conn, STATE_TABLE):
            cur.execute(
                f"""
                select distinct channel_id, playlist_id
                from {STATE_TABLE}
                where published_after <= %s and playlist_id is not null
                """,
                (config.published_after,),
            )
            done = {(row[0], row[1]) for row in cur.fetchall()}

        # Every video ID already stored, per playlist, so incremental runs know
        # where to stop. Keyed by playlist (not just channel) so the one-off
        # re-read above does store each old video again with its format.
        # A set of strings: a few MB even for ~1M videos.
        known: dict[tuple[str, str], set[str]] = {}
        if table_exists(conn, TABLE):
            cur.execute(
                f"select distinct channel_id, playlist_id, video_id from {TABLE} where playlist_id is not null"
            )
            for channel_id, playlist_id, video_id in cur.fetchall():
                known.setdefault((channel_id, playlist_id), set()).add(video_id)

    def playlist_ids(uploads_id: str) -> dict[str, str]:
        """{"long": "UULF...", ...} for a channel, from its uploads ID "UU..."."""
        suffix = uploads_id[2:]  # drop the "UU"
        return {fmt: prefix + suffix for fmt, prefix in FORMAT_PLAYLISTS.items()}

    def fully_backfilled(channel_id: str, uploads_id: str) -> bool:
        return all((channel_id, pid) in done for pid in playlist_ids(uploads_id).values())

    # Cheap incremental channels first, so routine updates always happen even
    # while a long backfill is still eating the budget. sorted() on a bool key
    # puts False (= already done) before True (= still needs backfill).
    channels.sort(key=lambda c: not fully_backfilled(c[0], c[2]))

    client = youtube.get_client()
    units = 0
    new_rows_total = 0
    finished_backfills = 0  # playlists, not channels
    truncated = []
    skipped = []
    bad_handles = []  # channels where every playlist 404'd; fix these in channels.yaml

    for channel_id, handle, uploads_id, video_count in channels:
        if units >= config.max_units:
            # Out of budget: leave the rest for the next run.
            skipped.append(handle)
            continue

        # Collected across the channel's three playlists, saved together below.
        rows = []
        state_rows = []
        not_found = 0  # playlists that returned 404
        quota_exhausted = False  # set if YouTube refuses mid-channel (daily limit hit)
        channel_truncated = False
        log_parts = []

        for playlist_type, playlist_id in playlist_ids(uploads_id).items():
            mode = "incremental" if (channel_id, playlist_id) in done else "backfill"
            seen = known.get((channel_id, playlist_id), set())
            new_in_playlist = 0
            oldest = None  # oldest publish date reached, for the backfill record
            reached_cutoff = False  # True once we've passed published_after
            read_whole_playlist = False

            try:
                for page in youtube.pages(
                    client.playlistItems(), part=",".join(PARTS), playlistId=playlist_id, maxResults=50
                ):
                    units += 1
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
                                "playlist_id": playlist_id,
                                "playlist_type": playlist_type,
                                "raw_data": item,
                            }
                        )
                        new_in_playlist += 1
                    if reached_cutoff:
                        break
                    # Incremental: once a page contains videos we already have, the
                    # rest of the playlist is already stored. Finishing the page
                    # (rather than stopping at the first known ID) tolerates small
                    # reorderings, e.g. a premiere that went live after videos
                    # uploaded around it.
                    if mode == "incremental" and page_has_known:
                        break
                else:
                    # for/else: runs only if the loop wasn't broken, i.e. we read
                    # every page the API would give us.
                    read_whole_playlist = True
            except HttpError as e:
                # 403 quotaExceeded: the project's 10,000 daily units are used up
                # (by this run, earlier runs today, or other assets). Stop reading
                # but don't raise yet: the pages already fetched for this channel
                # are in `rows`, and raising here would throw them away. They're
                # saved below, then the run stops. This playlist isn't marked as
                # backfilled, so the next run resumes it; `seen` skips the saved
                # videos (their pages still cost units to re-read, though).
                if e.resp.status == 403 and b"quotaExceeded" in e.content:
                    quota_exhausted = True
                    break
                # 404 playlistNotFound: YouTube has no such playlist. For one
                # format that just means the channel has no videos of that kind
                # (e.g. it has never streamed), so it counts as fully read and
                # empty. If all three 404, the channel has no public uploads at
                # all, which in a hand-curated catalog almost always means a
                # wrong handle; that's reported after the loop.
                elif e.resp.status == 404:
                    not_found += 1
                    read_whole_playlist = True
                else:
                    raise

            # The API only lists a channel's ~20,000 most recent uploads, and the
            # format playlists split that same window. So if we read a playlist to
            # its end without reaching published_after, and the channel has more
            # public videos than the cap, older uploads exist that the API won't
            # list: this format's history is truncated before `oldest`.
            # (Reading to the end of a small channel's playlist just means the
            # channel started after published_after; that's complete, not truncated.)
            # (`or 0`: a catalog row without statistics gives None.)
            is_truncated = read_whole_playlist and (video_count or 0) > UPLOADS_LIST_CAP
            channel_truncated = channel_truncated or is_truncated

            if mode == "backfill" and (reached_cutoff or read_whole_playlist):
                state_rows.append(
                    {
                        "channel_id": channel_id,
                        "playlist_id": playlist_id,
                        "playlist_type": playlist_type,
                        "published_after": config.published_after,
                        "oldest_published_at": oldest,
                        "truncated": is_truncated,
                    }
                )
            log_parts.append(f"{playlist_type} [{mode}] {new_in_playlist}")

        if not_found == len(FORMAT_PLAYLISTS):
            # Every format playlist 404'd: no public uploads at all. Rather than
            # failing the whole run, note it and move on; the list is reported
            # at the end. (Normally yt_catalog_initial skips these first via
            # videoCount == 0; this catches catalog rows stored before that check
            # existed, or channels emptied since.) Nothing to save, and no
            # backfill recorded, so it's checked again next run.
            bad_handles.append(f"{handle} ({channel_id})")
            context.log.warning(f"{handle} ({channel_id}): no uploads playlists (no public uploads), skipped")
            continue

        if channel_truncated:
            truncated.append(handle)
            context.log.warning(
                f"{handle}: uploads list capped at {UPLOADS_LIST_CAP:,} items "
                f"(channel has {video_count:,}); doesn't reach back to {config.published_after}"
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
                        "playlist_id": "varchar",  # e.g. UULF..., the playlist the row was read from
                        "playlist_type": "varchar",  # long / short / live
                        "raw_data": "variant",
                    },
                    rows=rows,
                    run_id=context.run_id,
                )
            # Record finished backfills right away, per channel, so a crash
            # later in the run doesn't make the next run redo this channel.
            # It's written after the uploads above, so "done" is never recorded
            # for uploads that failed to load.
            if state_rows:
                load_raw(
                    conn,
                    STATE_TABLE,
                    columns={
                        "channel_id": "varchar",
                        "playlist_id": "varchar",
                        "playlist_type": "varchar",
                        "published_after": "date",
                        # Oldest upload of THIS format reached, and whether
                        # the 20k cap cut the channel's history short. For a
                        # truncated channel, coverage starts at the MIN of
                        # oldest_published_at across its formats, not each
                        # format's own value (see the module docstring).
                        "oldest_published_at": "timestamp_tz",
                        "truncated": "boolean",
                    },
                    rows=state_rows,
                    run_id=context.run_id,
                )
                finished_backfills += len(state_rows)
                done.update((r["channel_id"], r["playlist_id"]) for r in state_rows)
        new_rows_total += len(rows)
        context.log.info(f"{handle}: new uploads {', '.join(log_parts)} ({units} units used so far)")

        if quota_exhausted:
            # Fail (rather than return normally like the max_units case) so the
            # run shows red and downstream assets like yt_videos don't start
            # and hit the same wall. Everything fetched so far is saved.
            raise dg.Failure(
                description=f"YouTube daily quota exhausted during {handle} after {units} units this run. "
                f"Saved {new_rows_total} new uploads; {handle}'s backfill resumes next run. "
                "Quota resets at midnight Pacific time."
            )

    if skipped:
        context.log.warning(
            f"Unit budget ({config.max_units}) reached; {len(skipped)} channels left for the next run: "
            + ", ".join(skipped)
        )
    if bad_handles:
        # Repeated here (not just per channel above) so the full list is in one
        # place at the end of the logs.
        context.log.warning(
            f"{len(bad_handles)} channels had no uploads playlists, likely wrong handles "
            "(check them in channels.yaml): " + ", ".join(bad_handles)
        )

    return dg.MaterializeResult(
        metadata={
            "quota_units_used": units,
            "new_uploads": new_rows_total,
            "playlist_backfills_completed": finished_backfills,
            "channels_backfilled_total": sum(fully_backfilled(c[0], c[2]) for c in channels),
            "channels_total": len(channels),
            "channels_skipped_for_budget": len(skipped),
            # Handles to fix in channels.yaml (empty list when all is well).
            "channels_bad_handle": bad_handles,
            # Channels whose history stops short of published_after because of
            # the API's 20k cap on the uploads list.
            "channels_truncated": truncated,
            "published_after": config.published_after,
        }
    )
