"""Jobs: named, reusable selections of assets plus the config to run them with.

An asset says *how* to produce data; a job says *which* assets to run together
and *with what settings*. Keeping analysis decisions (like the date window)
here means a different window is just another job, with no asset changes.

load_from_defs_folder picks up any job defined at module level in defs/, so
nothing else needs registering. Jobs appear under "Jobs" in the Dagster UI,
and are what schedules attach to later.
"""

import dagster as dg

# Start of the analysis window for this round of analysis.
PUBLISHED_AFTER = "2020-01-01"

youtube_since_2020_job = dg.define_asset_job(
    name="youtube_since_2020",
    description=f"Catalog -> uploads + curated playlists -> video snapshots, for videos published "
    f"since {PUBLISHED_AFTER}.",
    # Dagster runs the selected assets in dependency order (the deps= on each
    # asset), so listing order here doesn't matter.
    # yt_playlists only costs quota for channels it hasn't crawled yet, so
    # after the first pass over the catalog it's ~free; it's here so a newly
    # added channel gets its curated playlists crawled automatically.
    selection=["yt_catalog_initial", "yt_uploads", "yt_playlists", "yt_videos"],
    # Run config, in the same shape the Launchpad shows: for asset jobs every
    # asset is an "op", keyed by its name. yt_videos needs no date: it only
    # ever sees videos the other assets already filtered to the window.
    # max_units budgets keep their asset defaults unless overridden here:
    # uploads 4,000 + playlists 1,000 + videos 5,000 = the 10,000 daily limit.
    config={
        "ops": {
            "yt_uploads": {"config": {"published_after": PUBLISHED_AFTER}},
            "yt_playlists": {"config": {"published_after": PUBLISHED_AFTER}},
        }
    },
)

# The heavy lift, kept out of the daily job on purpose: search.list costs 100
# units per page, so this would eat the main job's quota. Run it on its own
# (by hand, or on a schedule later) when there's quota to spare, e.g. once the
# main job's backfills are done and it only uses a few hundred units a day.
# Videos it finds get their details from the next youtube_since_2020 run.
youtube_search_backfill_job = dg.define_asset_job(
    name="youtube_search_backfill",
    description=f"search.list backfill for channels whose uploads list is capped at 20k videos, "
    f"back to {PUBLISHED_AFTER}. Slow and quota-hungry; resumes where it left off.",
    selection=["yt_search_backfill"],
    config={
        "ops": {
            "yt_search_backfill": {"config": {"published_after": PUBLISHED_AFTER}},
        }
    },
)
