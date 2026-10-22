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
    description=f"Catalog -> uploads -> video snapshots, for videos published since {PUBLISHED_AFTER}.",
    # Dagster runs the selected assets in dependency order (the deps= on each
    # asset), so listing order here doesn't matter.
    selection=["yt_catalog_initial", "yt_uploads", "yt_videos"],
    # Run config, in the same shape the Launchpad shows: for asset jobs every
    # asset is an "op", keyed by its name. Only yt_uploads needs the date;
    # yt_videos only ever sees uploads already inside the window.
    # max_units budgets keep their asset defaults unless overridden here.
    config={
        "ops": {
            "yt_uploads": {"config": {"published_after": PUBLISHED_AFTER}},
        }
    },
)
