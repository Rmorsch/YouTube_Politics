from collections.abc import Iterator

import dagster as dg
from dagster_snowflake import SnowflakeResource
from googleapiclient.discovery import build


class YouTubeResource(dg.ConfigurableResource):
    """Read-only access to the YouTube Data API v3 via an API key.

    Wrapping the client in a resource (instead of calling build() inside each
    asset) means the API key is configured in one place, shows up in the
    Dagster UI's resource tab, and can be swapped for a fake in tests.

    An API key can only read public data; owner-only fields would need OAuth.
    """

    api_key: str

    def get_client(self):
        # build() returns a client whose methods mirror the API's endpoints,
        # e.g. client.channels().list(...). Nothing is sent until .execute().
        # cache_discovery=False silences a noisy warning about a file cache
        # that newer google-auth versions no longer support.
        return build("youtube", "v3", developerKey=self.api_key, cache_discovery=False)

    @staticmethod
    def pages(endpoint, **params) -> Iterator[list[dict]]:
        """Yield the items of a *.list call one page at a time, following nextPageToken.

        endpoint is a collection from the client, e.g. client.playlistItems().
        params are passed straight to .list(), e.g. part=..., playlistId=...

        Each page is one API call (1 quota unit for most list endpoints).
        Because this is a generator, the caller decides when to stop: simply
        `break` out of the loop and no further pages are requested.
        """
        request = endpoint.list(**params)
        # list_next() builds the request for the following page, or returns
        # None when the response had no nextPageToken (i.e. this was the last page).
        while request is not None:
            response = request.execute()
            yield response.get("items", [])
            request = endpoint.list_next(request, response)


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "snowflake": SnowflakeResource(
                account=dg.EnvVar("account"),
                user=dg.EnvVar("dagster_user"),
                role=dg.EnvVar("dagster_role"),
                warehouse=dg.EnvVar("warehouse"),
                database=dg.EnvVar("database"),
                schema=dg.EnvVar("schema"),
                private_key_path=dg.EnvVar("private_key_path"),
                # Key is unencrypted, so no private_key_password; an empty one fails to load.
            ),
            # dg.EnvVar (not os.environ) defers reading the key until run time
            # and keeps its value out of the UI, which shows only the var name.
            "youtube": YouTubeResource(api_key=dg.EnvVar("youtube_api_key")),
        }
    )
