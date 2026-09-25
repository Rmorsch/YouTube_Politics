import dagster as dg
from dagster_snowflake import SnowflakeResource


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
        }
    )
