"""Axeptio tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_axeptio import streams


class TapAxeptio(Tap):
    """Axeptio tap class."""

    name = "tap-axeptio"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="Client ID пары client-credentials из личного кабинета Axeptio",
        ),
        th.Property(
            "secret_key",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="Secret Key пары client-credentials из личного кабинета Axeptio",
        ),
        th.Property(
            "auth_url",
            th.StringType,
            default="https://login.axept.io",
            description="The url for the identity service issuing access tokens",
        ),
        th.Property(
            "username",
            th.StringType,
            required=False,
            description="Legacy, не используется: Axeptio убрал вход по логину/паролю",
        ),
        th.Property(
            "password",
            th.StringType,
            required=False,
            secret=True,  # Flag config as protected.
            description="Legacy, не используется: Axeptio убрал вход по логину/паролю",
        ),
        th.Property(
            "start_date",
            th.DateType,
            default="2022-07-01",
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.axept.io",
            description="The url for the API service",
        ),
        th.Property(
            "backoff_max_tries",
            th.IntegerType,
            default=5,
            description="The number of attempts before giving up when retrying requests",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.AxeptioStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.AxeptioExportsStream(self),
        ]


if __name__ == "__main__":
    TapAxeptio.cli()
