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
            "username",
            th.StringType,
            required=True,
            description="Username",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="Password",
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
