"""Stream type classes for tap-axeptio."""

from __future__ import annotations

import sys
import typing as t

from singer_sdk import typing as th  # JSON Schema typing helpers
import requests
import json

from tap_axeptio.client import AxeptioStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class AxeptioExportsStream(AxeptioStream):
    """Define custom stream."""

    name = "axeptio_exports"
    path = "/v1/app/exports/62bea2b1af0eb6c162613cf5.csv"
    primary_keys: t.ClassVar[list[str]] = ["token"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"  # noqa: ERA001
    schema = th.PropertiesList(
        th.Property("token", th.StringType),
        th.Property("collection", th.StringType),
        th.Property("identifier", th.StringType),
        th.Property("accept", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("value", th.StringType),
        th.Property("preferences", th.StringType),
        th.Property("project", th.StringType),
    ).to_dict()


    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        lines = response.text.split("\n")
        column_names = lines[0].split(";")
        for line in lines[1:]:
            record = dict(zip(column_names, line.split(";")))
            yield record


    def post_process(self, row: dict, context: t.Optional[dict] = None) -> t.Optional[dict]:
        json_preferences = json.loads(row.get("preferences", "{}"))
        row["project"] = json_preferences.get("config", {}).get("name", "")
        return row


    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        # if next_page_token:
        #     params["page"] = next_page_token
        # if self.replication_key:
        #     params["sort"] = "asc"
        #     params["order_by"] = self.replication_key

        params["start"] = f"{self.config.get('start_date', '2022-07-01')}T00:00:00.000Z"
        params["end"] = f"{self.config.get('start_date', '2022-07-01')}T23:59:59.999Z"
        
        return params
