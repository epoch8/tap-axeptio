"""Stream type classes for tap-axeptio."""

from __future__ import annotations

import sys
import typing as t
import requests
import json
import pendulum

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk import metrics

from tap_axeptio.client import AxeptioStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


TPageToken = t.TypeVar("TPageToken")


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class MyPaginator(BaseAPIPaginator):
    def has_more(self, response: requests.Response) -> bool:  # noqa: ARG002
        """Override this method to check if the endpoint has any pages left.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        return pendulum.parser.parse(self._value).date() < pendulum.today().date().subtract(days=1)


    def get_next(self, response: requests.Response) -> TPageToken | None:
        """Get the next pagination token or index from the API response.

        Args:
            response: API response object.

        Returns:
            The next page token or index. Return `None` from this method to indicate
                the end of pagination.
        """
        next = pendulum.parser.parse(self._value).date().add(days=1)
        return f"{next}"


class AxeptioExportsStream(AxeptioStream):
    """Define custom stream."""

    name = "axeptio_exports"
    path = "/v1/app/exports/62bea2b1af0eb6c162613cf5.csv"
    primary_keys: t.ClassVar[list[str]] = ["token"]
    replication_key = "date"
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
        for line in lines[1:-1]:
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
        
        if next_page_token:
            params["start"] = f"{next_page_token}T00:00:00.000Z"
            params["end"] = f"{next_page_token}T23:59:59.999Z"

        return params
    

    def get_new_paginator(self, start_date):
        return MyPaginator(start_value=start_date)


    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        if (replication_key_value := self.get_starting_replication_key_value(context=context)) is not None:
            replication_key_value = pendulum.parse(replication_key_value).date()

            start_date = self.compare_start_date(
                value=replication_key_value.strftime("%Y-%m-%d"),
                start_date_value=self.config["start_date"]
            )
        
        else:
            start_date = self.config["start_date"]

        paginator = self.get_new_paginator(start_date)
        decorated_request = self.request_decorator(self._request)
        pages = 0

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context

            while not paginator.finished:
                prepared_request = self.prepare_request(
                    context,
                    next_page_token=paginator.current_value,
                )
                resp = decorated_request(prepared_request, context)
                request_counter.increment()
                self.update_sync_costs(prepared_request, resp, context)
                records = iter(self.parse_response(resp))
                try:
                    first_record = next(records)
                except StopIteration:
                    # self.logger.info(
                    #     "Pagination stopped after %d pages because no records were "
                    #     "found in the last response",
                    #     pages,
                    # )
                    # break
                    self.logger.warning(f"No records were found for the date:\t{paginator._value}")
                    paginator.advance(resp)
                    continue

                yield first_record
                yield from records
                pages += 1

                paginator.advance(resp)
