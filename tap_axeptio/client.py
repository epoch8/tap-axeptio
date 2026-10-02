"""REST client handling, including AxeptioStream base class."""

from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING, Any, Callable, Iterable

import requests
from requests.auth import HTTPBasicAuth
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"


class AxeptioStream(RESTStream):
    """Axeptio stream class."""

    # Update this value if necessary or override `parse_response`.
    # records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    # next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        return self.config.get("api_url", "")

    # @property
    # def authenticator(self) -> HTTPBasicAuth:
    #     """Return a new authenticator object.

    #     Returns:
    #         An authenticator instance.
    #     """
    #     return HTTPBasicAuth(
    #         username=self.config.get("username", ""),
    #         password=self.config.get("password", ""),
    #     )

    # Axeptio убрал выдачу токена по логину/паролю: POST {api_url}/v1/auth/local/signin
    # отвечает 404 с июля 2026. Токен выдаёт отдельный хост login.axept.io по паре
    # clientId/secret из личного кабинета Axeptio.
    _access_token: str | None = None
    _access_token_expires_at: float = 0.0

    @property
    def authenticator_token(self) -> str:
        """Вернуть закэшированный bearer-токен, запросив новый когда истёк."""
        if self._access_token and time.monotonic() < self._access_token_expires_at:
            return self._access_token

        auth_url = self.config.get("auth_url", "https://login.axept.io")
        response_auth = requests.post(
            url=auth_url + "/identity/resources/auth/v2/api-token",
            json={
                "clientId": self.config.get("client_id", ""),
                "secret": self.config.get("secret_key", ""),
            },
            timeout=30,
        )
        # Без этой проверки 404 от сервиса авторизации молча превращался
        # в "Authorization: Bearer None" и отдавал 401 на выгрузке.
        response_auth.raise_for_status()

        payload = response_auth.json()
        token = payload.get("access_token")
        if not token:
            msg = f"No access_token in auth response, got keys: {sorted(payload)}"
            raise RuntimeError(msg)

        # Дока обещает expires_in=3600, API отдаёт 86400 — поэтому доверяем
        # ответу и держим минуту запаса, вместо того чтобы хардкодить любое из них.
        ttl = int(payload.get("expires_in", 3600))
        self._access_token = token
        self._access_token_expires_at = time.monotonic() + max(ttl - 60, 60)

        return token

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {"Authorization": f"Bearer {self.authenticator_token}"}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        return headers

    # def get_new_paginator(self) -> BaseAPIPaginator:
    #     """Create a new pagination helper instance.

    #     If the source API can make use of the `next_page_token_jsonpath`
    #     attribute, or it contains a `X-Next-Page` header in the response
    #     then you can remove this method.

    #     If you need custom pagination that uses page numbers, "next" links, or
    #     other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

    #     Returns:
    #         A pagination helper instance.
    #     """
    #     return super().get_new_paginator()

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def prepare_request_payload(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ARG002, ANN401
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row

    def backoff_max_tries(self) -> int:
        """The number of attempts before giving up when retrying requests.

        Returns:
            Number of max retries.
        """
        return self.config.get("backoff_max_tries")
