"""REST client handling, including HubspotStream base class."""

from __future__ import annotations

import datetime
import os
import sys
import typing as t
from functools import cached_property

import requests
from singer_sdk import typing as th
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.singerlib.utils import strptime_to_utc
from singer_sdk.streams import RESTStream
from singer_sdk.streams.core import REPLICATION_INCREMENTAL

from tap_hubspot.auth import HubSpotOAuthAuthenticator

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from singer_sdk.pagination import BaseAPIPaginator

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()

_Auth = t.Callable[[requests.PreparedRequest], requests.PreparedRequest]

# Single source of truth for the HubSpot API domain used by all streams and
# requests. Override here to point the tap at a different host.
BASE_HUBSPOT_API_URL = "https://api.hubapi.com"


class HubspotStream(RESTStream):
    """tap-hubspot stream class."""

    @property
    def url_base(self) -> str:
        """Returns base url."""
        return f"{BASE_HUBSPOT_API_URL}/"

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    # Set this value or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @cached_property
    def authenticator(self) -> _Auth:
        """Return a new authenticator object.
        Returns:
            An authenticator instance.
        """
        if "refresh_token" in self.config:
            return HubSpotOAuthAuthenticator(
                self,
                auth_endpoint=f"{BASE_HUBSPOT_API_URL}/oauth/v1/token",
            )
        return BearerTokenAuthenticator(
            self,
            token=self.config.get("access_token"),  # type: ignore[arg-type]
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.
        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def post_process(self, row: dict, context: t.Any = None) -> dict | None:
        """Inject the run's partition timestamp into every record."""
        row["dt"] = os.environ.get("CURRENT_DATE_MINUTE_LEVEL")
        return row

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.
        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.
        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.
        Returns:
            A pagination helper instance.
        """
        return super().get_new_paginator()

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: int | None,  # noqa: ARG002
    ) -> int | None:
        """Return a token for identifying next page or None if no more pages."""
        # If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        resp_json = response.json()
        paging = resp_json.get("paging")

        if paging is not None:
            next_page_token = resp_json.get("paging", {}).get("next", {}).get("after")
        else:
            next_page_token = None
        return next_page_token

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.
        Args:
            context: The stream context.
            next_page_token: The next page index or value.
        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        params["limit"] = 1000
        if next_page_token:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params


class DynamicHubspotStream(HubspotStream):
    """DynamicHubspotStream."""

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:  # noqa: D107
        super().__init__(*args, **kwargs)

    def _get_datatype(self, data_type: str) -> th.JSONTypeHelper:  # noqa: ARG002
        # TODO: consider typing more precisely  # noqa: TD002, TD003, FIX002
        return th.StringType()

    @cached_property
    def schema(self) -> dict:
        """Return a draft JSON schema for this stream."""
        hs_props = []
        self.hs_properties = self._get_available_properties()
        for name, prop_type in self.hs_properties.items():
            hs_props.append(
                th.Property(name, self._get_datatype(prop_type)),
            )
        schema = th.PropertiesList(
            th.Property("id", th.StringType),
            th.Property(
                "properties",
                th.ObjectType(*hs_props),
            ),
            th.Property("createdAt", th.DateTimeType),
            th.Property("updatedAt", th.DateTimeType),
            th.Property("archived", th.BooleanType),
            th.Property("dt", th.StringType),
        )
        return schema.to_dict()

    def _get_available_properties(self) -> dict[str, str]:
        session = requests.Session()
        session.auth = self.authenticator

        resp = session.get(
            f"{BASE_HUBSPOT_API_URL}/crm/v3/properties/{self.name}",
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return {prop["name"]: prop["type"] for prop in results}

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.
        Args:
            context: The stream context.
            next_page_token: The next page index or value.
        Returns:
            A dictionary of URL query parameters.
        """
        params = super().get_url_params(context, next_page_token)
        if self.hs_properties:
            params["properties"] = ",".join(self.hs_properties)
        return params


class DynamicIncrementalHubspotStream(DynamicHubspotStream):
    """DynamicIncrementalHubspotStream."""

    page_size = 200

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:  # noqa: D107
        super().__init__(*args, **kwargs)
        # Track incremented timestamp for 10k limit handling
        self._incremented_ts: dict[str, datetime.datetime] = {}

    def _is_incremental_search(self, context: Context | None) -> bool:
        return (
            self.replication_method == REPLICATION_INCREMENTAL  # type: ignore[return-value]
            and self.get_starting_replication_key_value(context)
            and hasattr(self, "incremental_path")
            and self.incremental_path
        )

    @cached_property
    def schema(self) -> dict:
        """Return a draft JSON schema for this stream."""
        hs_props = []
        self.hs_properties = self._get_available_properties()
        for name, prop_type in self.hs_properties.items():
            hs_props.append(
                th.Property(name, self._get_datatype(prop_type)),
            )
        schema = th.PropertiesList(
            th.Property("id", th.StringType),
            th.Property(
                "properties",
                th.ObjectType(*hs_props),
            ),
            th.Property("createdAt", th.DateTimeType),
            th.Property("updatedAt", th.DateTimeType),
            th.Property("archived", th.BooleanType),
            th.Property("dt", th.StringType),
        )
        if self.replication_key:
            schema.append(
                th.Property(
                    self.replication_key,
                    th.DateTimeType,
                ),
            )
        return schema.to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.
        Args:
            context: The stream context.
            next_page_token: The next page index or value.
        Returns:
            A dictionary of URL query parameters.
        """
        if self._is_incremental_search(context):
            return {}
        return super().get_url_params(context, next_page_token)

    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.
        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.
        Developers may also return `None` from this method to filter out
        invalid or not-applicable records from the stream.
        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.
        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        row = super().post_process(row, context)
        if row is None:
            return None
        if self.replication_key:
            val = None
            if props := row.get("properties"):
                val = props[self.replication_key]
            row[self.replication_key] = val
        return row

    def prepare_request(  # noqa: D102
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> requests.PreparedRequest:
        if self._is_incremental_search(context):
            # Search endpoints use POST request
            self.path = self.incremental_path  # type: ignore[attr-defined]
            self.http_method = "POST"
        return super().prepare_request(context, next_page_token)

    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.
        By default, no payload will be sent (return None).
        Developers may override this method if the API requires a custom payload along
        with the request. (This is generally not required for APIs which use the
        HTTP 'GET' method.)
        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        # Check if we're using the search endpoint (POST method)
        if hasattr(self, "http_method") and self.http_method == "POST":
            body: dict[str, t.Any] = {}

            if self._is_incremental_search(context):
                # Only filter in case we have a value to filter on
                # https://developers.hubspot.com/docs/api/crm/search

                # Create a context key for tracking incremented timestamp
                context_key = str(context) if context else "default"

                # Use incremented timestamp if available, otherwise start with replication key value
                if context_key in self._incremented_ts:
                    ts = self._incremented_ts[context_key]
                else:
                    ts = datetime.datetime.fromisoformat(
                        self.get_starting_replication_key_value(context),  # type: ignore[arg-type]
                    )

                if next_page_token:
                    # Hubspot wont return more than 10k records so when we hit 10k we
                    # need to increment our epoch timestamp to continue forward and not send the
                    # next_page_token
                    if int(next_page_token) + self.page_size >= 10000:  # noqa: PLR2004
                        # Get the current progress markers replication key value
                        progress_markers = self.get_context_state(context).get(  # type: ignore[union-attr]
                            "progress_markers", {}
                        )
                        if progress_replication_value := progress_markers.get(
                            "replication_key_value"
                        ):
                            # Use the most recent timestamp from progress markers and increment by 1 millisecond
                            progress_ts = strptime_to_utc(progress_replication_value)
                            ts = progress_ts + datetime.timedelta(milliseconds=1)
                        else:
                            # Fallback: increment current ts by 1 millisecond if no progress markers
                            ts = ts + datetime.timedelta(milliseconds=1)

                        # Store the incremented timestamp globally for future calls
                        self._incremented_ts[context_key] = ts

                    else:
                        body["after"] = next_page_token

                epoch_ts = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

                body.update(
                    {
                        "filterGroups": [
                            {
                                "filters": [
                                    {
                                        "propertyName": self.replication_key,
                                        "operator": "GT",
                                        "value": epoch_ts,
                                    },
                                ],
                            },
                        ],
                        "sorts": [
                            {
                                # This is inside the properties object
                                "propertyName": self.replication_key,
                                "direction": "ASCENDING",
                            },
                        ],
                        "limit": self.page_size,
                        "properties": list(self.hs_properties),
                    },
                )
            else:
                # If we're using POST but not incremental search, provide a basic search body
                # This handles the case where the stream is configured for search but doesn't
                # meet all incremental conditions
                body.update(
                    {
                        "limit": self.page_size,
                        "properties": list(self.hs_properties)
                        if hasattr(self, "hs_properties") and self.hs_properties
                        else [],
                    }
                )

                # Add pagination if needed
                if next_page_token:
                    body["after"] = next_page_token

            return body

        # For non-POST requests, return None (no body needed)
        return None
