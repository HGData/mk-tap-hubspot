"""tap-hubspot tap class."""

from __future__ import annotations

import datetime

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_hubspot import streams


class TapHubspot(Tap):
    """tap-hubspot is a Singer tap for Hubspot."""

    name = "tap-hubspot"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=False,
            description="Token to authenticate against the API service",
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=False,
            description="The OAuth app client ID.",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=False,
            description="The OAuth app client secret.",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=False,
            description="The OAuth app refresh token.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="Latest record date to sync",
        ),
        th.Property(
            "limit_events_month",
            th.IntegerType,
            required=False,
            description="Hard limit the start date to last X months from today (no limit if not set)",
        ),
        th.Property(
            "enabled_hubspot_pull_web_events",
            th.BooleanType,
            default=False,
            description="Enable syncing of web events for contacts (incremental, only for modified contacts)",
        ),
        th.Property(
            "enabled_hubspot_pull_global_web_events",
            th.BooleanType,
            default=False,
            description="Enable syncing of all web events globally (comprehensive, but may be slower)",
        ),
    ).to_dict()

    def get_effective_start_date(self) -> str | None:
        """Calculate the effective start date considering the limit_events_month parameter.

        Returns:
            The effective start date as ISO string, or None if no start_date is configured.
        """
        start_date = self.config.get("start_date")
        limit_events_month = self.config.get("limit_events_month")

        if not start_date:
            return None

        # If no limit is set, return the original start_date
        if not limit_events_month:
            return start_date

        # Calculate the limit date (X months ago from today using 31-day approximation)
        today = datetime.datetime.now(datetime.timezone.utc)
        limit_date = today - datetime.timedelta(days=31 * limit_events_month)

        limit_date_str = limit_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Use the more recent date between start_date and limit_date
        start_dt = datetime.datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        limit_dt = datetime.datetime.fromisoformat(limit_date_str.replace("Z", "+00:00"))

        if start_dt > limit_dt:
            # Original start_date is more recent, use it
            return start_date
        else:
            # Limit date is more recent, use the limit
            self.logger.info(
                f"Limiting start_date from {start_date} to {limit_date_str} "
                f"due to limit_events_month={limit_events_month}"
            )
            return limit_date_str

    def discover_streams(self) -> list[streams.HubspotStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ContactStream(self),
            streams.CompanyStream(self),
            streams.DealStream(self),
            streams.EmailEventsStream(self),
            streams.WebEventsStream(self),
        ]


if __name__ == "__main__":
    TapHubspot.cli()
