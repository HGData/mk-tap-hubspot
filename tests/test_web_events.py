"""Offline tests for how web event property values are typed."""

from __future__ import annotations

import copy
import logging
import types
import typing as t

import jsonschema  # type: ignore[import-untyped]
from singer_sdk.helpers._typing import (
    TypeConformanceLevel,
    conform_record_data_types,
)

from tap_hubspot.streams import WebEventsStream

if t.TYPE_CHECKING:
    from singer_sdk import Tap


def _stream() -> WebEventsStream:
    """Build the stream without a tap, whose start-up calls the HubSpot API."""
    tap = types.SimpleNamespace(
        name="tap-hubspot",
        config={"start_date": "2024-01-01T00:00:00Z"},
        state={},
        logger=logging.getLogger("tap-hubspot"),
        metrics_logger=logging.getLogger("tap-hubspot.metrics"),
    )
    return WebEventsStream(tap=t.cast("Tap", tap))


def _as_sent_to_target(stream: WebEventsStream, record: dict) -> dict:
    """Apply the same type conformance singer-sdk runs before writing a record."""
    return conform_record_data_types(
        stream_name=stream.name,
        record=record,
        schema=stream.schema,
        level=TypeConformanceLevel.RECURSIVE,
        logger=stream.logger,
    )


# The shape of the company-signal event that failed every sync, with values
# chosen by hand: HubSpot sends hs_revenue, and every other event property, as
# a string.
COMPANY_REVENUE_EVENT = {
    "id": "event-1",
    "objectType": "COMPANY",
    "objectId": "123",
    "eventType": "e_company_reports_revenue",
    "occurredAt": "2026-11-04T17:13:38+00:00",
    "properties": {
        "hs_revenue": "11920000000",
        "hs_revenue_formatted": "$11.92B",
        "hs_is_amp": "false",
        "hs_is_external": "true",
        "hs_page_views": "3",
        "hs_custom_metric_1": "n/a",
        "hs_is_contact": "false",
        "hs_url": "https://example.com/news",
    },
}


def test_every_event_property_is_declared_a_string() -> None:
    declared = _stream().schema["properties"]["properties"]["properties"]
    assert declared
    not_strings = {
        name: schema["type"] for name, schema in declared.items() if set(schema["type"]) != {"string", "null"}
    }
    assert not_strings == {}


def test_event_properties_are_written_as_sent() -> None:
    stream = _stream()
    row = stream.post_process(copy.deepcopy(COMPANY_REVENUE_EVENT), None)
    assert row is not None
    record = _as_sent_to_target(stream, row)

    jsonschema.Draft7Validator(stream.schema).validate(record)
    properties = record["properties"]
    assert properties["hs_revenue"] == "11920000000"
    # singer-sdk turns any string in a boolean property into True; as a string,
    # "false" stays "false".
    assert properties["hs_is_amp"] == "false"
    assert properties["hs_is_external"] == "true"
    assert properties["hs_page_views"] == "3"
    assert properties["hs_custom_metric_1"] == "n/a"
    assert properties["hs_is_contact"] == "false"
    assert properties["hs_url"] == "https://example.com/news"
    assert "hs_revenue_formatted" not in properties
