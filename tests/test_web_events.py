"""Offline tests for how web event property values are typed."""

from __future__ import annotations

import copy
import logging
import types
import typing as t

import jsonschema  # type: ignore[import-untyped]
import pytest
from singer_sdk.helpers._typing import (
    TypeConformanceLevel,
    conform_record_data_types,
)

from tap_hubspot.streams import WebEventsStream, _convert_string_to_schema_type

if t.TYPE_CHECKING:
    from singer_sdk import Tap

NUMBER = {"type": ["number", "null"]}
INTEGER = {"type": ["integer", "null"]}
BOOLEAN = {"type": ["boolean", "null"]}
STRING = {"type": ["string", "null"]}


@pytest.mark.parametrize(
    ("value", "schema", "expected"),
    [
        ("11920000000", NUMBER, 11920000000.0),
        ("2700000000", NUMBER, 2700000000.0),
        ("-12.5", NUMBER, -12.5),
        (" 42 ", NUMBER, 42.0),
        ("n/a", NUMBER, None),
        ("", NUMBER, None),
        ("nan", NUMBER, None),
        ("inf", NUMBER, None),
        ("3", INTEGER, 3),
        ("3.0", INTEGER, 3),
        ("3.5", INTEGER, None),
        ("many", INTEGER, None),
        ("true", BOOLEAN, True),
        ("false", BOOLEAN, False),
        ("FALSE", BOOLEAN, False),
        ("yes", BOOLEAN, None),
        ("", BOOLEAN, None),
        ("false", STRING, "false"),
        ("11920000000", STRING, "11920000000"),
    ],
)
def test_convert_string_to_schema_type(value, schema, expected) -> None:
    result = _convert_string_to_schema_type(value, schema)
    assert result == expected
    assert type(result) is type(expected)


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


# The shape of the company-signal event that failed every sync: HubSpot sends
# hs_revenue, and every other event property, as a string.
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
        "hs_is_contact": "false",
        "hs_url": "https://example.com/news",
    },
}


def _as_sent_to_target(stream: WebEventsStream, record: dict) -> dict:
    """Apply the same type conformance singer-sdk runs before writing a record."""
    return conform_record_data_types(
        stream_name=stream.name,
        record=record,
        schema=stream.schema,
        level=TypeConformanceLevel.RECURSIVE,
        logger=stream.logger,
    )


def test_raw_event_is_mistyped_without_conversion() -> None:
    stream = _stream()
    record = _as_sent_to_target(stream, copy.deepcopy(COMPANY_REVENUE_EVENT))

    errors = jsonschema.Draft7Validator(stream.schema).iter_errors(record)
    failing = {error.path[-1] for error in errors}
    assert failing == {"hs_revenue", "hs_page_views"}
    # singer-sdk reads any string in a boolean property as True.
    assert record["properties"]["hs_is_amp"] is True


def test_post_process_types_event_properties() -> None:
    stream = _stream()
    row = stream.post_process(copy.deepcopy(COMPANY_REVENUE_EVENT), None)
    assert row is not None
    record = _as_sent_to_target(stream, row)

    jsonschema.Draft7Validator(stream.schema).validate(record)
    properties = record["properties"]
    assert properties["hs_revenue"] == 11920000000.0
    assert properties["hs_is_amp"] is False
    assert properties["hs_is_external"] is True
    assert properties["hs_page_views"] == 3
    assert properties["hs_is_contact"] == "false"
    assert properties["hs_url"] == "https://example.com/news"
    assert "hs_revenue_formatted" not in properties
