import json

import pytest
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main, TestingSource
from bytewax import operators as op
from pytest_httpx import HTTPXMock

from src.config.models.output import RatedOutputConfig
from src.indexers.sinks.rated import build_http_sink
from src.indexers.filters.types import FilteredEvent
from datetime import timedelta


def test_http_sink_3_events(
    http_sink, httpx_mock: HTTPXMock, test_events, capture_output
):
    flow = Dataflow(flow_id="test_http_sink_time")
    input_source: TestingSource = TestingSource(test_events)
    op.input("read", flow=flow, source=input_source).then(op.output, "out", http_sink)

    run_main(flow)

    requests = httpx_mock.get_requests()
    assert len(requests) == 1, f"Expected 1 request, but got {len(requests)}"

    request = requests[0]
    body = json.loads(request.content)
    assert len(body) == len(
        test_events
    ), f"Expected {len(test_events)} events, but got {len(body)}"

    endpoint = "https://your_ingestion_url.com/v1/ingest"

    for event_data, event in zip(body, test_events):
        assert request.method == "POST"
        assert str(request.url) == f"{endpoint}/your_ingestion_id/your_ingestion_key"
        assert event_data["organization_id"] == event.organization_id
        assert event_data["timestamp"] == event.event_timestamp.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        assert event_data["values"] == event.values

    stdout, stderr = capture_output
    assert not stderr.getvalue(), f"Unexpected error output: {stderr.getvalue()}"


def test_http_sink_batch_size(
    http_sink, httpx_mock: HTTPXMock, test_events, capture_output
):
    events = []
    for i in range(100):
        event = test_events[i % len(test_events)]
        new_event = FilteredEvent(
            slaos_key="",
            organization_id=f"{event.organization_id}_{i}",
            idempotency_key=f"{event.idempotency_key}_{i}",
            event_timestamp=event.event_timestamp + timedelta(seconds=i),
            values={"example_key": f"example_value_{i}"},
        )
        events.append(new_event)

    flow = Dataflow(flow_id="test_http_sink_batch")
    input_source: TestingSource = TestingSource(events)
    op.input("read", flow=flow, source=input_source).then(op.output, "out", http_sink)

    stdout, stderr = capture_output
    run_main(flow)

    requests = httpx_mock.get_requests()
    assert len(requests) == 2, "We should have made 2 requests only"

    for request in requests:
        body = json.loads(request.content)
        assert len(body) == 50, "Each request should have 50 events"

    assert not stderr.getvalue(), f"Unexpected error output: {stderr.getvalue()}"


def test_http_sink_mixed_scenario(
    http_sink, httpx_mock: HTTPXMock, test_events, capture_output
):
    events = []
    for i in range(80):
        event = test_events[i % len(test_events)]
        new_event = FilteredEvent(
            slaos_key="",
            organization_id=f"{event.organization_id}_{i}",
            idempotency_key=f"{event.idempotency_key}_{i}",
            event_timestamp=event.event_timestamp + timedelta(seconds=i),
            values={"example_key": f"example_value_{i}"},
        )
        events.append(new_event)

    flow = Dataflow(flow_id="test_http_sink_mixed")
    input_source: TestingSource = TestingSource(events)
    op.input("read", flow=flow, source=input_source).then(op.output, "out", http_sink)

    stdout, stderr = capture_output
    run_main(flow)

    requests = httpx_mock.get_requests()
    assert len(requests) == 2, "We should have made 2 requests only"

    first_body = json.loads(requests[0].content)
    second_body = json.loads(requests[1].content)

    assert len(first_body) == 50
    assert len(second_body) == 30

    assert not stderr.getvalue(), f"Unexpected error output: {stderr.getvalue()}"


def test_http_sink_time_based_under_timeout(
    http_sink, httpx_mock: HTTPXMock, test_events, capture_output, mocked_time
):
    events_batch_1 = test_events[:3]
    events_batch_2 = [
        FilteredEvent(
            slaos_key="",
            organization_id=f"{event.organization_id}_new_{i}",
            idempotency_key=f"{event.idempotency_key}_new_{i}",
            event_timestamp=event.event_timestamp + timedelta(seconds=8),
            values={"example_key": f"new_value_{i}"},
        )
        for i, event in enumerate(test_events[:3])
    ]

    flow = Dataflow(flow_id="test_http_sink_time_under_timeout")
    input_source: TestingSource = TestingSource(
        events_batch_1 + [None] + events_batch_2
    )

    def add_delay(event):
        if event is None:
            mocked_time.tick(delta=timedelta(seconds=8))
            return None
        return event

    op.input("read", flow=flow, source=input_source).then(
        op.map, "delay", add_delay
    ).then(op.filter, "remove_none", lambda x: x is not None).then(
        op.output, "out", http_sink
    )

    stdout, stderr = capture_output
    run_main(flow)

    # Advance time by 1 second to allow for any pending flushes
    mocked_time.tick(delta=timedelta(seconds=1))

    requests = httpx_mock.get_requests()
    assert len(requests) == 1, f"Expected 1 request, but got {len(requests)}"

    body = json.loads(requests[0].content)
    assert len(body) == 6, f"Expected 6 events in total, but got {len(body)}"

    assert not stderr.getvalue(), f"Unexpected error output: {stderr.getvalue()}"


def test_http_sink_time_based_over_timeout(
    http_sink, httpx_mock: HTTPXMock, test_events, capture_output, mocked_time
):
    events_batch_1 = test_events
    events_batch_2 = [
        FilteredEvent(
            slaos_key="",
            organization_id=f"{event.organization_id}_new_{i}",
            idempotency_key=f"{event.idempotency_key}_new_{i}",
            event_timestamp=event.event_timestamp + timedelta(seconds=11),
            values={"example_key": f"new_value_{i}"},
        )
        for i, event in enumerate(test_events)
    ]

    flow = Dataflow(flow_id="test_http_sink_time_over_timeout")
    input_source: TestingSource = TestingSource(
        events_batch_1 + [None] + events_batch_2
    )

    def add_delay(event):
        if event is None:
            mocked_time.tick(delta=timedelta(seconds=11))
            return None
        return event

    op.input("read", flow=flow, source=input_source).then(
        op.map, "delay", add_delay
    ).then(op.filter, "remove_none", lambda x: x is not None).then(
        op.output, "out", http_sink
    )

    stdout, stderr = capture_output
    run_main(flow)

    # Advance time by 1 second to allow for any pending flushes
    mocked_time.tick(delta=timedelta(seconds=1))

    requests = httpx_mock.get_requests()
    assert len(requests) == 2, f"Expected 2 requests, but got {len(requests)}"

    first_body = json.loads(requests[0].content)
    second_body = json.loads(requests[1].content)

    total_events = len(first_body) + len(second_body)
    assert total_events == 6, f"Expected 6 events in total, but got {total_events}"

    assert (
        3 <= len(first_body) <= 4
    ), f"Expected 3-4 events in first request, but got {len(first_body)}"
    assert (
        2 <= len(second_body) <= 3
    ), f"Expected 2-3 events in second request, but got {len(second_body)}"

    all_events = first_body + second_body
    for i, event in enumerate(all_events):
        if i < 3:
            assert (
                event["organization_id"] == test_events[i].organization_id
            ), f"Mismatch in event {i} of first batch"
        else:
            assert (
                event["organization_id"]
                == f"{test_events[i-3].organization_id}_new_{i-3}"
            ), f"Mismatch in event {i} of second batch"

    assert not stderr.getvalue(), f"Unexpected error output: {stderr.getvalue()}"


@pytest.mark.skip(reason="To be implemented")
def test_http_sink_with_slaos_key(httpx_mock: HTTPXMock, test_events, capture_output):
    config = RatedOutputConfig(
        ingestion_id="your_ingestion_id",
        ingestion_key="your_ingestion_key",
        ingestion_url="https://your_ingestion_url.com/v1/ingest",
    )
    slaos_key = "test_integration"
    http_sink = build_http_sink(config, slaos_key)

    # Mock the HTTP response
    httpx_mock.add_response(
        method="POST",
        url=f"{config.ingestion_url}/{config.ingestion_id}/{config.ingestion_key}",
        status_code=200,
        json={"status": "success"},
    )

    flow = Dataflow(flow_id="test_http_sink_prefix")
    input_source: TestingSource = TestingSource(test_events)
    op.input("read", flow=flow, source=input_source).then(op.output, "out", http_sink)

    run_main(flow)

    requests = httpx_mock.get_requests()
    assert len(requests) == 1, "Expected 1 request"

    request = requests[0]
    body = json.loads(request.content)

    assert len(body) == len(
        test_events
    ), f"Expected {len(test_events)} events, but got {len(body)}"
    for item in body:
        for key in item["values"]:

            assert key.startswith(
                f"{slaos_key}_"
            ), f"Metric {key} does not start with the slaOS key {slaos_key}"

    stdout, stderr = capture_output
    assert not stderr.getvalue(), f"Unexpected error output: {stderr.getvalue()}"
