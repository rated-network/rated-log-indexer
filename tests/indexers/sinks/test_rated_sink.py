import json

from bytewax.dataflow import Dataflow
from bytewax.testing import run_main, TestingSource
from bytewax import operators as op
from pytest_httpx import HTTPXMock

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

    endpoint = "https://your_ingestion_url.com"

    for event_data, event in zip(body, test_events):
        assert request.method == "POST"
        assert str(request.url) == f"{endpoint}/your_ingestion_id/your_ingestion_key"
        assert event_data["customer_id"] == event.customer_id
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
            customer_id=f"{event.customer_id}_{i}",
            event_id=f"{event.event_id}_{i}",
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
            customer_id=f"{event.customer_id}_{i}",
            event_id=f"{event.event_id}_{i}",
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
            customer_id=f"{event.customer_id}_new_{i}",
            event_id=f"{event.event_id}_new_{i}",
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
            customer_id=f"{event.customer_id}_new_{i}",
            event_id=f"{event.event_id}_new_{i}",
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
                event["customer_id"] == test_events[i].customer_id
            ), f"Mismatch in event {i} of first batch"
        else:
            assert (
                event["customer_id"] == f"{test_events[i-3].customer_id}_new_{i-3}"
            ), f"Mismatch in event {i} of second batch"

    assert not stderr.getvalue(), f"Unexpected error output: {stderr.getvalue()}"
