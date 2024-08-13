import json

from bytewax.dataflow import Dataflow
from bytewax.testing import run_main, TestingSource
from bytewax import operators as op
from pytest_httpx import HTTPXMock

from src.config.models.output import RatedOutputConfig
from src.indexers.sinks.rated import build_http_sink


def test_http_sink(test_events, httpx_mock: HTTPXMock):
    endpoint = "https://your_ingestion_url.com"
    httpx_mock.add_response(
        method="POST",
        url=f"{endpoint}/your_ingestion_id/your_ingestion_key",
        status_code=200,
    )
    output_config = RatedOutputConfig(
        ingestion_id="your_ingestion_id",
        ingestion_key="your_ingestion_key",
        ingestion_url=endpoint,
    )
    http_sink = build_http_sink(output_config)

    flow = Dataflow(flow_id="test_http_sink")
    input_source: TestingSource = TestingSource(test_events)
    (op.input("read", flow=flow, source=input_source).then(op.output, "out", http_sink))
    run_main(flow)

    assert len(httpx_mock.get_requests()) == len(test_events)

    for request, event in zip(httpx_mock.get_requests(), test_events):
        body = json.loads(request.content)
        assert request.method == "POST"
        assert str(request.url) == f"{endpoint}/your_ingestion_id/your_ingestion_key"
        assert body["customer_id"] == event.customer_id
        assert body["timestamp"] == event.event_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        assert body["values"] == event.values
