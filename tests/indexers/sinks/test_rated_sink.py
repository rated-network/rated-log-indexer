import json

from bytewax.dataflow import Dataflow
from bytewax.testing import run_main, TestingSource
from bytewax import operators as op
from pytest_httpx import HTTPXMock

from src.indexers.sinks.rated import build_http_sink


def test_http_sink(test_events, httpx_mock: HTTPXMock):
    endpoint = "http://test-rated-endpoint.com/ingest"
    httpx_mock.add_response(method="POST", url=endpoint, status_code=200)
    http_sink = build_http_sink(endpoint=endpoint, max_concurrent_requests=2)

    flow = Dataflow(flow_id="test_http_sink")
    input_source: TestingSource = TestingSource(test_events)
    (op.input("read", flow=flow, source=input_source).then(op.output, "out", http_sink))
    run_main(flow)

    assert len(httpx_mock.get_requests()) == len(test_events)

    for request, event in zip(httpx_mock.get_requests(), test_events):
        assert request.method == "POST"
        assert str(request.url) == endpoint
        assert json.loads(request.content) == event
