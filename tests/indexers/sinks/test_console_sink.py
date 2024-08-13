from bytewax.dataflow import Dataflow
from bytewax.testing import run_main, TestingSource
from bytewax import operators as op


from src.indexers.sinks.console import build_console_sink


def test_console_sink(test_events, capsys):
    input_source: TestingSource = TestingSource(test_events)
    console_sink = build_console_sink()

    flow_ = Dataflow(flow_id="test_console_sink")
    (
        op.input("read", flow=flow_, source=input_source).then(
            op.output, "out", console_sink
        )
    )

    run_main(flow_)

    captured = capsys.readouterr()
    output = captured.out + captured.err

    print(output)

    assert "Worker 0: FilteredEvent(log_id='mock_log_one'" in output
    assert "Worker 0: FilteredEvent(log_id='mock_log_two'" in output
    assert "Worker 0: FilteredEvent(log_id='mock_log_three'" in output
