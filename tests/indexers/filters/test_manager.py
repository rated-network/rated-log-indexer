from rated_parser.payloads import LogFormat, MetricFieldDefinition  # type: ignore
from rated_parser.payloads.log_patterns import JsonFieldDefinition, FieldType  # type: ignore

from src.config.models.filters import LogFilterConfig, MetricFilterConfig
from src.config.models.inputs.input import InputTypes
from src.indexers.filters.manager import FilterManager


def test_replace_special_characters():
    filters = LogFilterConfig(
        version=1,
        log_format=LogFormat.JSON,
        log_example={
            "log_level": "INFO",
            "service": "user-auth",
            "event": "login_attempt",
            "user_id": "jsmith123",
            "ip_address": "192.168.1.100",
            "success": "true",
            "duration_ms": 250,
        },
        fields=[
            JsonFieldDefinition(
                key="service", field_type=FieldType.STRING, path="service"
            ),
            JsonFieldDefinition(key="event", field_type=FieldType.STRING, path="event"),
            JsonFieldDefinition(
                key="organization_id", field_type=FieldType.STRING, path="user_id"
            ),
        ],
    )
    filter_manager = FilterManager(
        filter_config=filters, slaos_key="test", input_type=InputTypes.LOGS
    )
    assert (
        filter_manager._replace_special_characters("HelloWorld123") == "HelloWorld123"
    )
    assert filter_manager._replace_special_characters("Hello@World!") == "Hello_World_"
    assert filter_manager._replace_special_characters("Hello World") == "Hello_World"
    assert filter_manager._replace_special_characters("") == ""
    assert filter_manager._replace_special_characters("1234567890") == "1234567890"
    assert (
        filter_manager._replace_special_characters("Hello!!!World???")
        == "Hello___World___"
    )
    assert filter_manager._replace_special_characters("H@ello123!!") == "H_ello123__"
    assert filter_manager._replace_special_characters("Hello😊") == "Hello_"
    assert filter_manager._replace_special_characters("api/path") == "api/path"


def test_filter_manager_parsing_metrics(test_metrics):
    filters = MetricFilterConfig(
        version=1,
        fields=[
            MetricFieldDefinition(
                key="environment",
                hash=True,
            ),
            MetricFieldDefinition(key="region"),
            MetricFieldDefinition(key="organization_id"),
        ],
    )
    filter_manager = FilterManager(
        filter_config=filters,
        slaos_key="metrics_test",
        input_type=InputTypes.METRICS,
    )

    parsed_metrics = [
        filter_manager.parse_and_filter_metrics(metric) for metric in test_metrics
    ]

    assert len(parsed_metrics) == 5

    for metric in parsed_metrics:
        assert (
            len(metric.values["environment"]) == 64
        ), "Environment field should be hashed"


def test_parsing_metrics_no_filter(test_metrics):
    filter_manager = FilterManager(
        filter_config=None,
        slaos_key="metrics_test",
        input_type=InputTypes.METRICS,
    )
    parsed_metrics = [
        filter_manager.parse_and_filter_metrics(metric) for metric in test_metrics
    ]

    assert len(parsed_metrics) == 5
