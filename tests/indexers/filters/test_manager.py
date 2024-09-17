from rated_parser.payloads import LogFormat  # type: ignore
from rated_parser.payloads.inputs import JsonFieldDefinition, FieldType  # type: ignore

from src.config.models.filters import FiltersYamlConfig
from src.config.models.inputs.input import InputTypes
from src.indexers.filters.manager import FilterManager

filters = FiltersYamlConfig(
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
        JsonFieldDefinition(key="service", field_type=FieldType.STRING, path="service"),
        JsonFieldDefinition(key="event", field_type=FieldType.STRING, path="event"),
        JsonFieldDefinition(
            key="customer_id", field_type=FieldType.STRING, path="user_id"
        ),
    ],
)
filter_manager = FilterManager(
    filter_config=filters, integration_prefix="test", input_type=InputTypes.LOGS
)


def test_replace_special_characters():
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
