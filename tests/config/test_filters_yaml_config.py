import pytest
from pydantic import ValidationError
from rated_parser.payloads.inputs import (  # type: ignore
    JsonFieldDefinition,
    RawTextFieldDefinition,
    FieldType as RatedParserFieldType,
    LogFormat as RatedParserLogFormat,
)
from src.config.models.filters import FiltersYamlConfig


class TestFiltersConfig:

    def test_filters_yaml_config_valid(self):
        valid_config = {
            "version": 1,
            "log_format": "raw_text",
            "log_example": "This is a raw text log example",
            "fields": [
                {
                    "key": "timestamp",
                    "value": "timestamp_value",
                    "field_type": "timestamp",
                    "format": "%Y-%m-%d %H:%M:%S",
                },
                {
                    "key": "organization_id",
                    "value": "organization_id_value",
                    "field_type": "string",
                },
            ],
        }
        config = FiltersYamlConfig(**valid_config)
        assert config.log_format == RatedParserLogFormat.RAW_TEXT
        assert config.log_example == "This is a raw text log example"
        assert len(config.fields) == 2
        assert config.fields[0].key == "timestamp"
        assert config.fields[0].value == "timestamp_value"
        assert config.fields[0].field_type == RatedParserFieldType.TIMESTAMP
        assert config.fields[0].format == "%Y-%m-%d %H:%M:%S"

    def test_filters_yaml_config_invalid_log_example(self):
        invalid_config = {
            "version": 1,
            "log_format": "raw_text",
            "log_example": {"key": "value"},  # This should be a string for RAW_TEXT
            "fields": [],
        }
        with pytest.raises(ValidationError) as exc_info:
            FiltersYamlConfig(**invalid_config)
        assert "log_example must be a string when log_format is RAW_TEXT" in str(
            exc_info.value
        )

    def test_filters_yaml_config_json_format(self):
        valid_json_config = {
            "version": 1,
            "log_format": "json_dict",
            "log_example": {"testing_key": "testing_value"},
            "fields": [
                {
                    "key": "testing_key",
                    "value": "testing_value",
                    "field_type": "string",
                    "path": "payload.testing_key",
                },
                {
                    "key": "organization_id",
                    "value": "organization_id_value",
                    "field_type": "string",
                    "path": "payload.organization_id",
                },
            ],
        }
        config = FiltersYamlConfig(**valid_json_config)
        assert config.log_format == RatedParserLogFormat.JSON
        assert config.log_example == {"testing_key": "testing_value"}

    def test_field_config_valid_timestamp(self):
        valid_field = {
            "key": "timestamp_field",
            "value": "timestamp_value",
            "field_type": "timestamp",
            "format": "%Y-%m-%d %H:%M:%S",
        }
        field = RawTextFieldDefinition(**valid_field)
        assert field.key == "timestamp_field"
        assert field.value == "timestamp_value"
        assert field.field_type == RatedParserFieldType.TIMESTAMP
        assert field.format == "%Y-%m-%d %H:%M:%S"

    def test_field_config_invalid_timestamp_format(self):
        invalid_field = {
            "key": "invalid_timestamp",
            "value": "timestamp_value",
            "field_type": "timestamp",
            "format": None,
        }
        with pytest.raises(ValidationError) as exc_info:
            RawTextFieldDefinition(**invalid_field)
        assert "Format is required for timestamp fields" in str(exc_info.value)

    def test_field_config_non_timestamp_null_format(self):
        valid_field = {
            "key": "string_field",
            "value": "string_value",
            "field_type": "string",
            "format": None,
        }
        field = RawTextFieldDefinition(**valid_field)
        assert field.key == "string_field"
        assert field.value == "string_value"
        assert field.field_type == RatedParserFieldType.STRING
        assert field.format is None

    def test_json_field_no_path_raises(self):
        invalid_field = {
            "key": "json_field",
            "field_type": "timestamp",
            "format": "%Y-%m-%d %H:%M:%S",
        }

        with pytest.raises(ValidationError) as exc_info:
            JsonFieldDefinition(**invalid_field)
            assert "value_error.missing" in str(exc_info.value)

    def test_json_field_has_path(self):
        valid_field = {
            "key": "json_field",
            "field_type": "string",
            "path": "timestamp.eventTime",
        }

        field = JsonFieldDefinition(**valid_field)
        assert field.key == "json_field"
        assert field.field_type == RatedParserFieldType.STRING
        assert field.path == "timestamp.eventTime"
