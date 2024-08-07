import pytest
from pydantic import ValidationError
from rated_parser import LogFormat as RatedParserLogFormat  # type: ignore
from rated_parser.core.payloads import FieldType as RatedParserFieldType  # type: ignore
from src.config.models.filters import FiltersYamlConfig, FieldConfig


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
                }
            ],
        }
        config = FiltersYamlConfig(**valid_config)
        assert config.log_format == RatedParserLogFormat.RAW_TEXT
        assert config.log_example == "This is a raw text log example"
        assert len(config.fields) == 1
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
            "log_example": {"key": "value"},
            "fields": [],
        }
        config = FiltersYamlConfig(**valid_json_config)
        assert config.log_format == RatedParserLogFormat.JSON
        assert config.log_example == {"key": "value"}

    def test_field_config_valid_timestamp(self):
        valid_field = {
            "key": "timestamp_field",
            "value": "timestamp_value",
            "field_type": "timestamp",
            "format": "%Y-%m-%d %H:%M:%S",
        }
        field = FieldConfig(**valid_field)
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
            FieldConfig(**invalid_field)
        assert "Format must not be null for TIMESTAMP field type" in str(exc_info.value)

    def test_field_config_non_timestamp_null_format(self):
        valid_field = {
            "key": "string_field",
            "value": "string_value",
            "field_type": "string",
            "format": None,
        }
        field = FieldConfig(**valid_field)
        assert field.key == "string_field"
        assert field.value == "string_value"
        assert field.field_type == RatedParserFieldType.STRING
        assert field.format is None
