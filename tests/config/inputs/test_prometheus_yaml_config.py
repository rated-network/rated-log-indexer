import logging

import pytest
from pydantic import ValidationError
from typing import Dict, Any

from src.config.models.inputs.prometheus import (
    PrometheusQueryConfig,
    PrometheusAuthConfig,
    PrometheusConfig,
)


@pytest.fixture
def valid_prometheus_query_config_data() -> Dict[str, Any]:
    return {
        "query": "up",
        "slaos_metric_name": "uptime",
        "organization_identifier": "org1",
        "step": {"value": 1, "unit": "s"},
    }


@pytest.fixture
def valid_prometheus_auth_config_data() -> Dict[str, Any]:
    return {
        "username": "user",
        "password": "pass",
        "verify_ssl": True,
    }


@pytest.fixture
def valid_prometheus_config_data(
    valid_prometheus_query_config_data, valid_prometheus_auth_config_data
) -> Dict[str, Any]:
    return {
        "base_url": "http://prometheus:9090",
        "auth": valid_prometheus_auth_config_data,
        "queries": [valid_prometheus_query_config_data],
        "timeout": 30.0,
        "pool_connections": 5,
        "pool_maxsize": 10,
        "max_parallel_queries": 3,
    }


class TestPrometheusQueryConfig:
    def test_valid_config(self, valid_prometheus_query_config_data):
        config = PrometheusQueryConfig(**valid_prometheus_query_config_data)
        assert config.query == "up"
        assert config.slaos_metric_name == "uptime"
        assert config.organization_identifier == "org1"

    def test_fallback_org_id(self):
        config = PrometheusQueryConfig(
            query="up", slaos_metric_name="uptime", fallback_org_id="fallback_org"
        )
        assert config.fallback_org_id == "fallback_org"

    def test_missing_org_id_and_fallback(self):
        with pytest.raises(ValidationError) as exc_info:
            PrometheusQueryConfig(query="up", slaos_metric_name="uptime")
        assert "fallback_org_id" in str(exc_info.value)

    def test_warning_logged_once(self, caplog):
        # First instance
        PrometheusQueryConfig(
            query="up", slaos_metric_name="uptime", fallback_org_id="fallback_org"
        )
        assert len(caplog.records) == 1

        # Second instance shouldn't log
        PrometheusQueryConfig(
            query="up", slaos_metric_name="uptime", fallback_org_id="fallback_org"
        )
        assert len(caplog.records) == 1


class TestPrometheusAuthConfig:
    def test_valid_basic_auth(self, valid_prometheus_auth_config_data):
        config = PrometheusAuthConfig(**valid_prometheus_auth_config_data)
        assert config.username == "user"
        assert config.password == "pass"

    def test_valid_token_auth(self):
        config = PrometheusAuthConfig(token="my_token")
        assert config.token == "my_token"

    def test_valid_cert_auth(self):
        config = PrometheusAuthConfig(
            cert_path="/path/to/cert", key_path="/path/to/key"
        )
        assert config.cert_path == "/path/to/cert"
        assert config.key_path == "/path/to/key"

    def test_missing_password(self):
        with pytest.raises(ValidationError) as exc_info:
            PrometheusAuthConfig(username="user")
        assert "Password is required" in str(exc_info.value)

    def test_missing_username(self):
        with pytest.raises(ValidationError) as exc_info:
            PrometheusAuthConfig(password="pass")
        assert "Username is required" in str(exc_info.value)

    def test_missing_key_path(self):
        with pytest.raises(ValidationError) as exc_info:
            PrometheusAuthConfig(cert_path="/path/to/cert")
        assert "Key path is required" in str(exc_info.value)

    def test_missing_cert_path(self):
        with pytest.raises(ValidationError) as exc_info:
            PrometheusAuthConfig(key_path="/path/to/key")
        assert "Certificate path is required" in str(exc_info.value)

    def test_multiple_auth_methods(self):
        with pytest.raises(ValidationError) as exc_info:
            PrometheusAuthConfig(username="user", password="pass", token="token")
        assert "Only one authentication method" in str(exc_info.value)


class TestPrometheusConfig:
    def test_valid_config(self, valid_prometheus_config_data):
        config = PrometheusConfig(**valid_prometheus_config_data)
        assert str(config.base_url) == "http://prometheus:9090/"
        assert len(config.queries) == 1
        assert config.timeout == 30.0

    def test_invalid_pool_connections(self, valid_prometheus_config_data):
        valid_prometheus_config_data["pool_connections"] = 20
        valid_prometheus_config_data["pool_maxsize"] = 10
        with pytest.raises(ValidationError) as exc_info:
            PrometheusConfig(**valid_prometheus_config_data)
        assert "pool_connections cannot be greater than pool_maxsize" in str(
            exc_info.value
        )

    def test_invalid_max_parallel_queries(self, valid_prometheus_config_data):
        valid_prometheus_config_data["max_parallel_queries"] = 20
        valid_prometheus_config_data["pool_maxsize"] = 10
        with pytest.raises(ValidationError) as exc_info:
            PrometheusConfig(**valid_prometheus_config_data)
        assert "max_parallel_queries cannot be greater than pool_maxsize" in str(
            exc_info.value
        )

    def test_no_auth_config(self, valid_prometheus_config_data):
        valid_prometheus_config_data.pop("auth")
        config = PrometheusConfig(**valid_prometheus_config_data)
        assert config.auth is None

    def test_invalid_base_url(self, valid_prometheus_config_data):
        valid_prometheus_config_data["base_url"] = "invalid_url"
        with pytest.raises(ValidationError) as exc_info:
            PrometheusConfig(**valid_prometheus_config_data)
        assert "URL" in str(exc_info.value)

    @pytest.mark.parametrize(
        "field,invalid_value",
        [
            ("timeout", -1.0),
            ("pool_connections", 0),
            ("pool_maxsize", -5),
            ("max_parallel_queries", 0),
            ("retry_backoff_factor", -0.1),
            ("max_retries", 0),
        ],
    )
    def test_invalid_positive_values(
        self, valid_prometheus_config_data, field, invalid_value
    ):
        valid_prometheus_config_data[field] = invalid_value
        with pytest.raises(ValidationError):
            PrometheusConfig(**valid_prometheus_config_data)

    def test_org_id_takes_precedence_over_fallback(self):
        """Test that organization_identifier is used when both are provided."""
        config = PrometheusQueryConfig(
            query="up",
            slaos_metric_name="uptime",
            organization_identifier="primary_org",
            fallback_org_id="fallback_org",
        )
        assert config.organization_identifier == "primary_org"
        assert config.fallback_org_id == "fallback_org"

    def test_fallback_org_id(self, caplog):
        caplog.set_level(logging.WARNING)
        config = PrometheusQueryConfig(
            query="up", slaos_metric_name="uptime", fallback_org_id="fallback_org"
        )
        assert config.fallback_org_id == "fallback_org"
        assert len(caplog.records) == 1
        assert "Organization identifier not provided" in caplog.records[0].message

    def test_missing_org_id_and_fallback(self):
        with pytest.raises(ValidationError) as exc_info:
            PrometheusQueryConfig(query="up", slaos_metric_name="uptime")
        assert "fallback_org_id" in str(exc_info.value)

    def test_warning_logged_once(self, caplog):
        caplog.set_level(logging.WARNING)
        PrometheusQueryConfig._warning_logged = False

        # First instance
        PrometheusQueryConfig(
            query="up", slaos_metric_name="uptime", fallback_org_id="fallback_org"
        )
        assert len(caplog.records) == 1

        caplog.clear()  # Clear the logs

        # Second instance shouldn't log
        PrometheusQueryConfig(
            query="up", slaos_metric_name="uptime", fallback_org_id="fallback_org"
        )
        assert len(caplog.records) == 0

    def test_fallback_org_id_used_when_org_id_none(self, caplog):
        """Test that fallback_org_id is used when organization_identifier is None."""
        caplog.set_level(logging.WARNING)
        PrometheusQueryConfig._warning_logged = False

        config = PrometheusQueryConfig(
            query="up",
            slaos_metric_name="uptime",
            organization_identifier=None,
            fallback_org_id="fallback_org",
        )
        assert config.organization_identifier is None
        assert config.fallback_org_id == "fallback_org"
        assert len(caplog.records) == 1
        assert "Organization identifier not provided" in caplog.records[0].message

    def test_warning_message_contains_correct_info(self, caplog):
        """Test that warning message contains all required information."""
        caplog.set_level(logging.WARNING)
        PrometheusQueryConfig._warning_logged = False

        test_query = "rate(http_requests_total[5m])"
        test_fallback = "test_fallback_org"

        PrometheusQueryConfig(
            query=test_query,
            slaos_metric_name="requests",
            fallback_org_id=test_fallback,
        )

        assert len(caplog.records) == 1
        log_record = caplog.records[0]

        assert test_fallback in log_record.message
        assert "Organization identifier not provided" in log_record.message

        message_str = str(log_record.message)
        assert test_query in message_str
        assert test_fallback in message_str

    @pytest.fixture(autouse=True)
    def reset_warning_logged(self):
        """Reset the _warning_logged class variable before each test."""
        PrometheusQueryConfig._warning_logged = False
        yield

    def test_warning_logged_with_multiple_instances_different_fallbacks(self, caplog):
        """Test that warnings are logged for different fallback values."""
        caplog.set_level(logging.WARNING)

        # First instance with one fallback
        PrometheusQueryConfig(
            query="up", slaos_metric_name="uptime", fallback_org_id="fallback_org1"
        )
        assert len(caplog.records) == 1
        assert "fallback_org1" in caplog.records[0].message

        caplog.clear()

        # Second instance with different fallback
        PrometheusQueryConfig(
            query="up", slaos_metric_name="uptime", fallback_org_id="fallback_org2"
        )
        assert len(caplog.records) == 0

    @pytest.mark.parametrize(
        "org_id,fallback,expected_error",
        [
            (None, None, "fallback_org_id` must be provided"),
            ("", None, "fallback_org_id` must be provided"),
            (None, "", "fallback_org_id` must be provided"),
            ("", "", "fallback_org_id` must be provided"),
        ],
    )
    def test_invalid_org_id_combinations(self, org_id, fallback, expected_error):
        """Test various invalid combinations of organization_identifier and fallback_org_id."""
        with pytest.raises(ValidationError) as exc_info:
            PrometheusQueryConfig(
                query="up",
                slaos_metric_name="uptime",
                organization_identifier=org_id,
                fallback_org_id=fallback,
            )
        assert expected_error in str(exc_info.value)

    def test_empty_string_handling(self):
        """Test that empty strings are properly converted to None"""
        # Test empty organization_identifier
        with pytest.raises(ValidationError) as exc_info:
            PrometheusQueryConfig(
                query="up",
                slaos_metric_name="uptime",
                organization_identifier="",
                fallback_org_id="",
            )
        assert "fallback_org_id` must be provided" in str(exc_info.value)

        # Test with valid fallback
        config = PrometheusQueryConfig(
            query="up",
            slaos_metric_name="uptime",
            organization_identifier="",
            fallback_org_id="fallback_org",
        )
        assert config.organization_identifier is None
        assert config.fallback_org_id == "fallback_org"

    def test_whitespace_string_handling(self):
        """Test that whitespace-only strings are treated as empty strings"""
        # Test with spaces
        with pytest.raises(ValidationError) as exc_info:
            PrometheusQueryConfig(
                query="up",
                slaos_metric_name="uptime",
                organization_identifier="   ",
                fallback_org_id=None,
            )
        assert "fallback_org_id` must be provided" in str(exc_info.value)

        # Test with tabs and newlines
        with pytest.raises(ValidationError) as exc_info:
            PrometheusQueryConfig(
                query="up",
                slaos_metric_name="uptime",
                organization_identifier="\t\n",
                fallback_org_id=None,
            )
        assert "fallback_org_id` must be provided" in str(exc_info.value)

    @pytest.mark.parametrize(
        "org_id,fallback",
        [
            ("", "valid"),
            (None, "valid"),
            ("   ", "valid"),
            ("\t\n", "valid"),
        ],
    )
    def test_valid_fallback_combinations(self, org_id, fallback):
        """Test valid combinations with empty/None organization_identifier and valid fallback"""
        config = PrometheusQueryConfig(
            query="up",
            slaos_metric_name="uptime",
            organization_identifier=org_id,
            fallback_org_id=fallback,
        )
        assert config.organization_identifier is None
        assert config.fallback_org_id == "valid"

    @pytest.mark.parametrize("fallback", ["", "   ", "\t\n", None])
    def test_invalid_fallback_values(self, fallback):
        """Test that invalid fallback values raise ValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            PrometheusQueryConfig(
                query="up",
                slaos_metric_name="uptime",
                organization_identifier=None,
                fallback_org_id=fallback,
            )
        assert "fallback_org_id` must be provided" in str(exc_info.value)
