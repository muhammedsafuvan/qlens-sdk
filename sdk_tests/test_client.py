from unittest.mock import MagicMock, patch

import pytest
from queuelens import QueueLens
from queuelens.exceptions import QueueLensConfigurationError


def test_client_initializes_with_explicit_arguments():
    client = QueueLens(
        api_key="explicit-key",
        endpoint="http://localhost:8001/events",
    )

    assert client._config.api_key == "explicit-key"
    assert client._config.endpoint == "http://localhost:8001/events"
    assert client._config.batch_size == 100


def test_client_loads_configuration_from_environment(monkeypatch):
    monkeypatch.setenv("QUEUELENS_API_KEY", "env-key")
    monkeypatch.setenv("QUEUELENS_ENDPOINT", "http://localhost:8001/events")
    monkeypatch.setenv("QUEUELENS_BATCH_SIZE", "50")

    client = QueueLens()

    assert client._config.api_key == "env-key"
    assert client._config.batch_size == 50


def test_explicit_arguments_override_environment(monkeypatch):
    monkeypatch.setenv("QUEUELENS_API_KEY", "env-key")
    monkeypatch.setenv("QUEUELENS_ENDPOINT", "http://env.example.com")
    monkeypatch.setenv("QUEUELENS_BATCH_SIZE", "50")

    client = QueueLens(
        api_key="explicit-key",
        endpoint="http://explicit.example.com",
        batch_size=25,
    )

    assert client._config.api_key == "explicit-key"
    assert client._config.endpoint == "http://explicit.example.com"
    assert client._config.batch_size == 25


@pytest.mark.parametrize(
    ("api_key", "endpoint"),
    [
        (None, None),
        ("test-key", None),
        (None, "http://localhost:8001"),
        ("", "http://localhost:8001"),
        ("test-key", ""),
    ],
)
def test_missing_or_empty_credentials_raise_configuration_error(
    monkeypatch,
    api_key,
    endpoint,
):
    monkeypatch.delenv("QUEUELENS_API_KEY", raising=False)
    monkeypatch.delenv("QUEUELENS_ENDPOINT", raising=False)

    with pytest.raises(QueueLensConfigurationError):
        QueueLens(api_key=api_key, endpoint=endpoint)


def test_invalid_configuration_raises_configuration_error():
    with pytest.raises(QueueLensConfigurationError):
        QueueLens(
            api_key="test-key",
            endpoint="http://localhost:8001",
            batch_size=0,
        )


def test_instrument_is_idempotent():
    client = QueueLens(
        api_key="test-key",
        endpoint="http://localhost:8001",
    )
    celery_app = MagicMock()

    with patch(
        "queuelens.instrumentation.celery.CeleryInstrumentation"
    ) as integration_class:
        integration = integration_class.return_value

        first = client.instrument(celery_app)
        second = client.instrument(celery_app)

        assert first is second
        integration_class.assert_called_once()
        integration.instrument.assert_called_once_with(celery_app)


def test_close_closes_all_integrations():
    client = QueueLens(
        api_key="test-key",
        endpoint="http://localhost:8001",
    )

    first = MagicMock()
    second = MagicMock()

    client._integrations = {
        1: first,
        2: second,
    }

    client.close()

    first.close.assert_called_once()
    second.close.assert_called_once()
    assert client._integrations == {}


def test_close_without_integrations_is_safe():
    client = QueueLens(
        api_key="test-key",
        endpoint="http://localhost:8001",
    )

    client.close()

    assert client._integrations == {}

def test_client_passes_configuration_to_celery_integration():
    client = QueueLens(
        api_key="test-key",
        endpoint="http://localhost:8001/internal/v1/events/batch",
        batch_size=50,
        flush_interval=2.0,
        timeout=8.0,
        buffer_capacity=2000,
        max_batches_per_cycle=20,
        shutdown_timeout=15.0,
    )

    celery_app = MagicMock()

    with patch(
        "queuelens.instrumentation.celery.CeleryInstrumentation"
    ) as integration_class:
        client.instrument(celery_app)

        config = integration_class.call_args.args[0]

        assert config.batch_size == 50
        assert config.flush_interval == 2.0
        assert config.timeout == 8.0
        assert config.buffer_capacity == 2000
        assert config.max_batches_per_cycle == 20
        assert config.shutdown_timeout == 15.0