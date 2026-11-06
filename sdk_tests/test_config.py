import pytest
from queuelens.config import QueueLensConfig


def test_from_env_uses_defaults(monkeypatch):
    monkeypatch.setenv("QUEUELENS_API_KEY", "test-key")
    monkeypatch.setenv("QUEUELENS_ENDPOINT", "http://localhost:8001/events")

    config = QueueLensConfig.from_env()

    assert config.api_key == "test-key"
    assert config.endpoint == "http://localhost:8001/events"
    assert config.batch_size == 100
    assert config.flush_interval == 5.0
    assert config.timeout == 5.0
    assert config.buffer_capacity == 1000
    assert config.max_batches_per_cycle == 10
    assert config.shutdown_timeout == 10.0


def test_from_env_loads_all_settings(monkeypatch):
    monkeypatch.setenv("QUEUELENS_API_KEY", "test-key")
    monkeypatch.setenv("QUEUELENS_ENDPOINT", "http://localhost:8001/events")
    monkeypatch.setenv("QUEUELENS_BATCH_SIZE", "50")
    monkeypatch.setenv("QUEUELENS_FLUSH_INTERVAL", "2")
    monkeypatch.setenv("QUEUELENS_TIMEOUT", "8")
    monkeypatch.setenv("QUEUELENS_BUFFER_CAPACITY", "2000")
    monkeypatch.setenv("QUEUELENS_MAX_BATCHES_PER_CYCLE", "20")
    monkeypatch.setenv("QUEUELENS_SHUTDOWN_TIMEOUT", "15")

    config = QueueLensConfig.from_env()

    assert config.batch_size == 50
    assert config.flush_interval == 2.0
    assert config.timeout == 8.0
    assert config.buffer_capacity == 2000
    assert config.max_batches_per_cycle == 20
    assert config.shutdown_timeout == 15.0


def test_explicit_arguments_override_environment(monkeypatch):
    monkeypatch.setenv("QUEUELENS_API_KEY", "env-key")
    monkeypatch.setenv("QUEUELENS_ENDPOINT", "http://env.example.com")
    monkeypatch.setenv("QUEUELENS_BATCH_SIZE", "50")

    config = QueueLensConfig.from_env(
        api_key="explicit-key",
        endpoint="http://explicit.example.com",
        batch_size=25,
    )

    assert config.api_key == "explicit-key"
    assert config.endpoint == "http://explicit.example.com"
    assert config.batch_size == 25


@pytest.mark.parametrize(
    ("variable", "message"),
    [
        ("QUEUELENS_API_KEY", "QUEUELENS_API_KEY is required"),
        ("QUEUELENS_ENDPOINT", "QUEUELENS_ENDPOINT is required"),
    ],
)
def test_missing_required_environment_variables(
    monkeypatch,
    variable,
    message,
):
    monkeypatch.delenv("QUEUELENS_API_KEY", raising=False)
    monkeypatch.delenv("QUEUELENS_ENDPOINT", raising=False)

    if variable == "QUEUELENS_API_KEY":
        monkeypatch.setenv("QUEUELENS_ENDPOINT", "http://localhost:8001")
    else:
        monkeypatch.setenv("QUEUELENS_API_KEY", "test-key")

    with pytest.raises(ValueError, match=message):
        QueueLensConfig.from_env()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("batch_size", 0),
        ("flush_interval", 0),
        ("timeout", 0),
        ("buffer_capacity", 0),
        ("max_batches_per_cycle", 0),
        ("shutdown_timeout", 0),
    ],
)
def test_invalid_configuration_values(field, value):
    with pytest.raises(ValueError):
        QueueLensConfig(
            api_key="test-key",
            endpoint="http://localhost:8001",
            **{field: value},
        )


def test_invalid_numeric_environment_value(monkeypatch):
    monkeypatch.setenv("QUEUELENS_API_KEY", "test-key")
    monkeypatch.setenv("QUEUELENS_ENDPOINT", "http://localhost:8001")
    monkeypatch.setenv("QUEUELENS_BATCH_SIZE", "invalid")

    with pytest.raises(ValueError):
        QueueLensConfig.from_env()