"""Configuration loading and validation for the Nova fleet tracking system.

Configuration is sourced from a YAML file whose path is taken from the
``NOVA_CONFIG`` environment variable (defaulting to ``config.yaml``) and is
then overlaid with environment-variable overrides. The merged result is
exposed as a typed, immutable :class:`AppConfig` dataclass.

Loading is fail-fast: any missing file, malformed YAML, or invalid value
raises :class:`ConfigError` with a clear, actionable message.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml

DEFAULT_CONFIG_PATH = "config.yaml"

ENV_CONFIG_PATH = "NOVA_CONFIG"
ENV_KAFKA_BOOTSTRAP_SERVERS = "NOVA_KAFKA_BOOTSTRAP_SERVERS"
ENV_KAFKA_GROUP_ID = "NOVA_KAFKA_GROUP_ID"
ENV_LOG_LEVEL = "NOVA_LOG_LEVEL"

DEFAULT_LOG_LEVEL = "INFO"


class ConfigError(Exception):
    """Raised when configuration is missing, malformed, or invalid."""


@dataclass(frozen=True)
class KafkaTopics:
    """Kafka topic names used by the Nova pipeline."""

    raw: str
    processed: str
    alerts: str
    dlq: str


@dataclass(frozen=True)
class KafkaSecurity:
    """Kafka security placeholders.

    Values are intentionally optional; secrets are expected to be injected
    at runtime via the environment rather than committed to the YAML file.
    """

    protocol: str = "SASL_PLAINTEXT"
    mechanism: str = "SCRAM-SHA-512"
    username: Optional[str] = None
    password: Optional[str] = None
    ca_file: Optional[str] = None


@dataclass(frozen=True)
class KafkaConfig:
    """Kafka connection and topic configuration."""

    bootstrap_servers: str
    topics: KafkaTopics
    group_id: str
    security: KafkaSecurity = field(default_factory=KafkaSecurity)


@dataclass(frozen=True)
class AppConfig:
    """Fully resolved, immutable application configuration."""

    kafka: KafkaConfig
    thresholds: Dict[str, Any] = field(default_factory=dict)
    geofences: List[Dict[str, Any]] = field(default_factory=list)
    log_level: str = DEFAULT_LOG_LEVEL
    source_path: str = DEFAULT_CONFIG_PATH


def _env_override(name: str) -> Optional[str]:
    """Return a non-empty, stripped environment value, or ``None``."""
    value = os.environ.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _read_yaml(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        raise ConfigError(
            f"Configuration file not found: {path}. "
            f"Set {ENV_CONFIG_PATH} to a valid path or create the file."
        )
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in configuration file {path}: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read configuration file {path}: {exc}") from exc

    if data is None:
        raise ConfigError(f"Configuration file {path} is empty")
    if not isinstance(data, Mapping):
        raise ConfigError(
            f"Configuration root in {path} must be a mapping, got {type(data).__name__}"
        )
    return data


def _require_mapping(data: Mapping[str, Any], key: str, path: Path) -> Mapping[str, Any]:
    value = data.get(key)
    if value is None:
        raise ConfigError(f"Missing required section '{key}' in {path}")
    if not isinstance(value, Mapping):
        raise ConfigError(
            f"Section '{key}' in {path} must be a mapping, got {type(value).__name__}"
        )
    return value


def _require_str(data: Mapping[str, Any], key: str, context: str) -> str:
    value = data.get(key)
    if value is None:
        raise ConfigError(f"Missing required key '{key}' in {context}")
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"Key '{key}' in {context} must be a non-empty string")
    return value.strip()


def _opt_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def load_config(path: Optional[str] = None) -> AppConfig:
    """Load, merge, and validate the Nova application configuration.

    Args:
        path: Explicit path to the YAML file. When omitted, ``NOVA_CONFIG``
            is consulted, falling back to ``config.yaml``.

    Returns:
        A fully resolved :class:`AppConfig`.

    Raises:
        ConfigError: If the file is missing, malformed, or contains invalid
            or missing required values.
    """
    config_path = Path(
        path or os.environ.get(ENV_CONFIG_PATH) or DEFAULT_CONFIG_PATH
    ).expanduser()

    raw = _read_yaml(config_path)

    kafka_raw = _require_mapping(raw, "kafka", config_path)
    topics_raw = _require_mapping(kafka_raw, "topics", config_path)

    security_raw = kafka_raw.get("security") or {}
    if not isinstance(security_raw, Mapping):
        raise ConfigError(
            f"Section 'kafka.security' in {config_path} must be a mapping, "
            f"got {type(security_raw).__name__}"
        )

    bootstrap_servers = _env_override(ENV_KAFKA_BOOTSTRAP_SERVERS) or _require_str(
        kafka_raw, "bootstrap_servers", f"kafka section of {config_path}"
    )
    group_id = _env_override(ENV_KAFKA_GROUP_ID) or _require_str(
        kafka_raw, "group_id", f"kafka section of {config_path}"
    )

    topics = KafkaTopics(
        raw=_require_str(topics_raw, "raw", f"kafka.topics of {config_path}"),
        processed=_require_str(topics_raw, "processed", f"kafka.topics of {config_path}"),
        alerts=_require_str(topics_raw, "alerts", f"kafka.topics of {config_path}"),
        dlq=_require_str(topics_raw, "dlq", f"kafka.topics of {config_path}"),
    )

    security = KafkaSecurity(
        protocol=_opt_str(security_raw.get("protocol")) or "SASL_PLAINTEXT",
        mechanism=_opt_str(security_raw.get("mechanism")) or "SCRAM-SHA-512",
        username=_opt_str(security_raw.get("username")),
        password=_opt_str(security_raw.get("password")),
        ca_file=_opt_str(security_raw.get("ca_file")),
    )

    thresholds = raw.get("thresholds") or {}
    if not isinstance(thresholds, Mapping):
        raise ConfigError(
            f"Section 'thresholds' in {config_path} must be a mapping, "
            f"got {type(thresholds).__name__}"
        )

    geofences = raw.get("geofences") or []
    if not isinstance(geofences, list):
        raise ConfigError(
            f"Section 'geofences' in {config_path} must be a list, "
            f"got {type(geofences).__name__}"
        )

    log_level = (
        _env_override(ENV_LOG_LEVEL) or _opt_str(raw.get("log_level")) or DEFAULT_LOG_LEVEL
    ).upper()

    return AppConfig(
        kafka=KafkaConfig(
            bootstrap_servers=bootstrap_servers,
            topics=topics,
            group_id=group_id,
            security=security,
        ),
        thresholds=dict(thresholds),
        geofences=list(geofences),
        log_level=log_level,
        source_path=str(config_path),
    )
