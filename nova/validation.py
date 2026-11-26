"""Validation and normalization for inbound Nova telemetry events.

``validate_and_normalize`` is the single entry point used by the ingest
path. It type-checks and coerces a raw payload into a
:class:`~nova.models.TelemetryEvent`, enforcing presence and range
constraints. Any failure is logged as a structured warning and reported
as ``None`` so the caller can route the offending message to the
dead-letter queue (DLQ).
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from nova.logging_setup import get_logger, log_extra
from nova.models import (
    DriverBehavior,
    EngineHealth,
    TelemetryEvent,
    _ensure_utc,
    _parse_iso,
)

__all__ = ["validate_and_normalize", "to_json_bytes"]

logger = get_logger(__name__)

# Inclusive range constraints.
_LAT_RANGE = (-90.0, 90.0)
_LON_RANGE = (-180.0, 180.0)
_FUEL_RANGE = (0.0, 100.0)
_COOLANT_RANGE = (-40.0, 200.0)

# Minimum-only constraints.
_SPEED_MIN = 0.0
_ODOMETER_MIN = 0.0
_RPM_MIN = 0
_OIL_PRESSURE_MIN = 0.0

_REQUIRED_FIELDS = ("vehicle_id", "timestamp", "lat", "lon", "speed_kph", "fuel_pct")
_ENGINE_REQUIRED_FIELDS = ("rpm", "coolant_temp_c", "oil_pressure_kpa")


class _Invalid(Exception):
    """Internal signal that a raw payload failed validation."""


def _coerce_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise _Invalid(f"{field_name} must be a number, got bool")
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise _Invalid(
            f"{field_name} must be a number, got {type(value).__name__}"
        ) from None
    if not math.isfinite(result):
        raise _Invalid(f"{field_name} must be a finite number")
    return result


def _coerce_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise _Invalid(f"{field_name} must be an integer, got bool")
    try:
        return int(value)
    except (TypeError, ValueError):
        raise _Invalid(
            f"{field_name} must be an integer, got {type(value).__name__}"
        ) from None


def _coerce_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "1", "yes"):
            return True
        if text in ("false", "0", "no", ""):
            return False
    raise _Invalid(f"{field_name} must be a boolean")


def _check_range(field_name: str, value: float, low: float, high: float) -> None:
    if value < low or value > high:
        raise _Invalid(f"{field_name}={value} out of range [{low}, {high}]")


def _check_min(field_name: str, value: float, low: float) -> None:
    if value < low:
        raise _Invalid(f"{field_name}={value} must be >= {low}")


def _parse_timestamp(value: Any) -> datetime:
    """Parse an ISO8601 string or epoch number into a UTC-aware datetime."""
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, bool):
        raise _Invalid("timestamp must be an ISO8601 string or epoch number")
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise _Invalid("timestamp is empty")
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except ValueError:
            pass
        try:
            return _parse_iso(text)
        except ValueError as exc:
            raise _Invalid(f"timestamp is not valid ISO8601: {exc}") from None
    raise _Invalid(f"timestamp has unsupported type {type(value).__name__}")


def _build_engine_health(value: Any) -> EngineHealth:
    if value is None:
        raise _Invalid("missing required field: engine_health")
    if not isinstance(value, Mapping):
        raise _Invalid("engine_health must be an object")

    missing = [f for f in _ENGINE_REQUIRED_FIELDS if value.get(f) is None]
    if missing:
        raise _Invalid(
            "missing required field(s): "
            + ", ".join(f"engine_health.{f}" for f in missing)
        )

    rpm = _coerce_int(value.get("rpm"), "engine_health.rpm")
    coolant = _coerce_float(value.get("coolant_temp_c"), "engine_health.coolant_temp_c")
    oil = _coerce_float(value.get("oil_pressure_kpa"), "engine_health.oil_pressure_kpa")

    _check_min("engine_health.rpm", rpm, _RPM_MIN)
    _check_range("engine_health.coolant_temp_c", coolant, *_COOLANT_RANGE)
    _check_min("engine_health.oil_pressure_kpa", oil, _OIL_PRESSURE_MIN)

    dtc_raw = value.get("dtc_codes") or []
    if not isinstance(dtc_raw, (list, tuple)):
        raise _Invalid("engine_health.dtc_codes must be a list")

    return EngineHealth(
        rpm=rpm,
        coolant_temp_c=coolant,
        oil_pressure_kpa=oil,
        dtc_codes=[str(code) for code in dtc_raw],
    )


def _build_driver_behavior(value: Any) -> DriverBehavior:
    if value is None:
        return DriverBehavior()
    if not isinstance(value, Mapping):
        raise _Invalid("driver_behavior must be an object")
    return DriverBehavior(
        harsh_brake=_coerce_bool(value.get("harsh_brake", False), "driver_behavior.harsh_brake"),
        harsh_accel=_coerce_bool(value.get("harsh_accel", False), "driver_behavior.harsh_accel"),
        sharp_turn=_coerce_bool(value.get("sharp_turn", False), "driver_behavior.sharp_turn"),
        speeding=_coerce_bool(value.get("speeding", False), "driver_behavior.speeding"),
    )


def _build_event(raw: Mapping[str, Any]) -> TelemetryEvent:
    missing = [f for f in _REQUIRED_FIELDS if raw.get(f) is None]
    if missing:
        raise _Invalid(f"missing required field(s): {', '.join(missing)}")

    vehicle_id = raw.get("vehicle_id")
    if not isinstance(vehicle_id, str) or not vehicle_id.strip():
        raise _Invalid("vehicle_id must be a non-empty string")

    timestamp = _parse_timestamp(raw.get("timestamp"))

    lat = _coerce_float(raw.get("lat"), "lat")
    lon = _coerce_float(raw.get("lon"), "lon")
    speed_kph = _coerce_float(raw.get("speed_kph"), "speed_kph")
    fuel_pct = _coerce_float(raw.get("fuel_pct"), "fuel_pct")
    odometer_km = _coerce_float(raw.get("odometer_km", 0.0), "odometer_km")

    _check_range("lat", lat, *_LAT_RANGE)
    _check_range("lon", lon, *_LON_RANGE)
    _check_min("speed_kph", speed_kph, _SPEED_MIN)
    _check_range("fuel_pct", fuel_pct, *_FUEL_RANGE)
    _check_min("odometer_km", odometer_km, _ODOMETER_MIN)

    engine_health = _build_engine_health(raw.get("engine_health"))
    driver_behavior = _build_driver_behavior(raw.get("driver_behavior"))

    return TelemetryEvent(
        vehicle_id=vehicle_id.strip(),
        timestamp=timestamp,
        lat=lat,
        lon=lon,
        speed_kph=speed_kph,
        fuel_pct=fuel_pct,
        engine_health=engine_health,
        driver_behavior=driver_behavior,
        odometer_km=odometer_km,
    )


def validate_and_normalize(raw: dict) -> Optional[TelemetryEvent]:
    """Validate and normalize a raw telemetry payload.

    Args:
        raw: The decoded message payload.

    Returns:
        A normalized :class:`~nova.models.TelemetryEvent`, or ``None`` if
        the payload is invalid. Invalid payloads are logged as structured
        warnings so the caller can route them to the DLQ.
    """
    if not isinstance(raw, Mapping):
        logger.warning(
            "telemetry rejected",
            extra=log_extra(
                reason="payload is not a mapping",
                payload_type=type(raw).__name__,
            ),
        )
        return None

    try:
        return _build_event(raw)
    except _Invalid as exc:
        logger.warning(
            "telemetry rejected",
            extra=log_extra(reason=str(exc), vehicle_id=raw.get("vehicle_id")),
        )
        return None


def to_json_bytes(event: TelemetryEvent) -> bytes:
    """Serialize a telemetry event to compact UTF-8 JSON bytes for Kafka."""
    return json.dumps(
        event.to_dict(), separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
