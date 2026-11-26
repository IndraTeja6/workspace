"""Domain models for the Nova fleet tracking system.

This module defines the immutable-by-convention dataclasses that flow
through the Nova pipeline: raw telemetry events, the nested engine-health
and driver-behaviour sub-structures, and the per-vehicle rolling state.

All models are JSON-serializable via ``to_dict`` / ``from_dict`` and use
UTC-aware :class:`datetime` values internally. Timestamps are serialized
as ISO8601 strings with a trailing ``Z`` (e.g. ``2026-11-26T12:00:00Z``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping

__all__ = [
    "EngineHealth",
    "DriverBehavior",
    "TelemetryEvent",
    "VehicleState",
    "utcnow",
]


def utcnow() -> datetime:
    """Return the current time as a UTC-aware :class:`datetime`."""
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime) -> datetime:
    """Return ``value`` as a UTC-aware datetime.

    Naive datetimes are assumed to already be in UTC; aware datetimes are
    converted to UTC.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    """Serialize a datetime as ISO8601 with a trailing ``Z``."""
    return _ensure_utc(value).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    """Parse an ISO8601 string into a UTC-aware datetime.

    Accepts both ``Z`` and explicit-offset forms. Relies on the stdlib
    :func:`datetime.fromisoformat` (Python 3.11+).
    """
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    return _ensure_utc(datetime.fromisoformat(text))


@dataclass
class EngineHealth:
    """Snapshot of engine telemetry for a single event."""

    rpm: int
    coolant_temp_c: float
    oil_pressure_kpa: float
    dtc_codes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rpm": self.rpm,
            "coolant_temp_c": self.coolant_temp_c,
            "oil_pressure_kpa": self.oil_pressure_kpa,
            "dtc_codes": list(self.dtc_codes),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EngineHealth":
        return cls(
            rpm=int(data["rpm"]),
            coolant_temp_c=float(data["coolant_temp_c"]),
            oil_pressure_kpa=float(data["oil_pressure_kpa"]),
            dtc_codes=[str(code) for code in (data.get("dtc_codes") or [])],
        )


@dataclass
class DriverBehavior:
    """Boolean flags describing aggressive driving behaviour."""

    harsh_brake: bool = False
    harsh_accel: bool = False
    sharp_turn: bool = False
    speeding: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "harsh_brake": self.harsh_brake,
            "harsh_accel": self.harsh_accel,
            "sharp_turn": self.sharp_turn,
            "speeding": self.speeding,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DriverBehavior":
        return cls(
            harsh_brake=bool(data.get("harsh_brake", False)),
            harsh_accel=bool(data.get("harsh_accel", False)),
            sharp_turn=bool(data.get("sharp_turn", False)),
            speeding=bool(data.get("speeding", False)),
        )


@dataclass
class TelemetryEvent:
    """A single normalized telemetry reading from a vehicle."""

    vehicle_id: str
    timestamp: datetime
    lat: float
    lon: float
    speed_kph: float
    fuel_pct: float
    engine_health: EngineHealth
    driver_behavior: DriverBehavior
    odometer_km: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "vehicle_id": self.vehicle_id,
            "timestamp": _iso(self.timestamp),
            "lat": self.lat,
            "lon": self.lon,
            "speed_kph": self.speed_kph,
            "fuel_pct": self.fuel_pct,
            "engine_health": self.engine_health.to_dict(),
            "driver_behavior": self.driver_behavior.to_dict(),
            "odometer_km": self.odometer_km,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TelemetryEvent":
        return cls(
            vehicle_id=str(data["vehicle_id"]),
            timestamp=_parse_iso(str(data["timestamp"])),
            lat=float(data["lat"]),
            lon=float(data["lon"]),
            speed_kph=float(data["speed_kph"]),
            fuel_pct=float(data["fuel_pct"]),
            engine_health=EngineHealth.from_dict(data["engine_health"]),
            driver_behavior=DriverBehavior.from_dict(data.get("driver_behavior") or {}),
            odometer_km=float(data.get("odometer_km", 0.0)),
        )


@dataclass
class VehicleState:
    """Latest known state for a vehicle, used for rolling rule evaluation."""

    vehicle_id: str
    last_ts: datetime
    lat: float
    lon: float
    speed_kph: float
    fuel_pct: float
    engine_health: EngineHealth
    driver_behavior: DriverBehavior
    odometer_km: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "vehicle_id": self.vehicle_id,
            "last_ts": _iso(self.last_ts),
            "lat": self.lat,
            "lon": self.lon,
            "speed_kph": self.speed_kph,
            "fuel_pct": self.fuel_pct,
            "engine_health": self.engine_health.to_dict(),
            "driver_behavior": self.driver_behavior.to_dict(),
            "odometer_km": self.odometer_km,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "VehicleState":
        return cls(
            vehicle_id=str(data["vehicle_id"]),
            last_ts=_parse_iso(str(data["last_ts"])),
            lat=float(data["lat"]),
            lon=float(data["lon"]),
            speed_kph=float(data["speed_kph"]),
            fuel_pct=float(data["fuel_pct"]),
            engine_health=EngineHealth.from_dict(data["engine_health"]),
            driver_behavior=DriverBehavior.from_dict(data.get("driver_behavior") or {}),
            odometer_km=float(data.get("odometer_km", 0.0)),
        )

    @classmethod
    def from_event(cls, event: TelemetryEvent) -> "VehicleState":
        """Build a :class:`VehicleState` from a :class:`TelemetryEvent`."""
        return cls(
            vehicle_id=event.vehicle_id,
            last_ts=event.timestamp,
            lat=event.lat,
            lon=event.lon,
            speed_kph=event.speed_kph,
            fuel_pct=event.fuel_pct,
            engine_health=event.engine_health,
            driver_behavior=event.driver_behavior,
            odometer_km=event.odometer_km,
        )
