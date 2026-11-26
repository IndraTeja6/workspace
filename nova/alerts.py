"""Maintenance and geofence alert rules for the Nova fleet system.

This module turns rolling telemetry state and geofence transitions into
:class:`Alert` records. :class:`AlertEngine` evaluates two families of
rules:

* **Maintenance** — threshold *crossings* for low fuel, high coolant
  temperature, low oil pressure, and newly observed diagnostic trouble
  codes (DTCs). A crossing fires once when the condition becomes true and
  does not re-fire while it persists; the first sample for a vehicle counts
  as a crossing.
* **Geofence** — ``GEOFENCE_ENTER`` / ``GEOFENCE_EXIT`` alerts built from
  :class:`~nova.geofence.GeofenceTransition` records.

Thresholds are read from ``config.thresholds`` and fall back to safe
defaults when missing or non-numeric. Configuration reads are guarded by a
re-entrant lock so the engine is safe to share across threads.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

from nova.geofence import GeofenceTransition
from nova.logging_setup import get_logger, log_extra
from nova.models import TelemetryEvent, VehicleState, utcnow

logger = get_logger(__name__)

__all__ = [
    "Alert",
    "AlertEngine",
    "build_alert",
    "SEVERITY_INFO",
    "SEVERITY_WARNING",
    "SEVERITY_CRITICAL",
]

SEVERITY_INFO = "info"
SEVERITY_WARNING = "warning"
SEVERITY_CRITICAL = "critical"
_VALID_SEVERITIES = (SEVERITY_INFO, SEVERITY_WARNING, SEVERITY_CRITICAL)

# Alert type identifiers.
ALERT_LOW_FUEL = "LOW_FUEL"
ALERT_HIGH_COOLANT_TEMP = "HIGH_COOLANT_TEMP"
ALERT_LOW_OIL_PRESSURE = "LOW_OIL_PRESSURE"
ALERT_DTC_FAULT = "DTC_FAULT"
ALERT_GEOFENCE_ENTER = "GEOFENCE_ENTER"
ALERT_GEOFENCE_EXIT = "GEOFENCE_EXIT"

# Default thresholds, used when config.thresholds omits or garbles a value.
DEFAULT_FUEL_PCT_MIN = 15.0
DEFAULT_COOLANT_TEMP_C_MAX = 105.0
DEFAULT_OIL_PRESSURE_KPA_MIN = 150.0


def _iso(value: datetime) -> str:
    """Serialize a datetime as ISO8601 with a trailing ``Z``."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    """Parse an ISO8601 string into a UTC-aware datetime."""
    text = str(value).strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass
class Alert:
    """A single alert emitted by the Nova rule engine."""

    vehicle_id: str
    alert_type: str
    severity: str
    message: str
    timestamp: datetime
    context: Dict[str, Any] = field(default_factory=dict)
    alert_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self) -> None:
        if self.severity not in _VALID_SEVERITIES:
            raise ValueError(
                f"severity must be one of {_VALID_SEVERITIES}, got {self.severity!r}"
            )
        if not self.alert_id:
            self.alert_id = str(uuid.uuid4())
        if self.context is None:
            self.context = {}

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of this alert."""
        return {
            "alert_id": self.alert_id,
            "vehicle_id": self.vehicle_id,
            "alert_type": self.alert_type,
            "severity": self.severity,
            "message": self.message,
            "timestamp": _iso(self.timestamp),
            "context": dict(self.context),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Alert":
        """Build an :class:`Alert` from a mapping produced by :meth:`to_dict`."""
        return cls(
            vehicle_id=str(data["vehicle_id"]),
            alert_type=str(data["alert_type"]),
            severity=str(data["severity"]),
            message=str(data["message"]),
            timestamp=_parse_iso(str(data["timestamp"])),
            context=dict(data.get("context") or {}),
            alert_id=str(data.get("alert_id") or uuid.uuid4()),
        )


def build_alert(
    vehicle_id: str,
    alert_type: str,
    severity: str,
    message: str,
    context: Optional[Mapping[str, Any]] = None,
    *,
    timestamp: Optional[datetime] = None,
    alert_id: Optional[str] = None,
) -> Alert:
    """Construct an :class:`Alert`, filling in id and timestamp defaults."""
    return Alert(
        vehicle_id=vehicle_id,
        alert_type=alert_type,
        severity=severity,
        message=message,
        timestamp=timestamp or utcnow(),
        context=dict(context or {}),
        alert_id=alert_id or str(uuid.uuid4()),
    )


def _coerce_threshold(value: Any, default: float) -> float:
    """Return ``value`` as a float, or ``default`` if missing/garbage."""
    if isinstance(value, bool) or value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class AlertEngine:
    """Evaluate maintenance and geofence rules into :class:`Alert` records."""

    def __init__(self, config: Any) -> None:
        self._lock = threading.RLock()
        raw = getattr(config, "thresholds", None)
        if not isinstance(raw, Mapping):
            raw = {}

        self._fuel_pct_min = _coerce_threshold(
            raw.get("fuel_pct_min"), DEFAULT_FUEL_PCT_MIN
        )
        self._coolant_temp_c_max = _coerce_threshold(
            raw.get("coolant_temp_c_max"), DEFAULT_COOLANT_TEMP_C_MAX
        )
        self._oil_pressure_kpa_min = _coerce_threshold(
            raw.get("oil_pressure_kpa_min"), DEFAULT_OIL_PRESSURE_KPA_MIN
        )

    @property
    def thresholds(self) -> Dict[str, float]:
        """Return a copy of the resolved thresholds."""
        with self._lock:
            return {
                "fuel_pct_min": self._fuel_pct_min,
                "coolant_temp_c_max": self._coolant_temp_c_max,
                "oil_pressure_kpa_min": self._oil_pressure_kpa_min,
            }

    def evaluate_maintenance(
        self,
        event: TelemetryEvent,
        prev_state: Optional[VehicleState],
    ) -> List[Alert]:
        """Return maintenance alerts triggered by ``event``.

        Only threshold *crossings* fire: a condition that was already true in
        ``prev_state`` does not re-fire. When ``prev_state`` is ``None`` the
        first sample is treated as a crossing.
        """
        with self._lock:
            fuel_min = self._fuel_pct_min
            coolant_max = self._coolant_temp_c_max
            oil_min = self._oil_pressure_kpa_min

        alerts: List[Alert] = []
        ts = event.timestamp

        # LOW_FUEL — fires when fuel drops below the minimum.
        prev_fuel = prev_state.fuel_pct if prev_state is not None else None
        if event.fuel_pct < fuel_min and (prev_fuel is None or prev_fuel >= fuel_min):
            alerts.append(
                build_alert(
                    vehicle_id=event.vehicle_id,
                    alert_type=ALERT_LOW_FUEL,
                    severity=SEVERITY_WARNING,
                    message=(
                        f"Fuel at {event.fuel_pct:.1f}% is below the "
                        f"{fuel_min:.1f}% minimum"
                    ),
                    context={
                        "fuel_pct": event.fuel_pct,
                        "threshold": fuel_min,
                    },
                    timestamp=ts,
                )
            )

        # HIGH_COOLANT_TEMP — fires when coolant temperature exceeds the maximum.
        coolant = event.engine_health.coolant_temp_c
        prev_coolant = (
            prev_state.engine_health.coolant_temp_c if prev_state is not None else None
        )
        if coolant > coolant_max and (
            prev_coolant is None or prev_coolant <= coolant_max
        ):
            alerts.append(
                build_alert(
                    vehicle_id=event.vehicle_id,
                    alert_type=ALERT_HIGH_COOLANT_TEMP,
                    severity=SEVERITY_CRITICAL,
                    message=(
                        f"Coolant temperature {coolant:.1f}C exceeds the "
                        f"{coolant_max:.1f}C maximum"
                    ),
                    context={
                        "coolant_temp_c": coolant,
                        "threshold": coolant_max,
                    },
                    timestamp=ts,
                )
            )

        # LOW_OIL_PRESSURE — fires when oil pressure drops below the minimum.
        oil = event.engine_health.oil_pressure_kpa
        prev_oil = (
            prev_state.engine_health.oil_pressure_kpa if prev_state is not None else None
        )
        if oil < oil_min and (prev_oil is None or prev_oil >= oil_min):
            alerts.append(
                build_alert(
                    vehicle_id=event.vehicle_id,
                    alert_type=ALERT_LOW_OIL_PRESSURE,
                    severity=SEVERITY_CRITICAL,
                    message=(
                        f"Oil pressure {oil:.1f} kPa is below the "
                        f"{oil_min:.1f} kPa minimum"
                    ),
                    context={
                        "oil_pressure_kpa": oil,
                        "threshold": oil_min,
                    },
                    timestamp=ts,
                )
            )

        # DTC_FAULT — fires for each diagnostic code not present previously.
        prev_codes = (
            set(prev_state.engine_health.dtc_codes) if prev_state is not None else set()
        )
        for code in event.engine_health.dtc_codes:
            if code in prev_codes:
                continue
            alerts.append(
                build_alert(
                    vehicle_id=event.vehicle_id,
                    alert_type=ALERT_DTC_FAULT,
                    severity=SEVERITY_WARNING,
                    message=f"New diagnostic trouble code {code}",
                    context={"dtc_code": code},
                    timestamp=ts,
                )
            )

        for alert in alerts:
            logger.info(
                "maintenance alert",
                extra=log_extra(
                    alert_id=alert.alert_id,
                    vehicle_id=alert.vehicle_id,
                    alert_type=alert.alert_type,
                    severity=alert.severity,
                ),
            )

        return alerts

    def evaluate_geofence(
        self, transitions: List[GeofenceTransition]
    ) -> List[Alert]:
        """Build geofence enter/exit alerts from ``transitions``."""
        alerts: List[Alert] = []
        for transition in transitions:
            is_enter = transition.transition == "enter"
            alert_type = ALERT_GEOFENCE_ENTER if is_enter else ALERT_GEOFENCE_EXIT
            verb = "entered" if is_enter else "exited"
            alerts.append(
                build_alert(
                    vehicle_id=transition.vehicle_id,
                    alert_type=alert_type,
                    severity=SEVERITY_INFO,
                    message=f"Vehicle {verb} geofence '{transition.geofence_name}'",
                    context={
                        "geofence_name": transition.geofence_name,
                        "transition": transition.transition,
                        "lat": transition.lat,
                        "lon": transition.lon,
                    },
                    timestamp=transition.timestamp,
                )
            )

        for alert in alerts:
            logger.info(
                "geofence alert",
                extra=log_extra(
                    alert_id=alert.alert_id,
                    vehicle_id=alert.vehicle_id,
                    alert_type=alert.alert_type,
                    severity=alert.severity,
                ),
            )

        return alerts
