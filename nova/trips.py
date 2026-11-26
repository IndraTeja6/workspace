"""Trip detection and accumulation for the Nova fleet tracking system.

A *trip* is a contiguous period during which a vehicle is moving. This module
derives trips from the stream of :class:`~nova.models.TelemetryEvent`
readings: a trip starts when a vehicle transitions from stationary to moving,
accumulates distance, speed, fuel and driver-behaviour statistics while it is
active, and ends when the vehicle stops or after a configurable idle timeout.

:class:`TripManager` is process-local and thread-safe. Active and completed
trips are held in memory; a durable backend (Redis, a database, ...) can
replace the in-memory dictionaries behind the same interface.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from nova.config import AppConfig
from nova.logging_setup import get_logger, log_extra
from nova.models import TelemetryEvent, VehicleState, utcnow

logger = get_logger(__name__)

__all__ = ["Trip", "TripManager", "DEFAULT_IDLE_TIMEOUT_S", "BEHAVIOR_KEYS"]

#: Idle timeout (seconds) used when the configuration does not specify one.
DEFAULT_IDLE_TIMEOUT_S = 300.0

#: Driver-behaviour flags tracked per trip.
BEHAVIOR_KEYS: Tuple[str, ...] = (
    "harsh_brake",
    "harsh_accel",
    "sharp_turn",
    "speeding",
)

#: Configuration keys consulted (in order) when resolving the idle timeout.
_IDLE_TIMEOUT_KEYS: Tuple[str, ...] = (
    "idle_timeout_s",
    "idle_timeout_seconds",
    "trip_idle_timeout_s",
    "idle_timeout",
)


def _ensure_utc(value: datetime) -> datetime:
    """Return ``value`` as a UTC-aware datetime (naive values assumed UTC)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    """Serialize a datetime as ISO8601 with a trailing ``Z``."""
    return _ensure_utc(value).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    """Parse an ISO8601 string (``Z`` or explicit offset) into a UTC datetime."""
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    return _ensure_utc(datetime.fromisoformat(text))


def _empty_behavior_counts() -> Dict[str, int]:
    """Return a zeroed driver-behaviour counter for every tracked flag."""
    return {key: 0 for key in BEHAVIOR_KEYS}


def _normalize_behavior_counts(value: Any) -> Dict[str, int]:
    """Coerce arbitrary input into a complete, integer behaviour counter."""
    counts = _empty_behavior_counts()
    if isinstance(value, Mapping):
        for key in BEHAVIOR_KEYS:
            raw = value.get(key)
            if raw is None:
                continue
            try:
                counts[key] = int(raw)
            except (TypeError, ValueError):
                continue
    return counts


def _flag(behavior: Any, name: str) -> bool:
    """Read a boolean behaviour flag from a ``DriverBehavior`` or a mapping."""
    if behavior is None:
        return False
    if isinstance(behavior, Mapping):
        return bool(behavior.get(name, False))
    return bool(getattr(behavior, name, False))


def _speed_of(state: Any) -> Optional[float]:
    """Best-effort extraction of ``speed_kph`` from a state object or mapping."""
    if state is None:
        return None
    value = state.get("speed_kph") if isinstance(state, Mapping) else getattr(state, "speed_kph", None)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_positive_float(value: Any) -> Optional[float]:
    """Return ``value`` as a positive float, or ``None`` if it is not usable."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0.0:
        return None
    return number


def _idle_timeout_sources(config: Any) -> List[Mapping[str, Any]]:
    """Collect the mappings that may carry the idle-timeout setting.

    Resolution order is ``config.trips`` -> ``config.thresholds`` (including a
    nested ``thresholds['trips']``) -> built-in default. Missing attributes or
    sections are ignored rather than raising.
    """
    sources: List[Mapping[str, Any]] = []

    trips = getattr(config, "trips", None)
    if isinstance(trips, Mapping):
        sources.append(trips)

    thresholds = getattr(config, "thresholds", None)
    if isinstance(thresholds, Mapping):
        nested = thresholds.get("trips")
        if isinstance(nested, Mapping):
            sources.append(nested)
        sources.append(thresholds)

    return sources


def _resolve_idle_timeout(config: Any) -> float:
    """Resolve the trip idle timeout from config, falling back to the default.

    Never raises: a missing or malformed value simply falls through to
    :data:`DEFAULT_IDLE_TIMEOUT_S`.
    """
    for source in _idle_timeout_sources(config):
        for key in _IDLE_TIMEOUT_KEYS:
            if key in source:
                resolved = _coerce_positive_float(source.get(key))
                if resolved is not None:
                    return resolved
    return DEFAULT_IDLE_TIMEOUT_S


@dataclass
class Trip:
    """A single detected trip for a vehicle.

    ``end_ts`` is ``None`` while the trip is still active. All fields are
    JSON-serializable via :meth:`to_dict` / :meth:`from_dict`.
    """

    vehicle_id: str
    trip_id: str
    start_ts: datetime
    end_ts: Optional[datetime] = None
    start_odometer_km: float = 0.0
    end_odometer_km: float = 0.0
    distance_km: float = 0.0
    avg_speed_kph: float = 0.0
    max_speed_kph: float = 0.0
    fuel_used_pct: float = 0.0
    driver_behavior_counts: Dict[str, int] = field(default_factory=_empty_behavior_counts)

    @property
    def is_active(self) -> bool:
        """Whether the trip is still in progress (no end timestamp yet)."""
        return self.end_ts is None

    @property
    def duration_s(self) -> float:
        """Trip duration in seconds.

        For an active trip this is measured against the current time; for a
        completed trip it is ``end_ts - start_ts``. Never negative.
        """
        end = self.end_ts or utcnow()
        return max(0.0, (_ensure_utc(end) - _ensure_utc(self.start_ts)).total_seconds())

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of the trip."""
        return {
            "vehicle_id": self.vehicle_id,
            "trip_id": self.trip_id,
            "start_ts": _iso(self.start_ts),
            "end_ts": _iso(self.end_ts) if self.end_ts is not None else None,
            "start_odometer_km": self.start_odometer_km,
            "end_odometer_km": self.end_odometer_km,
            "distance_km": self.distance_km,
            "avg_speed_kph": self.avg_speed_kph,
            "max_speed_kph": self.max_speed_kph,
            "fuel_used_pct": self.fuel_used_pct,
            "driver_behavior_counts": dict(self.driver_behavior_counts),
            "is_active": self.is_active,
            "duration_s": self.duration_s,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Trip":
        """Rebuild a :class:`Trip` from :meth:`to_dict` output."""
        end_raw = data.get("end_ts")
        return cls(
            vehicle_id=str(data["vehicle_id"]),
            trip_id=str(data["trip_id"]),
            start_ts=_parse_iso(str(data["start_ts"])),
            end_ts=_parse_iso(str(end_raw)) if end_raw else None,
            start_odometer_km=float(data.get("start_odometer_km", 0.0)),
            end_odometer_km=float(data.get("end_odometer_km", 0.0)),
            distance_km=float(data.get("distance_km", 0.0)),
            avg_speed_kph=float(data.get("avg_speed_kph", 0.0)),
            max_speed_kph=float(data.get("max_speed_kph", 0.0)),
            fuel_used_pct=float(data.get("fuel_used_pct", 0.0)),
            driver_behavior_counts=_normalize_behavior_counts(data.get("driver_behavior_counts")),
        )


@dataclass
class _Accumulator:
    """Internal running totals for an active trip (not part of the public API)."""

    speed_sum: float = 0.0
    speed_samples: int = 0
    last_odometer_km: float = 0.0
    last_fuel_pct: float = 0.0
    last_ts: Optional[datetime] = None


class TripManager:
    """Detects and accumulates trips from a stream of telemetry events.

    The manager is thread-safe: every public method serializes access to its
    internal dictionaries with a re-entrant lock.
    """

    def __init__(self, config: AppConfig, idle_timeout_s: Optional[float] = None) -> None:
        self._config = config
        self._idle_timeout_s = (
            float(idle_timeout_s)
            if idle_timeout_s is not None
            else _resolve_idle_timeout(config)
        )
        self._lock = threading.RLock()
        self._active: Dict[str, Trip] = {}
        self._accumulators: Dict[str, _Accumulator] = {}
        self._completed: Dict[str, List[Trip]] = {}
        self._trip_seq = 0

    @property
    def idle_timeout_s(self) -> float:
        """The resolved idle timeout, in seconds."""
        return self._idle_timeout_s

    def on_event(
        self, event: TelemetryEvent, prev_state: Optional[VehicleState]
    ) -> Optional[Trip]:
        """Feed a telemetry event into trip detection.

        Starts a trip when the vehicle transitions from stationary to moving,
        accumulates statistics while a trip is active, and ends the trip when
        the vehicle stops or the idle timeout elapses.

        Args:
            event: The latest telemetry reading.
            prev_state: The vehicle's previous state (``None`` on first sight),
                used to detect the stationary-to-moving transition.

        Returns:
            The completed :class:`Trip` if this event ended one, otherwise
            ``None``.
        """
        vehicle_id = event.vehicle_id
        speed = float(event.speed_kph)
        prev_speed = _speed_of(prev_state)

        with self._lock:
            completed = self._close_if_idle(vehicle_id, event.timestamp)
            trip = self._active.get(vehicle_id)

            if trip is None:
                if speed > 0.0:
                    self._start_trip(event, prev_speed=prev_speed)
                return completed

            if speed <= 0.0:
                return self._finalize(vehicle_id, end_ts=event.timestamp, final_event=event)

            self._accumulate(trip, self._accumulators[vehicle_id], event)
            return completed

    def get_active_trip(self, vehicle_id: str) -> Optional[Trip]:
        """Return the in-progress trip for ``vehicle_id``, or ``None``."""
        with self._lock:
            return self._active.get(vehicle_id)

    def get_trips(self, vehicle_id: str) -> List[Trip]:
        """Return all completed trips for ``vehicle_id`` (oldest first)."""
        with self._lock:
            return list(self._completed.get(vehicle_id, ()))

    def all_trips(self) -> List[Trip]:
        """Return every completed trip across all vehicles."""
        with self._lock:
            return [trip for trips in self._completed.values() for trip in trips]

    def recent_trips(self, limit: int = 10) -> List[Trip]:
        """Return the most recently ended trips, newest first.

        Args:
            limit: Maximum number of trips to return. ``None`` returns all.
        """
        with self._lock:
            trips = [trip for trips in self._completed.values() for trip in trips]
        trips.sort(key=lambda trip: _ensure_utc(trip.end_ts or trip.start_ts), reverse=True)
        if limit is None:
            return trips
        return trips[: max(0, int(limit))]

    def sweep_idle(self, now: Optional[datetime] = None) -> List[Trip]:
        """Close any active trips that have exceeded the idle timeout.

        Args:
            now: Reference time; defaults to the current UTC time.

        Returns:
            The list of trips that were closed by this sweep.
        """
        moment = now or utcnow()
        closed: List[Trip] = []
        with self._lock:
            for vehicle_id in list(self._active.keys()):
                trip = self._close_if_idle(vehicle_id, moment)
                if trip is not None:
                    closed.append(trip)
        return closed

    def _start_trip(self, event: TelemetryEvent, prev_speed: Optional[float] = None) -> Trip:
        vehicle_id = event.vehicle_id
        speed = float(event.speed_kph)
        odometer = float(event.odometer_km)

        self._trip_seq += 1
        trip = Trip(
            vehicle_id=vehicle_id,
            trip_id=self._make_trip_id(vehicle_id, event.timestamp),
            start_ts=event.timestamp,
            start_odometer_km=odometer,
            end_odometer_km=odometer,
            max_speed_kph=speed,
            avg_speed_kph=speed,
        )
        _count_behavior(trip, event.driver_behavior)

        self._active[vehicle_id] = trip
        self._accumulators[vehicle_id] = _Accumulator(
            speed_sum=speed,
            speed_samples=1,
            last_odometer_km=odometer,
            last_fuel_pct=float(event.fuel_pct),
            last_ts=event.timestamp,
        )

        logger.info(
            "Trip started",
            extra=log_extra(
                vehicle_id=vehicle_id,
                trip_id=trip.trip_id,
                start_ts=_iso(event.timestamp),
                prev_speed_kph=prev_speed,
            ),
        )
        return trip

    def _accumulate(
        self,
        trip: Trip,
        accum: _Accumulator,
        event: TelemetryEvent,
        include_speed: bool = True,
    ) -> None:
        speed = float(event.speed_kph)
        odometer = float(event.odometer_km)
        fuel = float(event.fuel_pct)

        if include_speed:
            accum.speed_sum += speed
            accum.speed_samples += 1
            if speed > trip.max_speed_kph:
                trip.max_speed_kph = speed

        delta = odometer - accum.last_odometer_km
        if delta > 0.0:
            trip.distance_km += delta
        trip.end_odometer_km = odometer

        drop = accum.last_fuel_pct - fuel
        if drop > 0.0:
            trip.fuel_used_pct += drop

        _count_behavior(trip, event.driver_behavior)

        accum.last_odometer_km = odometer
        accum.last_fuel_pct = fuel
        accum.last_ts = event.timestamp

        if accum.speed_samples > 0:
            trip.avg_speed_kph = accum.speed_sum / accum.speed_samples

    def _finalize(
        self,
        vehicle_id: str,
        end_ts: datetime,
        final_event: Optional[TelemetryEvent] = None,
    ) -> Trip:
        trip = self._active.pop(vehicle_id)
        accum = self._accumulators.pop(vehicle_id, None)

        if final_event is not None and accum is not None:
            self._accumulate(trip, accum, final_event, include_speed=False)

        trip.end_ts = end_ts
        if accum is not None and accum.speed_samples > 0:
            trip.avg_speed_kph = accum.speed_sum / accum.speed_samples

        self._completed.setdefault(vehicle_id, []).append(trip)

        logger.info(
            "Trip ended",
            extra=log_extra(
                vehicle_id=vehicle_id,
                trip_id=trip.trip_id,
                end_ts=_iso(end_ts),
                distance_km=round(trip.distance_km, 3),
                duration_s=round(trip.duration_s, 1),
            ),
        )
        return trip

    def _close_if_idle(self, vehicle_id: str, now: datetime) -> Optional[Trip]:
        trip = self._active.get(vehicle_id)
        if trip is None:
            return None

        accum = self._accumulators.get(vehicle_id)
        last_ts = accum.last_ts if accum is not None and accum.last_ts is not None else trip.start_ts
        if (_ensure_utc(now) - _ensure_utc(last_ts)).total_seconds() > self._idle_timeout_s:
            return self._finalize(vehicle_id, end_ts=last_ts)
        return None

    def _make_trip_id(self, vehicle_id: str, start_ts: datetime) -> str:
        stamp = int(_ensure_utc(start_ts).timestamp())
        return f"{vehicle_id}-{stamp}-{self._trip_seq:04d}"


def _count_behavior(trip: Trip, behavior: Any) -> None:
    """Increment the trip's behaviour counters for every set flag."""
    counts = trip.driver_behavior_counts
    for key in BEHAVIOR_KEYS:
        if _flag(behavior, key):
            counts[key] = counts.get(key, 0) + 1
