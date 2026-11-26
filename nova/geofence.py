"""Geofence definitions and enter/exit detection for the Nova fleet system.

A :class:`Geofence` is either a circle (centre point plus radius in metres)
or a polygon (an ordered ring of ``[lat, lon]`` vertices). The
:class:`GeofenceEngine` tracks, per ``(vehicle_id, geofence_name)`` pair,
whether the vehicle was last seen inside or outside each fence and emits a
:class:`GeofenceTransition` only when that membership *changes*.

The first observation of a vehicle against a fence records the initial
membership and emits nothing — a transition requires a prior known state.

All public methods are thread-safe; access to the membership map is
serialized by a re-entrant lock.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from nova.logging_setup import get_logger, log_extra
from nova.models import utcnow

logger = get_logger(__name__)

__all__ = [
    "Geofence",
    "GeofenceTransition",
    "GeofenceEngine",
    "haversine_m",
    "point_in_polygon",
]

#: Mean Earth radius in metres (IUGG).
EARTH_RADIUS_M = 6_371_008.8

SHAPE_CIRCLE = "circle"
SHAPE_POLYGON = "polygon"
_VALID_SHAPES = (SHAPE_CIRCLE, SHAPE_POLYGON)

TRANSITION_ENTER = "enter"
TRANSITION_EXIT = "exit"


def _iso(value: datetime) -> str:
    """Serialize a datetime as ISO8601 with a trailing ``Z``."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_float(value: Any, field: str) -> float:
    """Coerce ``value`` to ``float`` or raise a clear :class:`ValueError`."""
    if isinstance(value, bool) or value is None:
        raise ValueError(f"geofence field '{field}' must be a number, got {value!r}")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"geofence field '{field}' must be a number, got {value!r}"
        ) from exc


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance between two points, in metres.

    Uses the haversine formula on a spherical Earth of radius
    :data:`EARTH_RADIUS_M`.
    """
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


def point_in_polygon(lat: float, lon: float, points: Sequence[Sequence[float]]) -> bool:
    """Return ``True`` if ``(lat, lon)`` lies inside the polygon ``points``.

    ``points`` is an ordered ring of ``[lat, lon]`` vertices. The test uses
    the ray-casting (even-odd) algorithm. A ring with fewer than three
    vertices cannot enclose an area and always returns ``False``.
    """
    if not points or len(points) < 3:
        return False

    inside = False
    count = len(points)
    j = count - 1
    for i in range(count):
        lat_i = float(points[i][0])
        lon_i = float(points[i][1])
        lat_j = float(points[j][0])
        lon_j = float(points[j][1])

        if (lat_i > lat) != (lat_j > lat):
            x_intersect = (lon_j - lon_i) * (lat - lat_i) / (lat_j - lat_i) + lon_i
            if lon < x_intersect:
                inside = not inside
        j = i

    return inside


def _normalize_points(raw: Any) -> List[List[float]]:
    """Normalize polygon vertices to a list of ``[lat, lon]`` float pairs.

    Accepts either ``[[lat, lon], ...]`` or ``[{"lat": .., "lon": ..}, ...]``.
    """
    if not isinstance(raw, (list, tuple)):
        raise ValueError(
            f"geofence 'points' must be a list, got {type(raw).__name__}"
        )

    points: List[List[float]] = []
    for index, vertex in enumerate(raw):
        if isinstance(vertex, Mapping):
            if "lat" not in vertex or "lon" not in vertex:
                raise ValueError(
                    f"geofence point {index} must contain 'lat' and 'lon'"
                )
            lat = _as_float(vertex["lat"], f"points[{index}].lat")
            lon = _as_float(vertex["lon"], f"points[{index}].lon")
        elif isinstance(vertex, (list, tuple)):
            if len(vertex) < 2:
                raise ValueError(
                    f"geofence point {index} must have at least [lat, lon]"
                )
            lat = _as_float(vertex[0], f"points[{index}][0]")
            lon = _as_float(vertex[1], f"points[{index}][1]")
        else:
            raise ValueError(
                f"geofence point {index} must be a [lat, lon] pair or a "
                f"mapping, got {type(vertex).__name__}"
            )
        points.append([lat, lon])

    return points


@dataclass
class Geofence:
    """A named circular or polygonal region of interest.

    For ``shape == "circle"`` the fields ``center_lat``, ``center_lon`` and
    ``radius_m`` are populated. For ``shape == "polygon"`` the ``points``
    list holds the ordered ``[lat, lon]`` ring.
    """

    name: str
    shape: str
    center_lat: Optional[float] = None
    center_lon: Optional[float] = None
    radius_m: Optional[float] = None
    points: Optional[List[List[float]]] = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("geofence 'name' must be a non-empty string")
        self.name = self.name.strip()

        shape = str(self.shape).strip().lower()
        if shape not in _VALID_SHAPES:
            raise ValueError(
                f"geofence '{self.name}' has invalid shape {self.shape!r}; "
                f"expected one of {_VALID_SHAPES}"
            )
        self.shape = shape

        if shape == SHAPE_CIRCLE:
            if self.center_lat is None or self.center_lon is None:
                raise ValueError(
                    f"circle geofence '{self.name}' requires center_lat and center_lon"
                )
            if self.radius_m is None:
                raise ValueError(
                    f"circle geofence '{self.name}' requires radius_m"
                )
            self.center_lat = _as_float(self.center_lat, "center_lat")
            self.center_lon = _as_float(self.center_lon, "center_lon")
            self.radius_m = _as_float(self.radius_m, "radius_m")
            if self.radius_m <= 0:
                raise ValueError(
                    f"circle geofence '{self.name}' radius_m must be > 0"
                )
            self.points = None
        else:  # polygon
            if not self.points:
                raise ValueError(
                    f"polygon geofence '{self.name}' requires at least 3 points"
                )
            self.points = _normalize_points(self.points)
            if len(self.points) < 3:
                raise ValueError(
                    f"polygon geofence '{self.name}' requires at least 3 points, "
                    f"got {len(self.points)}"
                )
            self.center_lat = None
            self.center_lon = None
            self.radius_m = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Geofence":
        """Build a :class:`Geofence` from a mapping, inferring the shape.

        The shape is taken from ``shape`` when present; otherwise it is
        inferred: a ``points`` key implies ``polygon``, while any of
        ``center_lat`` / ``center_lon`` / ``radius_m`` (or the ``radius``
        alias) implies ``circle``.

        Raises:
            ValueError: If the mapping is malformed or missing required
                fields for the resolved shape.
        """
        if not isinstance(data, Mapping):
            raise ValueError(
                f"geofence definition must be a mapping, got {type(data).__name__}"
            )

        name = data.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("geofence definition requires a non-empty 'name'")

        raw_shape = data.get("shape")
        if raw_shape is not None:
            shape = str(raw_shape).strip().lower()
            if shape not in _VALID_SHAPES:
                raise ValueError(
                    f"geofence '{name}' has invalid shape {raw_shape!r}; "
                    f"expected one of {_VALID_SHAPES}"
                )
        elif data.get("points"):
            shape = SHAPE_POLYGON
        elif any(
            key in data for key in ("center_lat", "center_lon", "radius_m", "radius")
        ):
            shape = SHAPE_CIRCLE
        else:
            raise ValueError(
                f"geofence '{name}' has no 'shape' and none could be inferred "
                f"(provide 'points' for a polygon or 'center_lat'/'center_lon'/"
                f"'radius_m' for a circle)"
            )

        if shape == SHAPE_CIRCLE:
            radius = data.get("radius_m", data.get("radius"))
            return cls(
                name=name,
                shape=SHAPE_CIRCLE,
                center_lat=data.get("center_lat"),
                center_lon=data.get("center_lon"),
                radius_m=radius,
            )

        return cls(name=name, shape=SHAPE_POLYGON, points=data.get("points"))

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of this geofence."""
        if self.shape == SHAPE_CIRCLE:
            return {
                "name": self.name,
                "shape": SHAPE_CIRCLE,
                "center_lat": self.center_lat,
                "center_lon": self.center_lon,
                "radius_m": self.radius_m,
            }
        return {
            "name": self.name,
            "shape": SHAPE_POLYGON,
            "points": [list(point) for point in (self.points or [])],
        }

    def contains(self, lat: float, lon: float) -> bool:
        """Return ``True`` if ``(lat, lon)`` lies within this geofence."""
        if self.shape == SHAPE_CIRCLE:
            distance = haversine_m(
                float(lat), float(lon), self.center_lat, self.center_lon
            )
            return distance <= self.radius_m
        return point_in_polygon(float(lat), float(lon), self.points or [])


@dataclass
class GeofenceTransition:
    """A single enter or exit event for a vehicle against a geofence."""

    vehicle_id: str
    geofence_name: str
    transition: str
    lat: float
    lon: float
    timestamp: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.transition not in (TRANSITION_ENTER, TRANSITION_EXIT):
            raise ValueError(
                f"transition must be {TRANSITION_ENTER!r} or {TRANSITION_EXIT!r}, "
                f"got {self.transition!r}"
            )
        if self.timestamp is None:
            self.timestamp = utcnow()

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of this transition."""
        return {
            "vehicle_id": self.vehicle_id,
            "geofence_name": self.geofence_name,
            "transition": self.transition,
            "lat": self.lat,
            "lon": self.lon,
            "timestamp": _iso(self.timestamp),
        }


class GeofenceEngine:
    """Evaluate vehicle positions against a set of geofences.

    The engine remembers the last known inside/outside membership for each
    ``(vehicle_id, geofence_name)`` pair so that it can emit a transition
    only when membership actually changes.
    """

    def __init__(self, geofences: Optional[List[Geofence]] = None) -> None:
        self._lock = threading.RLock()
        self._geofences: List[Geofence] = list(geofences or [])
        self._inside: Dict[Tuple[str, str], bool] = {}

    @property
    def geofences(self) -> List[Geofence]:
        """Return a shallow copy of the configured geofences."""
        with self._lock:
            return list(self._geofences)

    def contains(self, geofence: Geofence, lat: float, lon: float) -> bool:
        """Return ``True`` if ``(lat, lon)`` is inside ``geofence``."""
        return geofence.contains(lat, lon)

    def evaluate(
        self,
        vehicle_id: str,
        lat: float,
        lon: float,
        timestamp: Optional[datetime] = None,
    ) -> List[GeofenceTransition]:
        """Evaluate a position and return any membership transitions.

        The first observation of a vehicle against a given fence records the
        initial membership and emits nothing. Subsequent calls emit an
        ``enter`` or ``exit`` transition only when membership flips.
        """
        ts = timestamp or utcnow()
        transitions: List[GeofenceTransition] = []

        with self._lock:
            for geofence in self._geofences:
                inside = geofence.contains(lat, lon)
                key = (vehicle_id, geofence.name)
                previous = self._inside.get(key)

                if previous is None:
                    self._inside[key] = inside
                    continue

                if inside == previous:
                    continue

                self._inside[key] = inside
                transition = GeofenceTransition(
                    vehicle_id=vehicle_id,
                    geofence_name=geofence.name,
                    transition=TRANSITION_ENTER if inside else TRANSITION_EXIT,
                    lat=float(lat),
                    lon=float(lon),
                    timestamp=ts,
                )
                transitions.append(transition)

        for transition in transitions:
            logger.info(
                "geofence transition",
                extra=log_extra(
                    vehicle_id=transition.vehicle_id,
                    geofence_name=transition.geofence_name,
                    transition=transition.transition,
                    lat=transition.lat,
                    lon=transition.lon,
                ),
            )

        return transitions

    def reset(self) -> None:
        """Forget all remembered membership state."""
        with self._lock:
            self._inside.clear()
