"""Thread-safe in-memory vehicle state store for the Nova fleet system.

The store keeps the latest known :class:`~nova.models.VehicleState` for each
vehicle, keyed by ``vehicle_id``. It is the rolling-state primitive that
downstream rule evaluation (thresholds, geofences, trips) reads from.

The store is **process-local**: state lives only in the memory of the running
process and is lost on restart. A durable backend (Redis, a database, ...)
can replace the internal ``dict`` behind this same interface without changing
any caller — the public methods are the contract, the storage is an
implementation detail.
"""

from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from nova.models import TelemetryEvent, VehicleState

__all__ = ["VehicleStateStore"]


class VehicleStateStore:
    """In-memory, thread-safe store of the latest state per vehicle.

    All public methods are safe to call concurrently from multiple threads;
    access to the backing dictionary is serialized by a re-entrant lock.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._states: Dict[str, VehicleState] = {}

    def update(self, event: TelemetryEvent) -> VehicleState:
        """Upsert the state for ``event``'s vehicle and return the new state.

        The state is derived from the event via
        :meth:`~nova.models.VehicleState.from_event`, so the most recent
        reading always wins.
        """
        state = VehicleState.from_event(event)
        with self._lock:
            self._states[state.vehicle_id] = state
        return state

    def get(self, vehicle_id: str) -> Optional[VehicleState]:
        """Return the latest state for ``vehicle_id``, or ``None`` if unknown."""
        with self._lock:
            return self._states.get(vehicle_id)

    def all(self) -> List[VehicleState]:
        """Return a snapshot list of every known vehicle state."""
        with self._lock:
            return list(self._states.values())

    def snapshot(self) -> Dict[str, Any]:
        """Return a JSON-serializable mapping of ``vehicle_id`` -> state dict.

        Suitable for dashboards or API responses; each value is the output of
        :meth:`~nova.models.VehicleState.to_dict`.
        """
        with self._lock:
            return {vehicle_id: state.to_dict() for vehicle_id, state in self._states.items()}

    def count(self) -> int:
        """Return the number of vehicles currently tracked."""
        with self._lock:
            return len(self._states)

    def clear(self) -> None:
        """Remove every tracked vehicle state."""
        with self._lock:
            self._states.clear()
