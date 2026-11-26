"""Nova — a real-time fleet tracking and telemetry processing system.

Nova ingests vehicle telemetry from a streaming source, applies
threshold and geofence rules, and emits processed events and alerts.
This package contains the core configuration and logging primitives
used by the Nova services.
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
