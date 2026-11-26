"""Nova streaming processor — main pipeline skeleton.

Reads raw telemetry from Kafka, validates and normalizes each event,
publishes the normalized event to the processed topic, and commits the
source offset. Invalid payloads are routed to the dead-letter topic.

The per-vehicle state store and trip detector are wired into the pipeline,
and a background sweeper thread closes out trips that have gone idle so that
trips end even when a vehicle stops emitting events. Geofence transitions
and maintenance threshold crossings are evaluated per event and published to
the alerts topic.
"""

from __future__ import annotations

import json
import signal
import threading
from typing import Any, List, Optional

from nova.alerts import Alert, AlertEngine
from nova.config import AppConfig, ConfigError, load_config
from nova.geofence import Geofence, GeofenceEngine
from nova.kafka_io import (
    close_consumer,
    commit_message,
    consume_loop,
    flush_producer,
    make_consumer,
    make_producer,
    produce_json,
    send_to_dlq,
)
from nova.logging_setup import get_logger, log_extra, setup_logging
from nova.models import TelemetryEvent
from nova.state import VehicleStateStore
from nova.trips import Trip, TripManager
from nova.validation import to_json_bytes, validate_and_normalize

logger = get_logger(__name__)

__all__ = ["Processor", "main"]

#: How often the background sweeper closes idle trips, in seconds.
DEFAULT_SWEEP_INTERVAL_S = 30.0


class Processor:
    """Owns the Kafka clients and the per-message processing loop."""

    def __init__(
        self,
        config: AppConfig,
        sweep_interval_s: float = DEFAULT_SWEEP_INTERVAL_S,
    ) -> None:
        self._config = config
        self._consumer = make_consumer(config)
        self._producer = make_producer(config)
        self._stop_event = threading.Event()
        self.state_store = VehicleStateStore()
        self.trip_manager = TripManager(config)
        self.geofence_engine = GeofenceEngine(self._load_geofences(config))
        self.alert_engine = AlertEngine(config)
        interval = float(sweep_interval_s)
        self._sweep_interval_s = interval if interval > 0 else DEFAULT_SWEEP_INTERVAL_S
        self._sweeper: Optional[threading.Thread] = None

    def request_stop(self, *_: Any) -> None:
        """Signal the processing loop to shut down gracefully."""
        if not self._stop_event.is_set():
            logger.info("shutdown requested")
            self._stop_event.set()

    def run(self) -> None:
        """Consume and process messages until a stop is requested."""
        logger.info(
            "processor started",
            extra=log_extra(
                raw_topic=self._config.kafka.topics.raw,
                processed_topic=self._config.kafka.topics.processed,
                sweep_interval_s=self._sweep_interval_s,
            ),
        )
        self._start_sweeper()
        try:
            for msg, raw in consume_loop(self._consumer, stop_event=self._stop_event):
                self._handle_message(msg, raw)
        finally:
            self._shutdown()

    def _handle_message(self, msg: Any, raw: bytes) -> None:
        event = self._decode(raw)
        if event is None:
            send_to_dlq(self._producer, self._config, raw, "validation failed")
            commit_message(self._consumer, msg)
            return

        prev_state = self.state_store.get(event.vehicle_id)
        completed_trip = self.trip_manager.on_event(event, prev_state)
        self.state_store.update(event)
        if completed_trip is not None:
            self._log_completed_trip(completed_trip)

        try:
            transitions = self.geofence_engine.evaluate(
                event.vehicle_id, event.lat, event.lon, timestamp=event.timestamp
            )
            alerts = self.alert_engine.evaluate_geofence(
                transitions
            ) + self.alert_engine.evaluate_maintenance(event, prev_state)
        except Exception as exc:  # noqa: BLE001 - alerting must not stall the pipeline
            logger.error(
                "alert evaluation failed",
                extra=log_extra(vehicle_id=event.vehicle_id, error=str(exc)),
            )
            alerts = []
        for alert in alerts:
            self.publish_alert(alert)

        try:
            produce_json(
                self._producer,
                self._config.kafka.topics.processed,
                key=event.vehicle_id,
                payload=to_json_bytes(event),
            )
        except Exception as exc:  # noqa: BLE001 - keep the pipeline alive
            logger.error(
                "failed to publish processed event",
                extra=log_extra(vehicle_id=event.vehicle_id, error=str(exc)),
            )
            return

        commit_message(self._consumer, msg)

    def _decode(self, raw: bytes) -> Optional[TelemetryEvent]:
        try:
            decoded = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning(
                "telemetry rejected",
                extra=log_extra(reason=f"invalid json: {exc}"),
            )
            return None
        return validate_and_normalize(decoded)

    @staticmethod
    def _load_geofences(config: AppConfig) -> List[Geofence]:
        """Parse configured geofences, skipping (and logging) invalid entries.

        A malformed geofence definition is logged and dropped rather than
        aborting startup, so a single bad entry cannot take the processor down.
        """
        geofences: List[Geofence] = []
        for index, entry in enumerate(config.geofences or []):
            try:
                geofences.append(Geofence.from_dict(entry))
            except (ValueError, TypeError) as exc:
                logger.error(
                    "skipping invalid geofence",
                    extra=log_extra(index=index, error=str(exc)),
                )
        return geofences

    def publish_alert(self, alert: Alert) -> None:
        """Publish ``alert`` to the alerts topic; never raises.

        A publish failure is logged and swallowed so a transient broker issue
        cannot stall the processing pipeline.
        """
        try:
            payload = json.dumps(
                alert.to_dict(), separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
            produce_json(
                self._producer,
                self._config.kafka.topics.alerts,
                key=alert.vehicle_id,
                payload=payload,
            )
        except Exception as exc:  # noqa: BLE001 - alert publishing must not stall
            logger.error(
                "failed to publish alert",
                extra=log_extra(
                    vehicle_id=alert.vehicle_id,
                    alert_type=alert.alert_type,
                    error=str(exc),
                ),
            )
            return
        logger.info(
            "alert published",
            extra=log_extra(
                alert_id=alert.alert_id,
                vehicle_id=alert.vehicle_id,
                alert_type=alert.alert_type,
                severity=alert.severity,
            ),
        )

    def _start_sweeper(self) -> None:
        """Start the daemon thread that closes idle trips periodically."""
        self._sweeper = threading.Thread(
            target=self._sweep_loop, name="trip-sweeper", daemon=True
        )
        self._sweeper.start()

    def _sweep_loop(self) -> None:
        """Close idle trips on an interval and emit a periodic health log."""
        while not self._stop_event.wait(self._sweep_interval_s):
            try:
                closed = self.trip_manager.sweep_idle()
            except Exception as exc:  # noqa: BLE001 - keep the sweeper alive
                logger.error("trip sweep failed", extra=log_extra(error=str(exc)))
                closed = []
            for trip in closed:
                self._log_completed_trip(trip)
            logger.info(
                "health",
                extra=log_extra(
                    vehicles=self.state_store.count(),
                    trips_closed=len(closed),
                ),
            )

    def _log_completed_trip(self, trip: Trip) -> None:
        logger.info(
            "trip completed",
            extra=log_extra(
                vehicle_id=trip.vehicle_id,
                trip_id=trip.trip_id,
                distance_km=round(trip.distance_km, 3),
                duration_s=round(trip.duration_s, 1),
                max_speed_kph=round(trip.max_speed_kph, 1),
            ),
        )

    def _shutdown(self) -> None:
        logger.info("processor shutting down")
        self._stop_event.set()
        sweeper = self._sweeper
        if sweeper is not None and sweeper.is_alive():
            sweeper.join(timeout=self._sweep_interval_s + 1.0)
        flush_producer(self._producer)
        close_consumer(self._consumer)
        logger.info("processor stopped")


def main() -> int:
    """Entry point: load config, install signal handlers, run the processor."""
    setup_logging()
    try:
        config = load_config()
    except ConfigError as exc:
        logger.error("configuration error", extra=log_extra(error=str(exc)))
        return 1
    setup_logging(config.log_level)

    processor = Processor(config)

    def _handle_signal(signum: int, _frame: Any) -> None:
        logger.info("signal received", extra=log_extra(signal=signum))
        processor.request_stop()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    processor.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
