"""Generate realistic Nova telemetry and publish it to the raw Kafka topic.

Simulates a fleet of vehicles emitting telemetry on a fixed interval. Each
vehicle moves around a base coordinate, burns fuel, accumulates odometer
distance, and occasionally reports diagnostic trouble codes (DTCs) or
aggressive-driving flags.

Usage::

    python scripts/generate_sample_telemetry.py --vehicles 10 --interval 1.0

The script reads the same configuration as the processor (``NOVA_CONFIG``
or ``config.yaml``) and publishes to ``kafka.topics.raw``.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import signal
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Sequence

# Allow running as a script from the repository root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nova.config import load_config
from nova.kafka_io import KafkaIOError, flush_producer, make_producer, produce_json
from nova.logging_setup import get_logger, log_extra, setup_logging

logger = get_logger(__name__)

DEFAULT_VEHICLES = 10
DEFAULT_INTERVAL = 1.0
DEFAULT_BASE_LAT = 12.9716
DEFAULT_BASE_LON = 77.5946
DEFAULT_SEED = 42

_KM_PER_DEG_LAT = 111.32
_DRIFT_LIMIT_KM = 2.0
_DTC_POOL = ("P0300", "P0420", "P0171", "P0128", "P0455", "C0035", "B1318")


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * radius_km * math.asin(math.sqrt(a))


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    x = math.sin(dlambda) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(
        dlambda
    )
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


@dataclass
class VehicleSim:
    """Mutable simulation state for a single vehicle."""

    vehicle_id: str
    lat: float
    lon: float
    heading_deg: float
    speed_kph: float
    fuel_pct: float
    odometer_km: float
    rpm: int
    coolant_temp_c: float
    oil_pressure_kpa: float
    dtc_codes: List[str] = field(default_factory=list)

    def step(
        self,
        rng: random.Random,
        interval: float,
        base_lat: float,
        base_lon: float,
    ) -> None:
        """Advance the simulation by one interval."""
        self.speed_kph = _clamp(self.speed_kph + rng.uniform(-8.0, 8.0), 0.0, 120.0)
        self.heading_deg = (self.heading_deg + rng.uniform(-25.0, 25.0)) % 360.0

        if _haversine_km(self.lat, self.lon, base_lat, base_lon) > _DRIFT_LIMIT_KM:
            self.heading_deg = _bearing_deg(self.lat, self.lon, base_lat, base_lon)

        distance_km = self.speed_kph * (interval / 3600.0)
        heading_rad = math.radians(self.heading_deg)
        self.lat += (distance_km / _KM_PER_DEG_LAT) * math.cos(heading_rad)
        cos_lat = max(math.cos(math.radians(self.lat)), 1e-6)
        self.lon += (distance_km / (_KM_PER_DEG_LAT * cos_lat)) * math.sin(heading_rad)

        self.fuel_pct = _clamp(
            self.fuel_pct - rng.uniform(0.02, 0.12) * (1.0 + self.speed_kph / 100.0),
            0.0,
            100.0,
        )
        self.odometer_km += distance_km

        self.rpm = int(
            _clamp(800 + self.speed_kph * 25 + rng.uniform(-150, 150), 600, 6500)
        )
        self.coolant_temp_c = _clamp(
            85 + self.speed_kph * 0.15 + rng.uniform(-3, 3), 60, 120
        )
        self.oil_pressure_kpa = _clamp(
            300 + self.rpm * 0.05 + rng.uniform(-20, 20), 100, 600
        )

        if rng.random() < 0.02 and len(self.dtc_codes) < 3:
            code = rng.choice(_DTC_POOL)
            if code not in self.dtc_codes:
                self.dtc_codes.append(code)
        elif self.dtc_codes and rng.random() < 0.10:
            self.dtc_codes.pop(0)

    def to_payload(self, rng: random.Random) -> bytes:
        """Serialize the current state as a raw telemetry JSON payload."""
        event = {
            "vehicle_id": self.vehicle_id,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed_kph": round(self.speed_kph, 2),
            "fuel_pct": round(self.fuel_pct, 2),
            "engine_health": {
                "rpm": self.rpm,
                "coolant_temp_c": round(self.coolant_temp_c, 2),
                "oil_pressure_kpa": round(self.oil_pressure_kpa, 2),
                "dtc_codes": list(self.dtc_codes),
            },
            "driver_behavior": {
                "harsh_brake": rng.random() < 0.05,
                "harsh_accel": rng.random() < 0.05,
                "sharp_turn": rng.random() < 0.04,
                "speeding": self.speed_kph > 100.0,
            },
            "odometer_km": round(self.odometer_km, 3),
        }
        return json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )


def _make_vehicle(
    index: int, rng: random.Random, base_lat: float, base_lon: float
) -> VehicleSim:
    return VehicleSim(
        vehicle_id=f"VH-{1001 + index}",
        lat=base_lat + rng.uniform(-0.01, 0.01),
        lon=base_lon + rng.uniform(-0.01, 0.01),
        heading_deg=rng.uniform(0.0, 360.0),
        speed_kph=rng.uniform(0.0, 60.0),
        fuel_pct=rng.uniform(40.0, 100.0),
        odometer_km=rng.uniform(1000.0, 50000.0),
        rpm=800,
        coolant_temp_c=85.0,
        oil_pressure_kpa=300.0,
    )


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate sample Nova fleet telemetry to the raw Kafka topic."
    )
    parser.add_argument(
        "--vehicles",
        type=int,
        default=DEFAULT_VEHICLES,
        help="number of simulated vehicles (default: %(default)s)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        help="seconds between emissions (default: %(default)s)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="total messages to emit, 0 = run until stopped (default: %(default)s)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="random seed for reproducibility (default: %(default)s)",
    )
    parser.add_argument(
        "--base-lat",
        type=float,
        default=DEFAULT_BASE_LAT,
        help="base latitude (default: %(default)s)",
    )
    parser.add_argument(
        "--base-lon",
        type=float,
        default=DEFAULT_BASE_LON,
        help="base longitude (default: %(default)s)",
    )
    return parser.parse_args(argv)


def _validate_args(args: argparse.Namespace) -> None:
    if args.vehicles < 1:
        raise SystemExit("--vehicles must be >= 1")
    if args.interval <= 0:
        raise SystemExit("--interval must be > 0")
    if args.count < 0:
        raise SystemExit("--count must be >= 0")
    if not -90.0 <= args.base_lat <= 90.0:
        raise SystemExit("--base-lat must be within [-90, 90]")
    if not -180.0 <= args.base_lon <= 180.0:
        raise SystemExit("--base-lon must be within [-180, 180]")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    _validate_args(args)

    config = load_config()
    setup_logging(config.log_level)

    producer = make_producer(config)
    stop_event = threading.Event()

    def _handle_signal(signum: int, _frame: object) -> None:
        logger.info("shutdown requested", extra=log_extra(signal=signum))
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    rng = random.Random(args.seed)
    vehicles = [
        _make_vehicle(i, rng, args.base_lat, args.base_lon)
        for i in range(args.vehicles)
    ]
    topic = config.kafka.topics.raw

    logger.info(
        "telemetry generator started",
        extra=log_extra(
            topic=topic,
            vehicles=len(vehicles),
            interval_s=args.interval,
            count=args.count,
        ),
    )

    emitted = 0
    try:
        while not stop_event.is_set():
            for vehicle in vehicles:
                if stop_event.is_set() or (args.count and emitted >= args.count):
                    break
                vehicle.step(rng, args.interval, args.base_lat, args.base_lon)
                try:
                    produce_json(
                        producer,
                        topic,
                        key=vehicle.vehicle_id,
                        payload=vehicle.to_payload(rng),
                    )
                except KafkaIOError as exc:
                    logger.error(
                        "failed to publish telemetry",
                        extra=log_extra(vehicle_id=vehicle.vehicle_id, error=str(exc)),
                    )
                    continue
                emitted += 1

            if args.count and emitted >= args.count:
                break
            if stop_event.wait(args.interval):
                break
    finally:
        flush_producer(producer)
        logger.info("telemetry generator stopped", extra=log_extra(emitted=emitted))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
