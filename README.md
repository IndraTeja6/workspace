# Nova — Fleet Tracking System

Nova is a real-time fleet tracking system that ingests vehicle telemetry from
Kafka, validates and normalizes it, and (in later stages) derives vehicle
state, trips, geofence events, and maintenance alerts. It is built as a set of
small, stateless, container-friendly services that run on Kubernetes and are
configured entirely through environment variables.

## Overview

Vehicles emit telemetry (position, speed, fuel, engine health, driver
behavior) to a raw Kafka topic. The Nova stream processor consumes that topic,
validates each event, publishes a normalized event to a processed topic, and
routes malformed payloads to a dead-letter topic. Downstream stages build on
this normalized stream to produce trips, alerts, and a live dashboard.

## Architecture

Nova follows a **separate-service design**: the stream processor and the
dashboard are independent services that communicate only through Kafka. This
keeps the ingestion path lightweight and horizontally scalable, and lets the
dashboard be deployed, scaled, and restarted without affecting ingestion.

```
                 +-------------------+        +----------------------+
  vehicles --->  |  raw topic        |  --->  |  Nova stream         |
  (telemetry)    |  fleet.telemetry  |        |  processor           |
                 |  .raw             |        |  (nova.processor)    |
                 +-------------------+        +----------+-----------+
                                                         |
                        +--------------------------------+------------------+
                        |                                                   |
              +---------v----------+                            +-----------v---------+
              |  processed topic   |                            |  dead-letter topic  |
              |  fleet.telemetry   |                            |  fleet.telemetry    |
              |  .processed         |                            |  .dlq               |
              +---------+----------+                            +---------------------+
                        |
              +---------v----------+        +----------------------+
              |  alerts topic      |  --->  |  Dashboard service   |
              |  fleet.alerts      |        |  (FastAPI, stage 4)  |
              +--------------------+        +----------------------+
```

- **Stream processor** (`nova/processor.py`) — consumes the raw topic,
  validates and normalizes events, publishes to the processed topic, and
  commits offsets. Invalid payloads go to the dead-letter topic.
- **Dashboard** (stage 4) — a separate FastAPI service that reads the
  processed and alerts topics to present live fleet state.

### Module layout

| Path | Purpose |
| --- | --- |
| `nova/config.py` | Typed configuration loading (`AppConfig`, `load_config`) with env-var overrides. |
| `nova/models.py` | Domain models: `TelemetryEvent`, `VehicleState`, `EngineHealth`, `DriverBehavior`. |
| `nova/validation.py` | `validate_and_normalize` and JSON serialization helpers. |
| `nova/kafka_io.py` | Kafka consumer/producer wrappers, retry/backoff, DLQ, deferred-commit loop. |
| `nova/processor.py` | The streaming pipeline and its `main()` entry point. |
| `nova/logging_setup.py` | Structured logging helpers. |
| `scripts/generate_sample_telemetry.py` | Simulated fleet telemetry generator. |
| `config.yaml` | Base configuration (topics, group id, security placeholders). |

## Current status

**Stage 1 — ingestion core (complete).** The processor consumes raw telemetry,
validates and normalizes it, publishes normalized events to the processed
topic, routes invalid payloads to the dead-letter topic, and commits offsets
with at-least-once semantics. A sample telemetry generator is included for
local testing. State/trips, alerts, and the dashboard are not yet implemented.

## Setup

Requires Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy the example environment file and adjust as needed:

```bash
cp .env.example .env
```

A running Kafka broker is required. The default configuration targets
`localhost:9092`; override it with `NOVA_KAFKA_BOOTSTRAP_SERVERS`.

## Configuration

Configuration is read from a YAML file whose path comes from the `NOVA_CONFIG`
environment variable (default `config.yaml`), then overlaid with environment
variables. Loading is fail-fast: a missing file, malformed YAML, or invalid
value raises a clear error.

`config.yaml` structure:

```yaml
kafka:
  bootstrap_servers: localhost:9092
  topics:
    raw: fleet.telemetry.raw
    processed: fleet.telemetry.processed
    alerts: fleet.alerts
    dlq: fleet.telemetry.dlq
  group_id: nova-processor
  security:
    protocol: SASL_PLAINTEXT
    mechanism: SCRAM-SHA-512
    username: null
    password: null
    ca_file: null

thresholds: {}   # populated in later stages
geofences: []    # populated in later stages
```

Environment overrides (see `.env.example`):

| Variable | Overrides |
| --- | --- |
| `NOVA_CONFIG` | Path to the YAML config file. |
| `NOVA_KAFKA_BOOTSTRAP_SERVERS` | `kafka.bootstrap_servers` |
| `NOVA_KAFKA_GROUP_ID` | `kafka.group_id` |
| `NOVA_LOG_LEVEL` | `log_level` |
| `NOVA_KAFKA_SECURITY_PROTOCOL` | SASL protocol (optional) |
| `NOVA_KAFKA_SASL_MECHANISM` | SASL mechanism (optional) |
| `NOVA_KAFKA_SASL_USERNAME` | SASL username (optional) |
| `NOVA_KAFKA_SASL_PASSWORD` | SASL password (optional) |

Secrets are never committed; inject them at runtime via the environment.

## Running

Start the stream processor (consumes the raw topic, publishes to processed):

```bash
python -m nova.processor
```

In a separate terminal, generate sample telemetry:

```bash
python scripts/generate_sample_telemetry.py --vehicles 10 --interval 1.0
```

The generator accepts `--vehicles`, `--interval`, `--count`, `--seed`,
`--base-lat`, and `--base-lon`. Both processes shut down gracefully on
`SIGTERM`/`SIGINT`, flushing the producer and closing the consumer.

## Roadmap

- **Stage 2 — State & trips:** maintain per-vehicle state and derive trips
  (start/end, distance, duration) from the processed stream.
- **Stage 3 — Geofence & maintenance alerts:** evaluate geofence rules and
  engine-health thresholds, publishing alerts to the alerts topic.
- **Stage 4 — FastAPI dashboard:** a separate service exposing live fleet
  state and alerts over HTTP.
- **Stage 5 — Docker & tests:** containerize the services, add Docker Compose
  for local orchestration, and build out the automated test suite.
