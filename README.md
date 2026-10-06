# Orbit Events Kafka

`orbit-events-kafka` is an optional Apache Kafka transport for Orbit Core's typed `EventTransport`
contract. It is installed independently; neither Core nor the in-process Core event bus gains an
`aiokafka` dependency.

```bash
python -m pip install orbit-core orbit-events orbit-events-kafka
```

## Configure and publish

Keep broker credentials out of source control. The config masks SASL passwords in representations;
use TLS/SASL settings appropriate to the deployment and restrict Kafka ACLs to the required topics.

```python
from orbit import Application, ApplicationConfig
from orbit_events_kafka import KafkaConfig, KafkaEventTransport, KafkaTransportPlugin

application = Application(ApplicationConfig(name="orders"))
transport = KafkaEventTransport(
    KafkaConfig(
        bootstrap_servers=("localhost:9092",),
        topic_prefix="orbit.events",
        group_id="orders-service",
    )
)
application.plugins.register(KafkaTransportPlugin(transport))
await application.startup()

await transport.publish(order_created_event)
```

The plugin registers the transport under `KAFKA_EVENT_TRANSPORT_KEY` and closes it during Core
shutdown. Producer connection is lazy and occurs on the first publish. `subscribe()` starts a
consumer for one event name; a consumer group distributes that event's records among its members.
Register at most one handler for a given event name on a transport instance. Concurrent
`unsubscribe()` and transport shutdown are serialized so shutdown waits for a consumer stop already
in progress.

## Delivery contract and limits

Events are encoded as their Core JSON envelope and mapped to `<topic_prefix>.<event_name>`. The
maximum encoded message size is configurable and bounded. Consumers disable automatic offset commits
and commit only after the handler completes, providing at-least-once processing. A failed handler is
retried a configured finite number of times with bounded backoff. Once retries are exhausted, that
consumer stops without committing the failed offset; restarting/rebalancing the group can deliver it
again. Inspect `failed_subscriptions` and application logs for stopped consumers.

This is not exactly-once processing: handlers must be idempotent when duplicate delivery matters.
The package does not provide a dead-letter topic, schema registry, outbox, cross-service transaction,
automatic topic creation, or broker provisioning. Topic ACLs, retention, replication, TLS, monitoring,
and deployment reliability are operator responsibilities. No live Kafka broker test is claimed by
the fake-client unit suite.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development and status

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

This package is pre-alpha, its API is not stable, and it supports Python 3.11 through 3.14.

The producer/consumer lifecycle and manual-offset-commit behavior follow the
[aiokafka producer](https://aiokafka.readthedocs.io/en/latest/producer.html) and
[consumer](https://aiokafka.readthedocs.io/en/latest/consumer.html) APIs.

Licensed under Apache-2.0.

