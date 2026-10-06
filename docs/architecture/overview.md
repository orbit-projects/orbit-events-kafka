# Orbit Events Kafka: architecture and boundaries

## Responsibility

`orbit-events-kafka` is an optional Apache Kafka transport for Orbit Core's typed `EventTransport`
contract. It is installed independently; neither Core nor the in-process Core event bus gains an
`aiokafka` dependency.

```bash
python -m pip install orbit-core orbit-events-kafka
```

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `aiokafka>=0.12,<1`
- `orbit-core>=0.1.0a1,<0.2`
- Optional `dev` group: `pytest>=8,<10`, `pytest-asyncio>=0.24,<2`, `ruff>=0.8,<1`, `mypy>=1.13,<2`.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `.tox/wheel-install-final/orbit_events_kafka/__init__.py`
- `.tox/wheel-install-final/orbit_events_kafka/config.py`
- `.tox/wheel-install-final/orbit_events_kafka/plugin.py`
- `.tox/wheel-install-final/orbit_events_kafka/transport.py`
- `.tox/wheel-install/orbit_events_kafka/__init__.py`
- `.tox/wheel-install/orbit_events_kafka/config.py`
- `.tox/wheel-install/orbit_events_kafka/plugin.py`
- `.tox/wheel-install/orbit_events_kafka/transport.py`
- `src/orbit_events_kafka/__init__.py`
- `src/orbit_events_kafka/config.py`
- `src/orbit_events_kafka/plugin.py`
- `src/orbit_events_kafka/transport.py`

## Public contract and scope

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

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
