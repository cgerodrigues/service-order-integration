# Service Order Integration

Synchronizes service orders between enterprise systems through validation,
contract transformation and an external Field Service HTTP API.

## Current architecture

```text
service-order.created (Kafka, structured CloudEvents JSON)
  -> ServiceOrderCreatedListener -> ServiceOrderCreatedData (v1)
  -> explicit domain mapping -> Jakarta Bean Validation
  -> SynchronizeServiceOrder -> FieldServicePort
  -> HttpFieldServiceAdapter -> POST /service-orders
  -> SynchronizationEventPort -> service-order.synchronized (Kafka)
```

Packages under `io.github.cgerodrigues.serviceorderintegration`:

- `domain.serviceorder`: existing business records and validation constraints.
- `application.serviceorder`: use case, business event metadata and outbound ports;
  no Kafka, HTTP or Spring dependencies.
- `infrastructure.cloudevents`: strict CloudEvents 1.0 codec, independent v1
  payload records/enums and explicit domain mapping for schema evolution.
- `infrastructure.kafka`: listener and confirmed success-event publisher.
- `infrastructure.fieldservice`: HTTP adapter and dedicated external DTOs,
  including customer, address and requested-window contracts.
- `infrastructure.config`: dependency wiring and stop-on-failure policy.

Technologies: Java 21, Spring Boot 4.1.1, Maven Wrapper, Spring Kafka,
Jackson 3, Jakarta Bean Validation, Spring RestClient, Micrometer/OpenTelemetry
and OTLP. Dependencies follow the Spring Boot BOM. Tests use JUnit, AssertJ,
Mockito, Spring HTTP mocks and embedded-Kafka/real-HTTP integration tests.

Business input is Kafka only. WebMVC and the embedded HTTP server are retained
for operational Actuator endpoints, including `/actuator/health`. There are no
business REST controllers; outbound delivery uses the existing RestClient.

## Contracts and behavior

Inbound type: `com.cgerodrigues.serviceorder.created.v1`.
The Kafka `content-type` header must have base media type
`application/cloudevents+json`; valid parameters such as `charset=UTF-8` are
accepted. Missing, malformed and unsupported transport types are rejected before
JSON processing. This header is distinct from the envelope's `datacontenttype`,
which describes the business data and must be `application/json`.
Required envelope fields: `specversion` (`1.0`), `id`, `source` (URI reference),
`type`, `subject`, `time` (offset timestamp), `datacontenttype` (`application/json`),
`correlationid` and object-valued `data`. Only structured JSON is supported;
malformed JSON, duplicate attributes, trailing documents, unsupported types and
invalid domain data are rejected. Unknown extension attributes are ignored.

`subject` must equal `data.id`. Producers should use the service order ID as the
Kafka key; absent keys are accepted, mismatched keys are rejected. Event ID,
business entity ID and functional correlation ID remain distinct. Optional
requested windows retain their received UTC offset.

The assumed Field Service contract is `POST /service-orders` with
`serviceOrderId`, `name`, `type`, `description`, `customer`, `serviceAddress`,
`priority`, `requestedWindow` and `notes`. A successful 2xx JSON response must
contain a nonblank `externalOrderId`. The external enum strings currently match
the internal enum names. No authentication is assumed for this initial mock.

After HTTP success, `com.cgerodrigues.serviceorder.synchronized.v1` is published
with a fresh UUID, source `urn:service-order-integration`, the order ID as subject
and Kafka key, current UTC time, the original `correlationid` and inbound event
ID as `causationid`. Data contains `serviceOrderId`, `externalOrderId` and
`status: SYNCHRONIZED`. The Kafka record has
`content-type: application/cloudevents+json`.

The listener commits each record only after processing and broker confirmation
of the success event. On any failure the listener container stops; there is no
business retry loop, recovery that skips a failed record, or DLQ. An operator must resolve
the cause and restart the service. HTTP timeouts and Kafka send waits are bounded.

## Configuration and observability

| Environment variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_CONSUMER_GROUP` | `service-order-integration` |
| `SERVICE_ORDER_CREATED_TOPIC` | `service-order.created` |
| `SERVICE_ORDER_SYNCHRONIZED_TOPIC` | `service-order.synchronized` |
| `FIELD_SERVICE_BASE_URL` | `http://localhost:8081` |
| `OTLP_ENDPOINT` | `http://localhost:4318` |
| `TRACING_SAMPLING_PROBABILITY` | `1.0` |

`OTLP_ENDPOINT` is the collector's HTTP base URL, without a trailing slash;
traces use `/v1/traces` and metrics use `/v1/metrics`. Kafka listener/template
observations and the Boot-configured RestClient propagate W3C `traceparent` and
`tracestate` through transport headers. Business payloads contain no trace IDs.
Structured JSON console logs include trace/span IDs and an MDC `correlationId`
during validation and synchronization. MDC is restored when processing ends.
There are no manual spans. The local vendor-neutral collector prints received
telemetry; it is not a persistent observability backend.

## Tests

Requires JDK 21; no running Docker daemon is needed:

```sh
./mvnw verify
```

Tests cover event contracts and rejection, nested validation, application
orchestration and failure propagation, complete outbound mapping, HTTP response
validation, success-event construction, correlation/causation, MDC cleanup,
broker confirmation and the full Kafka -> HTTP -> Kafka path with W3C context.
The failure integration test waits for consumer termination after HTTP failure,
then checks that no success event exists and the failed offset was not committed.
Only transport integration tests start the application context. Other tests
mirror the application/infrastructure packages, with shared test-only support.

## Local development

Requires Docker Compose and a running Docker engine:

```sh
docker compose config --quiet
docker compose up -d
./mvnw spring-boot:run
```

Compose starts a single-node Kafka broker, a one-shot topic initializer, WireMock
on port 8081 and an OTLP collector on port 4318. Topic initialization uses the
same topic environment variables as the application. External ports bind to
loopback. Infrastructure data is ephemeral; this is a development setup.

In another terminal, use jq to publish the fixture with its business key and
required structured CloudEvent transport header:

```sh
jq -r '"content-type:application/cloudevents+json\t" + .data.id + "\t" + tojson' \
  src/test/resources/events/service-order-created.json |
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:29092 --topic service-order.created \
    --property parse.headers=true --property parse.key=true

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic service-order.synchronized \
  --from-beginning --max-messages 1 --property print.key=true

docker compose logs otel-collector
docker compose down
```

Adjust the CLI topic arguments if overriding topic names. The fixture represents
a historical event; the WireMock response uses `fs-<serviceOrderId>`. The console
producer does not send an upstream trace context, so the consumer starts a trace.

## Current limitations

Persistence, business retry, DLQ, business-level idempotency/deduplication,
outbox and distributed transactions are intentionally deferred. HTTP delivery and Kafka publication are not atomic:
a crash or publication failure after HTTP success can create duplicates after
restart, and a publish timeout can have an ambiguous broker outcome. This is
not exactly-once processing. Kafka producer **transport idempotence** and transport
retries use the safe Kafka client defaults (Boot resolves Kafka clients 4.2.1).
Retries remain bounded by the configured delivery timeout. This protects producer
retransmissions; it does not answer whether a service order was already delivered
to Field Service. Business idempotency remains unimplemented. Ordering depends
on upstream producers consistently keying records by service order ID.

Binary CloudEvents, API authentication, production Kafka security, deployment
hardening and an operational recovery workflow are deferred. The mock API
contract must be aligned with the real Field Service provider before deployment.
