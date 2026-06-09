# ADR 0011: Data Plane gRPC API

## Status

Accepted.

## Context

Milestone 5 established a local HTTP control plane for topic administration and broker inspection, but publish, consume, ACK, and NACK remained available only through Rust broker APIs. FerrumQ needs a network data-plane boundary that keeps broker semantics in Rust, exposes an explicit versioned protocol, and can later support generated clients without turning TypeScript packages into a second broker implementation.

The existing `DurableBroker` is synchronous and local-filesystem backed. It already owns at-least-once publish, consume, ACK, NACK, retry, DLQ, and reopen behavior. The first data-plane adapter should therefore be a thin boundary around that API, not a new broker execution model.

## Decision

Define `ferrumq.dataplane.v1.FerrumQDataPlane` in protobuf under `crates/msg-protocol/proto/ferrumq/dataplane/v1/dataplane.proto` and generate Rust tonic/prost types from `msg-protocol`.

Add `msg-data-plane` as the gRPC adapter crate. It stores `DurableBroker` behind `Arc<Mutex<_>>`, maps protobuf DTOs explicitly into core domain values and broker commands, calls public broker APIs only, and maps errors to stable sanitized tonic statuses.

Wire runtime support through:

```sh
brokerd serve-grpc --data-dir ./.ferrumq --listen 127.0.0.1:9090
```

Keep `brokerd --version` and the existing `brokerd serve --data-dir ... --listen ...` HTTP control-plane behavior unchanged.

The Milestone 6 service is unary-only:

- `Publish(PublishRequest) -> PublishResponse`.
- `Consume(ConsumeRequest) -> ConsumeResponse`.
- `Ack(AckRequest) -> AckResponse`.
- `Nack(NackRequest) -> NackResponse`.

Consume requests carry `lease_ms` and `now_unix_ms`. The broker command layer accepts an explicit per-request lease while preserving broker-config leases for older callers.

## Error Mapping

- Domain validation and malformed request values: `INVALID_ARGUMENT`.
- Unknown topic: `NOT_FOUND`.
- Unknown, duplicate, or stale delivery: `NOT_FOUND`.
- Wrong consumer or invalid delivery ownership: `FAILED_PRECONDITION`.
- Duplicate topic, if surfaced through broker APIs: `ALREADY_EXISTS`.
- Poisoned or unavailable broker mutex: `UNAVAILABLE`.
- Storage, corruption, serialization, and unexpected broker failures: `INTERNAL`.

Messages must be sanitized and must not include filesystem paths, Rust type names, debug dumps, or backtraces.

## Consequences

The data plane now has a versioned protobuf contract and an executable local gRPC adapter backed by durable broker state. This enables in-process tonic tests and future generated clients without duplicating broker semantics.

The adapter remains deliberately small. It does not implement streaming consume, TypeScript generated clients, auth/RBAC, TLS, rate limiting, observability export, dashboards, clustering, replication, exactly-once semantics, MaaS/multi-tenancy, or production daemon hardening.
