# ADR 0001: Rust Core, TypeScript CLI

## Status

Accepted.

## Decision

FerrumQ uses Rust for the core messaging engine and TypeScript for terminal tooling, including the CLI and TUI.

## Rationale

Rust gives the broker memory safety, explicit error handling, predictable performance, and strong concurrency tools. These properties fit the storage, routing, delivery, and recovery responsibilities that the core will own.

TypeScript gives a strong developer experience for terminal tooling, package distribution, tests, and integration with the JavaScript ecosystem. This mirrors a practical pattern for developer tools: a systems core with a polished human-facing interface.

## Consequences

Rust is the source of truth for broker behavior. TypeScript can validate input and present state, but it must call Rust-owned process or API boundaries rather than reimplement delivery, storage, or broker semantics.
