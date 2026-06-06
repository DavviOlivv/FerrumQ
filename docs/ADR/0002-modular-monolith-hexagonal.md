# ADR 0002: Modular Monolith and Hexagonal Architecture

## Status

Accepted.

## Decision

FerrumQ starts as a modular monolith and uses hexagonal architecture with explicit ports and adapters.

## Rationale

A messaging engine is already distributed at its boundaries: producers, consumers, storage, network protocols, and operators all interact with it. Starting with microservices would multiply coordination, deployment, and failure-mode complexity before the domain is proven.

Hexagonal architecture keeps domain logic isolated from HTTP, gRPC, filesystem, CLI, TUI, and runtime adapters. That lets the project test core behavior deterministically and swap adapters as the broker matures.

## Consequences

The initial codebase is one workspace with clear crate boundaries. Future distribution is possible, but it must evolve from stable module contracts rather than from premature service extraction.
