# Release Checklist

Use this checklist before presenting FerrumQ as a local `0.1.0` release
candidate.

## Scope Gate

- No new broker behavior is being introduced as part of release hardening.
- The release is described as local-first and portfolio-ready, not production
  ready.
- Claims are limited to local durable at-least-once delivery.
- Consumers are documented as responsible for idempotency.
- Exactly-once delivery, clustering, replication, consensus, auth/RBAC, TLS,
  rate limiting, MaaS/multi-tenancy, hosted telemetry, dashboards, and
  production daemon hardening are not claimed.
- `idempotency_key` is described as metadata-only with no producer
  deduplication guarantee.
- Message payloads are not claimed to be logged or exposed as metric labels.

## Version Gate

- Root `package.json` version is `0.1.0`.
- `packages/*/package.json` versions are `0.1.0`.
- Rust workspace package version in `Cargo.toml` is `0.1.0`.
- `packages/cli/src/config.ts` `cliVersion` is `0.1.0`.
- `packages/tui/src/config.ts` `tuiVersion` is `0.1.0`.
- `brokerd --version` reports the Rust package version.
- Built CLI and TUI dist output has been regenerated after any version change.

## Docs Gate

- `README.md` is an entry point, not only a milestone log.
- [LOCAL_DEMO.md](LOCAL_DEMO.md) matches the CLI, TUI, HTTP, and gRPC command
  surface.
- [API.md](API.md), [CLI.md](CLI.md), [TUI.md](TUI.md), and
  [OBSERVABILITY.md](OBSERVABILITY.md) do not overclaim production readiness.
- Release-facing docs mention `/metrics` as process-local operational data.
- Release-facing quickstarts use `brokerd serve-all` as the recommended local
  coherent demo/dev runtime.
- Release-facing docs document that `brokerd serve` is HTTP-only and
  `brokerd serve-grpc` is gRPC-only. Split local processes load state at
  startup; shared `--data-dir` persists state across restarts but does not
  provide live shared in-memory state, live reload, live HTTP/TUI inspection of
  gRPC-process changes, or shared HTTP `/metrics` counters.
- Release-facing docs state that `serve-all` solves live state and metrics
  coherence only inside one process, not cross-process reload or aggregation.
- Release-facing docs mention that payloads are not logged by default and are
  not metric labels.
- Any remaining milestone history is clearly historical or deep reference
  material, not the primary release status.

## Validation Gate

Run the full local release suite:

```sh
cargo fmt --all --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo nextest run --workspace
cargo deny check
pnpm install --frozen-lockfile
pnpm format:check
pnpm lint
pnpm typecheck
pnpm test
pnpm build
node packages/cli/dist/cli.js --version
node packages/cli/dist/cli.js --help
node packages/cli/dist/cli.js topic --help
node packages/cli/dist/cli.js publish --help
node packages/tui/dist/cli.js --version
node packages/tui/dist/cli.js --help
cargo run -p msg-runtime --bin brokerd -- --version
make ci
git diff --check
```

`cargo deny check` can emit a duplicate `hashbrown` warning. It is a known
non-fatal warning only if `cargo deny check` exits successfully.

## Final Review

- No package publishing or publishing automation was added.
- CI uses the same local harness path as developer validation.
- The worktree contains no unrelated generated or dependency churn.
- `git diff --check` passes.
