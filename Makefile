SHELL := /usr/bin/bash

.PHONY: fmt lint typecheck test test-rust test-ts test-integration test-e2e build audit ci

fmt:
	cargo fmt --all
	pnpm format

lint:
	cargo clippy --workspace --all-targets -- -D warnings
	pnpm lint

typecheck:
	cargo check --workspace
	pnpm typecheck

test: test-rust test-ts test-integration test-e2e

test-rust:
	if command -v cargo-nextest >/dev/null 2>&1; then \
		cargo nextest run --workspace; \
	else \
		echo "cargo-nextest not installed; falling back to cargo test."; \
		cargo test --workspace; \
	fi

test-ts:
	pnpm test

test-integration:
	echo "Milestone 0: no dedicated integration tests yet."

test-e2e:
	echo "Milestone 0: no end-to-end tests yet."

build:
	cargo build --workspace
	pnpm build

audit:
	if command -v cargo-deny >/dev/null 2>&1; then \
		cargo deny check; \
	else \
		echo "Milestone 0: cargo-deny is not installed; audit is a documented non-breaking follow-up."; \
	fi

ci:
	cargo fmt --all --check
	cargo check --workspace
	cargo clippy --workspace --all-targets -- -D warnings
	if command -v cargo-nextest >/dev/null 2>&1; then \
		cargo nextest run --workspace; \
	else \
		echo "cargo-nextest not installed; falling back to cargo test."; \
		cargo test --workspace; \
	fi
	cargo build --workspace
	pnpm install --frozen-lockfile
	pnpm format:check
	pnpm lint
	pnpm typecheck
	pnpm test
	pnpm build
	pnpm --filter @ferrumq/cli build
	node packages/cli/dist/cli.js --version
	node packages/cli/dist/cli.js --help
	$(MAKE) audit
