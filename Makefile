SHELL := /usr/bin/bash

.PHONY: \
	rust-fmt rust-fmt-check rust-check rust-clippy rust-test rust-nextest rust-deny \
	ts-format ts-format-check ts-lint ts-typecheck ts-test ts-build \
	fmt lint typecheck test build smoke hygiene ci

rust-fmt:
	cargo fmt --all

rust-fmt-check:
	cargo fmt --all --check

rust-check:
	cargo check --workspace

rust-clippy:
	cargo clippy --workspace --all-targets -- -D warnings

rust-test:
	cargo test --workspace

rust-nextest:
	cargo nextest run --workspace

rust-deny:
	cargo deny check

ts-format:
	pnpm format

ts-format-check:
	pnpm format:check

ts-lint:
	pnpm lint

ts-typecheck:
	pnpm typecheck

ts-test:
	pnpm test

ts-build:
	pnpm build

fmt: rust-fmt ts-format

lint: rust-clippy ts-lint

typecheck: rust-check ts-typecheck

test: rust-test rust-nextest ts-test

build:
	cargo build --workspace
	pnpm build

smoke:
	node packages/cli/dist/cli.js --version
	node packages/cli/dist/cli.js --help
	node packages/cli/dist/cli.js topic --help
	node packages/cli/dist/cli.js publish --help
	node packages/tui/dist/cli.js --version
	node packages/tui/dist/cli.js --help
	cargo run -p msg-runtime --bin brokerd -- --version

hygiene:
	git diff --check

ci:
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
	git diff --check
