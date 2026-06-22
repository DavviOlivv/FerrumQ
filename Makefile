SHELL := /usr/bin/bash

POSTGRES_CONTAINER ?= ferrumq-postgres
POSTGRES_PORT ?= 5432
POSTGRES_PASSWORD ?= ferrumq

.PHONY: \
	rust-fmt rust-fmt-check rust-check rust-clippy rust-test rust-nextest rust-deny \
	ts-format ts-format-check ts-lint ts-typecheck ts-test ts-build \
	fmt lint typecheck test build smoke hygiene \
	postgres-up postgres-wait postgres-test postgres-down ci

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
	node packages/chat/dist/cli.js --help
	cargo run -p msg-runtime --bin brokerd -- --version

hygiene:
	git diff --check

postgres-up:
	@if docker container inspect "$(POSTGRES_CONTAINER)" >/dev/null 2>&1; then \
		docker start "$(POSTGRES_CONTAINER)" >/dev/null; \
		echo "Started PostgreSQL container $(POSTGRES_CONTAINER)"; \
	else \
		docker run --name "$(POSTGRES_CONTAINER)" \
			-e POSTGRES_PASSWORD="$(POSTGRES_PASSWORD)" \
			-p "$(POSTGRES_PORT):5432" \
			-d postgres:16-alpine >/dev/null; \
		echo "Created PostgreSQL container $(POSTGRES_CONTAINER)"; \
	fi

postgres-wait:
	@attempt=1; \
	while [ "$$attempt" -le 30 ]; do \
		if docker exec "$(POSTGRES_CONTAINER)" \
			pg_isready -U postgres -d postgres >/dev/null 2>&1; then \
			echo "PostgreSQL container $(POSTGRES_CONTAINER) is ready"; \
			exit 0; \
		fi; \
		sleep 1; \
		attempt=$$((attempt + 1)); \
	done; \
	echo "PostgreSQL container $(POSTGRES_CONTAINER) did not become ready" >&2; \
	exit 1

postgres-test: postgres-wait
	@FERRUMQ_POSTGRES_TEST_URL="postgres://postgres:$(POSTGRES_PASSWORD)@127.0.0.1:$(POSTGRES_PORT)/postgres" \
		cargo test -p msg-postgres
	@FERRUMQ_POSTGRES_TEST_URL="postgres://postgres:$(POSTGRES_PASSWORD)@127.0.0.1:$(POSTGRES_PORT)/postgres" \
		cargo nextest run -p msg-postgres --no-fail-fast

postgres-down:
	@if docker container inspect "$(POSTGRES_CONTAINER)" >/dev/null 2>&1; then \
		docker rm -f "$(POSTGRES_CONTAINER)" >/dev/null; \
		echo "Removed PostgreSQL container $(POSTGRES_CONTAINER)"; \
	else \
		echo "PostgreSQL container $(POSTGRES_CONTAINER) does not exist"; \
	fi

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
	node packages/chat/dist/cli.js --help
	cargo run -p msg-runtime --bin brokerd -- --version
	git diff --check
