.PHONY: install ui-build rust-checks perf-check
.PHONY: postgres-start postgres-url postgres-stop postgres-clean postgres-tests
.PHONY: license-check lint-proto lint-sources fix-sources
.PHONY: docs-generate docs-check schema-generate schema-check

LOCAL_POSTGRES_IMAGE ?= postgres:17
LOCAL_POSTGRES_CONTAINER ?= coral-test-postgres
LOCAL_POSTGRES_PORT ?=

install: ui-build
	cargo install --path crates/coral-cli --locked

ui-build:
	npm ci --prefix apps/ui
	npm run build --prefix apps/ui
	test -s apps/ui/dist/index.html

rust-checks:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
	cargo nextest run --workspace --all-targets --all-features --locked --no-fail-fast
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked

perf-check:
	cargo build --locked -p coral-cli --release
	cargo run --locked -p xtask --release -- perf-check --coral-bin target/release/coral

# ----------------------------------------------------------------------------
# Local Postgres-backed tests
# ----------------------------------------------------------------------------
# Starts a Docker Postgres matching CI's major version and runs the ignored
# Postgres coverage against it. By default Docker chooses an available localhost
# port, and postgres-tests creates a fresh database inside the container for
# each run. Set LOCAL_POSTGRES_PORT=55432 if you need a stable host port.
#
#   make postgres-start   # start/wait for local Docker Postgres
#   make postgres-url     # print the local Postgres connection URL
#   make postgres-tests   # start Docker Postgres, then run Postgres tests
#   make postgres-stop    # stop the local Docker Postgres container
#   make postgres-clean   # remove the local Docker Postgres container

postgres-start:
	@set -eu; \
	if ! command -v docker >/dev/null 2>&1; then \
	  echo "docker is required to start local Postgres"; \
	  exit 1; \
	fi; \
	if docker container inspect "$(LOCAL_POSTGRES_CONTAINER)" >/dev/null 2>&1; then \
	  docker start "$(LOCAL_POSTGRES_CONTAINER)" >/dev/null; \
	else \
	  local_postgres_port="$(LOCAL_POSTGRES_PORT)"; \
	  if [ -n "$$local_postgres_port" ]; then \
	    port_arg="-p 127.0.0.1:$$local_postgres_port:5432"; \
	  else \
	    port_arg="-p 127.0.0.1::5432"; \
	  fi; \
	  docker run -d \
	    --name "$(LOCAL_POSTGRES_CONTAINER)" \
	    -e POSTGRES_PASSWORD=postgres \
	    $$port_arg \
	    "$(LOCAL_POSTGRES_IMAGE)" >/dev/null; \
	fi; \
	for _ in $$(seq 1 60); do \
	  if docker exec "$(LOCAL_POSTGRES_CONTAINER)" pg_isready -U postgres >/dev/null 2>&1; then \
	    host_port=$$(docker port "$(LOCAL_POSTGRES_CONTAINER)" 5432/tcp | sed -n 's/.*:\([0-9][0-9]*\)$$/\1/p' | head -n 1); \
	    if [ -z "$$host_port" ]; then \
	      echo "Could not determine local Postgres host port"; \
	      exit 1; \
	    fi; \
	    echo "Postgres is ready at postgres://postgres:postgres@127.0.0.1:$$host_port/postgres"; \
	    exit 0; \
	  fi; \
	  sleep 1; \
	done; \
	docker logs "$(LOCAL_POSTGRES_CONTAINER)" || true; \
	echo "Postgres did not become ready"; \
	exit 1

postgres-url:
	@set -eu; \
	host_port=$$(docker port "$(LOCAL_POSTGRES_CONTAINER)" 5432/tcp | sed -n 's/.*:\([0-9][0-9]*\)$$/\1/p' | head -n 1); \
	if [ -z "$$host_port" ]; then \
	  echo "Local Postgres is not exposing 5432/tcp. Run make postgres-start first."; \
	  exit 1; \
	fi; \
	echo "postgres://postgres:postgres@127.0.0.1:$$host_port/postgres"

postgres-stop:
	@docker stop "$(LOCAL_POSTGRES_CONTAINER)" >/dev/null 2>&1 || true

postgres-clean:
	@docker rm -f "$(LOCAL_POSTGRES_CONTAINER)" >/dev/null 2>&1 || true

postgres-tests: postgres-start
	@set -eu; \
	host_port=$$(docker port "$(LOCAL_POSTGRES_CONTAINER)" 5432/tcp | sed -n 's/.*:\([0-9][0-9]*\)$$/\1/p' | head -n 1); \
	if [ -z "$$host_port" ]; then \
	  echo "Local Postgres is not exposing 5432/tcp"; \
	  exit 1; \
	fi; \
	db_name="coral_test_$$(date +%s)_$$$$"; \
	docker exec "$(LOCAL_POSTGRES_CONTAINER)" createdb -U postgres "$$db_name"; \
	cleanup() { docker exec "$(LOCAL_POSTGRES_CONTAINER)" dropdb -U postgres --if-exists "$$db_name" >/dev/null 2>&1 || true; }; \
	trap cleanup EXIT INT TERM; \
	url="postgres://postgres:postgres@127.0.0.1:$$host_port/$$db_name"; \
	echo "Running Postgres tests against $$url"; \
	CORAL_TEST_POSTGRES_URL="$$url" cargo test --locked -p coral-app --lib \
	  state::db::repositories::workspaces::tests::workspace_repository_round_trips_against_postgres \
	  -- --ignored; \
	CORAL_TEST_POSTGRES_URL="$$url" cargo test --locked -p coral-app --lib \
	  state::db::migrations::migration_order_tests::postgres_identity_database_contracts \
	  -- --ignored; \
	CORAL_TEST_POSTGRES_URL="$$url" cargo test --locked -p coral-app \
	  --test postgres_database_tests \
	  -- --ignored

# ----------------------------------------------------------------------------
# Dependency license scan
# ----------------------------------------------------------------------------
# Fails if any workspace dependency uses a license outside the allow-list in
# deny.toml. Requires `cargo-deny`.
#
#   make license-check

license-check:
	cargo deny --version >/dev/null 2>&1 || cargo install --locked cargo-deny
	cargo deny check licenses

# ----------------------------------------------------------------------------
# Protobuf API linting
# ----------------------------------------------------------------------------
# Lints crates/coral-api/proto with Buf.
#
#   make lint-proto   # check protobuf style and API-shape rules

lint-proto:
	cd crates/coral-api && buf lint

# ----------------------------------------------------------------------------
# Source manifest linting
# ----------------------------------------------------------------------------
# Lints sources/ with ryl (Rust-native yamllint port).
#
#   make lint-sources   # check only — run before pushing changes
#   make fix-sources    # apply ryl's safe auto-fixes in place

lint-sources:
	ryl sources

fix-sources:
	ryl --fix sources

# ----------------------------------------------------------------------------
# Source docs generation
# ----------------------------------------------------------------------------
# Regenerates the source catalog pages and Mintlify navigation from
# sources/core/*/manifest.y{a,}ml and sources/community/*/manifest.y{a,}ml
# via the xtask binary. docs-check intentionally skips the community source
# catalog so PRs do not fail on aggregate community source catalog drift.
#
#   make docs-generate   # write/refresh the generated files in apps/docs/
#   make docs-check      # CI freshness check: non-zero exit if stale

docs-generate:
	cargo run --locked -p xtask -- generate-docs \
	  --sources-dir sources/core \
	  --index apps/docs/reference/bundled-sources.mdx \
	  --community-sources-dir sources/community \
	  --community-index apps/docs/reference/community-sources.mdx \
	  --docs-json apps/docs/docs.json

docs-check:
	cargo run --locked -p xtask -- generate-docs \
	  --sources-dir sources/core \
	  --index apps/docs/reference/bundled-sources.mdx \
	  --docs-json apps/docs/docs.json \
	  --skip-community-sources \
	  --check

# ----------------------------------------------------------------------------
# JSON schema generation
# ----------------------------------------------------------------------------
# Regenerates manifest schemas that are generated from Rust types.
#
#   make schema-generate   # write/refresh generated schemas
#   make schema-check      # CI freshness check: non-zero exit if stale

schema-generate:
	cargo run --locked -p xtask -- generate-schemas

schema-check:
	cargo run --locked -p xtask -- generate-schemas --check
