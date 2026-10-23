.PHONY: install docker-build reef-docker-build docker-test rust-checks
.PHONY: postgres-start postgres-url postgres-stop postgres-clean postgres-tests load-test
.PHONY: license-check lint-migrations lint-proto reef-image-checks

# Images build and test with Docker or Podman.
CONTAINER_ENGINE ?= $(shell command -v docker >/dev/null 2>&1 && echo docker || echo podman)
DOCKER_IMAGE ?= coral:local
CORAL_WEB_DOCKER_IMAGE ?= coral-web:local
DOCKER_NO_CACHE ?= 0

RUST_WORKSPACE_TESTS = cargo nextest run --workspace --all-targets --locked --no-fail-fast

# Podman drops HEALTHCHECK unless it writes the Docker image format. Docker
# needs --load so an image built by a container builder reaches the local
# store; Podman does not accept that flag.
define image_build
set -eu; \
case "$(DOCKER_NO_CACHE)" in \
  0|false|'') ;; \
  1|true) set -- --no-cache ;; \
  *) echo "DOCKER_NO_CACHE must be 0, 1, false, or true" >&2; exit 1 ;; \
esac; \
if $(CONTAINER_ENGINE) --version | grep -qi podman; then \
  set -- "$$@" --format docker; \
else \
  set -- "$$@" --load; \
fi; \
$(CONTAINER_ENGINE) build "$$@"
endef

install:
	cargo install --path crates/coral --locked

# ----------------------------------------------------------------------------
# Images
# ----------------------------------------------------------------------------
# docker-build compiles this checkout into the runtime image. docker-test
# builds both images and starts them together on fresh state.
#
#   make docker-test
#   DOCKER_IMAGE=coral:test make docker-build
#   DOCKER_NO_CACHE=1 make docker-build
#   CONTAINER_ENGINE=podman make docker-test

docker-build:
	@git_sha=$$(git rev-parse --short HEAD); \
	$(image_build) --build-arg "CORAL_GIT_SHA=$$git_sha" --file docker/Dockerfile --tag "$(DOCKER_IMAGE)" .

# Source-only image contracts run without a container engine and before a build.
reef-image-checks:
	node --test docker/reef-image.test.mjs

reef-docker-build: reef-image-checks
	@$(image_build) --file docker/Dockerfile.reef --tag "$(CORAL_WEB_DOCKER_IMAGE)" .

# The test runs from the recipe, not as a prerequisite: `make -j` runs
# prerequisites concurrently and would start it against a half-built image.
docker-test: docker-build reef-docker-build
	CONTAINER_ENGINE="$(CONTAINER_ENGINE)" CORAL_IMAGE="$(DOCKER_IMAGE)" \
	  CORAL_WEB_IMAGE="$(CORAL_WEB_DOCKER_IMAGE)" docker/image-test.sh

rust-checks: lint-migrations
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --locked -- -D warnings
	$(RUST_WORKSPACE_TESTS)
	# RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked

# ----------------------------------------------------------------------------
# Local Postgres-backed tests
# ----------------------------------------------------------------------------
# Runs the same normal Rust tests under the Postgres test selector.
# Backend-specific tests use coral-test-support's explicit database marker to
# return before arranging a fixture when the other Adapter is selected. When
# CORAL_TEST_POSTGRES_URL is set, the target uses that disposable test
# database. Otherwise it starts a containerized Postgres matching CI's major
# version and creates a fresh database inside the reusable container. Docker is
# preferred when both Docker and Podman are installed; set
# POSTGRES_CONTAINER_ENGINE=podman to
# choose Podman explicitly. The engine chooses an available localhost port by
# default; set LOCAL_POSTGRES_PORT=55432 if you need a stable host port.
#
#   make postgres-start   # start/wait for local containerized Postgres
#   make postgres-url     # print the local Postgres connection URL
#   make postgres-tests   # provision locally, then run the Rust repository gate
#   npm run test:e2e --prefix apps/reef -- --database postgres  # Reel product suite
#   POSTGRES_CONTAINER_ENGINE=podman make postgres-tests  # choose Podman
#   CORAL_TEST_POSTGRES_URL=... make postgres-tests  # use a disposable database
#   make postgres-stop    # stop the local Postgres container
#   make postgres-clean   # remove the local Postgres container

# Defaults and lifecycle behavior live in scripts/postgres.ts. These targets are
# CLI shortcuts only; command-line overrides cross bootstrap through environment.
postgres-start:
	@node scripts/postgres-cli.mjs start

postgres-url:
	@node scripts/postgres-cli.mjs url

postgres-stop:
	@node scripts/postgres-cli.mjs stop

postgres-clean:
	@node scripts/postgres-cli.mjs clean

# No recursive make invocation: make -n must remain side-effect free.
export CORAL_TEST_POSTGRES_URL LOCAL_POSTGRES_IMAGE LOCAL_POSTGRES_CONTAINER LOCAL_POSTGRES_PORT POSTGRES_CONTAINER_ENGINE

postgres-tests:
	@node scripts/postgres-tests.mjs $(RUST_WORKSPACE_TESTS) --profile postgres

# Run coordination load harness. Not a gate: its cells (fan-out, emit,
# concurrent Runs) measure one busy Workspace against the probes the rest of the
# server runs. Options and reports are described in scripts/load.mjs.
#   make load-test LOAD_ARGS="--calls 30,101 --latency 0,5,20 --release"
#   make load-test LOAD_ARGS="--backend sqlite"
#   make load-test LOAD_ARGS="--cells fanout --mode hold"
load-test:
	@node scripts/load.mjs $(LOAD_ARGS)

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
# Postgres migration linting
# ----------------------------------------------------------------------------
# Lints the Postgres-only migrations with Squawk. Settings live in
# .squawk.toml. Installs `squawk-cli` from npm, which ships a prebuilt binary.
#
#   make lint-migrations

lint-migrations:
	squawk --version >/dev/null 2>&1 || npm install --global squawk-cli
	squawk crates/coral-db/migrations/postgres/*.sql

# ----------------------------------------------------------------------------
# Protobuf API linting
# ----------------------------------------------------------------------------
# Lints crates/coral-grpc/proto with Buf.
#
#   make lint-proto   # check protobuf style and API-shape rules

lint-proto:
	cd crates/coral-grpc && buf lint
	cargo run -p xtask --locked -- generate-proto --check
