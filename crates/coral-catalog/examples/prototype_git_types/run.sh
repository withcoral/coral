#!/usr/bin/env bash
# PROTOTYPE — throwaway. Exports the Demo Workspace's Providers from a copy of the
# local dev database, regenerates declarations with the current materializer,
# and type-checks the sample functions outside Coral with TypeScript 6.0.3.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../../.." && pwd)
scratch=${TMPDIR:-/tmp}/coral-PROTOTYPE-git-types-wipe-me
workspace=${1:?usage: run.sh <workspace-id>}
rm -rf "$scratch" && mkdir -p "$scratch/inputs"
sqlite3 -readonly "$root/.dev/state/coral.db" ".backup $scratch/coral.db"
sqlite3 "$scratch/coral.db" "select p.namespace || '.' || g.input_kind, writefile('$scratch/inputs/' || p.namespace || '.' || g.input_kind, g.pinned_input) from provider_records p join provider_generations g on g.id = p.active_generation_id where p.workspace_id = '$workspace'"
cp -r "$here" "$scratch/repo"
cargo run -q --manifest-path "$root/Cargo.toml" -p coral-catalog --example prototype_export_types -- "$scratch/inputs" "$scratch/repo"
cd "$scratch/repo" && node check.mjs --extendedDiagnostics
