#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <llvm-cov-json> [output-markdown-path]" >&2
  exit 1
fi

json_path="$1"
output_path="${2:-/dev/stdout}"
workspace_crates_json="$(cargo metadata --no-deps --format-version 1 | jq -c '[.packages[].name]')"

jq -r '
  def pct(covered; total):
    if total == 0 then
      "n/a"
    else
      ((((10000 * covered) / total) | round) / 100 | tostring) + "%"
    end;
  def ratio(covered; total): "\(covered)/\(total)";
  .data[0] as $root
  | (
      $root.files
      | map(select(.filename | test("/crates/[^/]+/")))
      | map({
          crate: (.filename | capture("/crates/(?<crate>[^/]+)/").crate),
          lines_count: .summary.lines.count,
          lines_covered: .summary.lines.covered,
          functions_count: .summary.functions.count,
          functions_covered: .summary.functions.covered,
          regions_count: .summary.regions.count,
          regions_covered: .summary.regions.covered
        })
      | sort_by(.crate)
      | group_by(.crate)
      | map({
          crate: .[0].crate,
          lines_count: (map(.lines_count) | add),
          lines_covered: (map(.lines_covered) | add),
          functions_count: (map(.functions_count) | add),
          functions_covered: (map(.functions_covered) | add),
          regions_count: (map(.regions_count) | add),
          regions_covered: (map(.regions_covered) | add)
        })
      | map({key: .crate, value: .})
      | from_entries
    ) as $crate_totals
  | (
      $workspace_crates
      | sort
      | map(
          $crate_totals[.] // {
            crate: .,
            lines_count: 0,
            lines_covered: 0,
            functions_count: 0,
            functions_covered: 0,
            regions_count: 0,
            regions_covered: 0
          }
        )
    ) as $crates
  | [
      "# Coverage Summary",
      "",
      "| Crate | Lines | Functions | Regions |",
      "| --- | ---: | ---: | ---: |",
      (
        $crates[]
        | "| `\(.crate)` | \(pct(.lines_covered; .lines_count)) (\(ratio(.lines_covered; .lines_count))) | \(pct(.functions_covered; .functions_count)) (\(ratio(.functions_covered; .functions_count))) | \(pct(.regions_covered; .regions_count)) (\(ratio(.regions_covered; .regions_count))) |"
      ),
      "",
      "Workspace totals:",
      "",
      "| Metric | Coverage |",
      "| --- | ---: |",
      "| Lines | \(pct($root.totals.lines.covered; $root.totals.lines.count)) (\(ratio($root.totals.lines.covered; $root.totals.lines.count))) |",
      "| Functions | \(pct($root.totals.functions.covered; $root.totals.functions.count)) (\(ratio($root.totals.functions.covered; $root.totals.functions.count))) |",
      "| Regions | \(pct($root.totals.regions.covered; $root.totals.regions.count)) (\(ratio($root.totals.regions.covered; $root.totals.regions.count))) |"
    ]
  | join("\n")
' --argjson workspace_crates "$workspace_crates_json" "$json_path" > "$output_path"
