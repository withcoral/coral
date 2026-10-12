#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.9"]
# ///
"""Plot metrics to track from a benchmark's results.jsonl as one PNG.

Usage: uv run plot.py <results.jsonl> <out.png> <measure> [<measure> ...]

Each <measure> is a key under "tracked" or "standard" in the result lines and
gets its own panel, stacked top to bottom. Yes/no values are plotted as the share
of runs that were yes, per (sha, agent). Numbers are plotted as the median.
The x axis is commit SHAs in date order. Non-main branches are labelled.
"""
import json
import math
import logging
import statistics
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("agg")
import matplotlib.pyplot as plt

# xkcd style tries xkcd Script, Comic Neue, then Comic Sans MS. Quiet the lookup
# warnings so whichever is installed is used without noise.
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

AGENTS = ["claude-code", "codex", "opencode"]
COLOURS = {"claude-code": "#2a78d6", "codex": "#eb6834", "opencode": "#1baf7a"}


def load(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def value(row, measure):
    for section in ("tracked", "standard"):
        if measure in row.get(section, {}):
            return row[section][measure]
    return None


def sha_order(rows):
    first_date, branch = {}, {}
    for r in rows:
        first_date[r["sha"]] = min(first_date.get(r["sha"], r["date"]), r["date"])
        branch[r["sha"]] = r.get("branch", "main")
    return sorted(first_date, key=first_date.get), branch


def aggregate(rows, measure):
    by = defaultdict(list)
    kind = None
    for r in rows:
        v = value(r, measure)
        if v is None:
            continue
        kind = kind or ("yesno" if isinstance(v, bool) else "number")
        by[(r["sha"], r["agent"])].append(v)
    points = {
        key: (sum(1 for v in vs if v) / len(vs)) if kind == "yesno" else statistics.median(vs)
        for key, vs in by.items()
    }
    return kind, points


def draw_panel(ax, measure, kind, shas, points):
    for agent in AGENTS:
        series = [(i, points[(sha, agent)]) for i, sha in enumerate(shas) if (sha, agent) in points]
        if not series:
            continue
        ax.plot(*zip(*series), marker="o", ms=6, lw=2, color=COLOURS[agent], label=agent,
                markeredgecolor="white", markeredgewidth=1.5)
    ax.set_title(f"{measure} ({'share of runs' if kind == 'yesno' else 'median'})", loc="left")
    if kind == "yesno":
        ax.set_ylim(0, 1.05)
        ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    else:
        ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="#e6e6e3")
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


def main():
    if len(sys.argv) < 4:
        sys.exit(__doc__)
    results, out, measures = sys.argv[1], sys.argv[2], sys.argv[3:]
    rows = load(results)
    shas, branch = sha_order(rows)
    plt.xkcd()
    # A near-square grid: ceil(sqrt(n)) columns, as many rows as needed.
    grid_cols = math.ceil(math.sqrt(len(measures)))
    grid_rows = math.ceil(len(measures) / grid_cols)
    fig, axes = plt.subplots(grid_rows, grid_cols, figsize=(5 * grid_cols, 3.6 * grid_rows), squeeze=False)
    panels = axes.flatten()
    for ax in panels[len(measures):]:
        ax.set_visible(False)
    for ax, measure in zip(panels, measures):
        kind, points = aggregate(rows, measure)
        if not points:
            sys.exit(f"no values for {measure!r} in {results}")
        draw_panel(ax, measure, kind, shas, points)
    ticks = [sha[:7] + ("" if branch[sha] == "main" else f"\n{branch[sha]}") for sha in shas]
    for i, ax in enumerate(panels[:len(measures)]):
        ax.set_xticks(range(len(shas)), ticks)
        ax.set_xlim(-0.5, len(shas) - 0.5)
        # Label the commits only on the lowest panel of each column.
        if i + grid_cols < len(measures):
            ax.tick_params(labelbottom=False)
    legend = {}
    for ax in panels[:len(measures)]:
        handles, labels = ax.get_legend_handles_labels()
        legend.update(zip(labels, handles))
    fig.legend(legend.values(), legend.keys(), frameon=False, loc="upper center", ncol=len(legend), bbox_to_anchor=(0.5, 1.0))
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.35 / (3.6 * grid_rows)))
    fig.savefig(out, dpi=150)


if __name__ == "__main__":
    main()
