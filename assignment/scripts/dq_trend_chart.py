"""Bonus: data-quality trend chart built from dq_results.

Usage: python3 scripts/dq_trend_chart.py <warehouse.duckdb> [out.png]
For every run it plots the share of checks that PASSED, per data-quality dimension, for both pipelines.
"""
import sys

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

db = sys.argv[1]
out = sys.argv[2] if len(sys.argv) > 2 else "answers/dq_trend.png"
con = duckdb.connect(db, read_only=True)
df = con.execute("""SELECT dag_id, run_id, dimension, min(checked_at) AS t,
                           round(100.0 * count(*) FILTER (WHERE status = 'PASS') / count(*), 1) AS pass_pct
                    FROM dq_results GROUP BY ALL ORDER BY t""").df()
runs = {r: i + 1 for i, r in enumerate(df.sort_values("t").run_id.drop_duplicates())}
df["n"] = df.run_id.map(runs)
fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True)
for ax, (dag, g) in zip(axes, df.groupby("dag_id")):
    for dim, d in g.groupby("dimension"):
        d = d.sort_values("n")
        ax.plot(d.n, d.pass_pct, marker="o", label=dim)
    ax.set_title(dag.replace("nammamart_", "").replace("_pravin", "").upper() + " pipeline")
    ax.set_xlabel("run number (oldest = 1)")
    ax.set_xticks(sorted(runs.values()))
    ax.grid(alpha=.3)
axes[0].set_ylabel("% of checks passed")
axes[0].set_ylim(0, 105)
h, l = axes[0].get_legend_handles_labels()
fig.legend(h, l, loc="lower center", ncol=6, fontsize=9, frameon=False)
fig.suptitle("Data-quality pass rate per run, by dimension (from dq_results)")
fig.tight_layout(rect=(0, 0.07, 1, 1))
fig.savefig(out, dpi=130)
print("saved", out, "| runs:", len(runs))
