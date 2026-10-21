"""Run answers/business_questions_{etl,elt}.sql on a warehouse file and print markdown (SQL + result)."""
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
db = sys.argv[1]
con = duckdb.connect(db, read_only=True)
for kind in ("etl", "elt"):
    print(f"\n## Answers from the {kind.upper()} pipeline\n")
    text = (ROOT / "answers" / f"business_questions_{kind}.sql").read_text()
    blocks, cur, title = [], [], None
    for line in text.splitlines():
        if line.startswith("-- Q"):
            title = line[3:]
        if line.strip() and not line.startswith("--"):
            cur.append(line)
            if line.rstrip().endswith(";"):
                blocks.append((title, "\n".join(cur)))
                cur = []
    last = None
    for title, sql in blocks:
        if title != last:
            print(f"### {title}\n")
            last = title
        df = con.execute(sql.rstrip(";")).df()
        shown = df.head(12)
        print("```sql\n" + sql + "\n```\n")
        print(shown.to_markdown(index=False) if len(shown) else "_(no rows)_")
        if len(df) > len(shown):
            print(f"\n_...{len(df) - len(shown)} more rows ({len(df)} total)_")
        print()
