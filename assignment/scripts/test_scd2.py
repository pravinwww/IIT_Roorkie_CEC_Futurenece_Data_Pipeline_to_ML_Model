"""Bonus proof: SCD Type 2 keeps loyalty-tier history. Changes one customer's tier between two runs."""
import shutil, subprocess, sys, tempfile
from pathlib import Path
import duckdb

root = Path(__file__).resolve().parents[1]
tmp = Path(tempfile.mkdtemp())
data = tmp / "data"; shutil.copytree(root / "include/data/nammamart", data)
def run(rid):
    subprocess.run([sys.executable, str(root / "scripts/run_local.py"), "both", "--data-dir", str(data), "--db", str(tmp / "wh.duckdb"),
                    "--quarantine-dir", str(tmp / "q"), "--work-dir", str(tmp / "w"), "--run-id", rid], check=True, capture_output=True)
def show(title):
    con = duckdb.connect(str(tmp / "wh.duckdb"), read_only=True)
    for t in ("dim_customer_scd2", "etl_dim_customer_scd2"):
        n = con.execute(f"select count(*), count(*) filter (where is_current) from {t}").fetchone()
        print(f"{title} | {t}: rows={n[0]} current={n[1]}")
    return con
run("scd__1"); show("after run 1")
run("scd__2"); show("after rerun, no change")
lines = (data / "customers.csv").read_text().splitlines()
hdr = lines[0].split(","); i = hdr.index("loyalty_tier")
cid = "C00001"
for k, l in enumerate(lines):
    if l.startswith(cid + ","):
        p = l.split(","); old = p[i]; p[i] = "Platinum" if old != "Platinum" else "Gold"; lines[k] = ",".join(p); new = p[i]
(data / "customers.csv").write_text("\n".join(lines) + "\n")
run("scd__3"); con = show("after tier change")
print(f"\n{cid}: {old} -> {new}")
for t in ("dim_customer_scd2", "etl_dim_customer_scd2"):
    print(f"\n{t}:"); print(con.execute(f"select customer_id, loyalty_tier, valid_from, valid_to, is_current from {t} where customer_id='{cid}' order by valid_from").df().to_string(index=False))
