"""Run the ETL and/or ELT pipeline once WITHOUT Airflow (for testing the logic).

  python scripts/run_local.py etl            # September files only (no increment present)
  python scripts/run_local.py elt
  python scripts/run_local.py both --data-dir include/data/nammamart --db /tmp/test.duckdb

The Airflow DAGs call exactly the same functions, one per task.
"""
import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("which", choices=["etl", "elt", "both"])
    ap.add_argument("--data-dir")
    ap.add_argument("--db")
    ap.add_argument("--quarantine-dir")
    ap.add_argument("--work-dir")
    ap.add_argument("--run-id")
    ap.add_argument("--volume-check", action="store_true")
    ap.add_argument("--as-of")
    a = ap.parse_args(argv)
    os.environ["NM_BASE_DIR"] = str(ROOT)
    for k, v in [("NM_DATA_DIR", a.data_dir), ("NM_DB_PATH", a.db), ("NM_QUARANTINE_DIR", a.quarantine_dir),
                 ("NM_WORK_DIR", a.work_dir), ("NM_AS_OF", a.as_of)]:
        if v:
            os.environ[k] = str(Path(v).resolve()) if k != "NM_AS_OF" else v
    if a.volume_check:
        os.environ["NM_VOLUME_CHECK"] = "true"
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from nammamart import common, elt, etl
    cfg = common.get_settings()
    rid = a.run_id or "manual__" + datetime.now().strftime("%Y%m%dT%H%M%S")
    out = {}
    if a.which in ("etl", "both"):
        ex = {n: etl.extract(cfg, rid, n) for n in common.FILES}
        v = etl.validate(cfg, rid, ex)
        etl.quarantine(cfg, rid, v)
        t = etl.transform(cfg, rid, v)
        ld = etl.load(cfg, rid, t)
        etl.reconcile(cfg, rid, ex, ld)
        etl.report(cfg, rid)
        out["etl_audit"] = common.write_audit(cfg, common.DAG_ETL, rid, etl.ETL_TASKS, None)
        out["etl_load"] = ld
    if a.which in ("elt", "both"):
        out.update(elt.run_all(cfg, rid))
    return out


if __name__ == "__main__":
    print(main())
