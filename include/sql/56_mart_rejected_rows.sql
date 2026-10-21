-- MART: Q6 rows rejected per run, by file and by reason (history kept per run_id)
CREATE TABLE IF NOT EXISTS mart_rejected_rows (run_id VARCHAR, source_file VARCHAR, failure_reason VARCHAR,
  rows_rejected BIGINT, rejected_at TIMESTAMP);
DELETE FROM mart_rejected_rows WHERE run_id = '${RUN_ID}';
INSERT INTO mart_rejected_rows
SELECT run_id, source_file, reason, count(*), now()
FROM (SELECT run_id, source_file, unnest(string_split(failure_reason, '; ')) AS reason
      FROM elt_quarantine WHERE run_id = '${RUN_ID}')
GROUP BY ALL;
