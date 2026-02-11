CREATE TABLE IF NOT EXISTS dedupe_run (
  run_id BIGINT PRIMARY KEY,
  run_type VARCHAR NOT NULL,
  status VARCHAR NOT NULL,
  model_version VARCHAR NOT NULL,
  source_table VARCHAR NOT NULL,
  source_since_ts TIMESTAMP NULL,
  records_processed BIGINT NOT NULL DEFAULT 0,
  auto_matches BIGINT NOT NULL DEFAULT 0,
  review_candidates BIGINT NOT NULL DEFAULT 0,
  new_entities BIGINT NOT NULL DEFAULT 0,
  threshold_auto DOUBLE NOT NULL,
  threshold_review DOUBLE NOT NULL,
  started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMP NULL,
  metadata_json VARCHAR NULL,
  error_message VARCHAR NULL
);

CREATE TABLE IF NOT EXISTS unique_inmates (
  entity_id BIGINT PRIMARY KEY,
  cif_number VARCHAR NULL,
  canonical_record_id VARCHAR NOT NULL,
  canonical_id_upt VARCHAR NULL,
  canonical_nik VARCHAR NULL,
  canonical_nomor_induk_nasional VARCHAR NULL,
  canonical_nama_lengkap VARCHAR NULL,
  canonical_alias_names VARCHAR NULL,
  canonical_nama_kecil VARCHAR NULL,
  canonical_tanggal_lahir DATE NULL,
  canonical_id_jenis_kelamin VARCHAR NULL,
  canonical_alamat VARCHAR NULL,
  canonical_kodepos VARCHAR NULL,
  canonical_telepon VARCHAR NULL,
  canonical_nm_ayah VARCHAR NULL,
  canonical_nm_ibu VARCHAR NULL,
  canonical_nm_istri_suami VARCHAR NULL,
  canonical_payload_json VARCHAR NULL,
  canonical_trace_json VARCHAR NULL,
  created_run_id BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE unique_inmates ADD COLUMN IF NOT EXISTS cif_number VARCHAR;
ALTER TABLE unique_inmates ADD COLUMN IF NOT EXISTS canonical_trace_json VARCHAR;

CREATE TABLE IF NOT EXISTS record_entity_map (
  record_id VARCHAR PRIMARY KEY,
  entity_id BIGINT NOT NULL,
  source_updated_at TIMESTAMP NULL,
  best_match_probability DOUBLE NOT NULL,
  decision VARCHAR NOT NULL,
  linked_run_id BIGINT NOT NULL,
  linked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS review_queue (
  review_id BIGINT PRIMARY KEY,
  run_id BIGINT NOT NULL,
  left_record_id VARCHAR NOT NULL,
  right_record_id VARCHAR NULL,
  candidate_entity_id BIGINT NULL,
  match_probability DOUBLE NOT NULL,
  status VARCHAR NOT NULL DEFAULT 'open',
  reviewer VARCHAR NULL,
  notes VARCHAR NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  reviewed_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS pipeline_bootstrap_state (
  source_table VARCHAR PRIMARY KEY,
  cutoff_ts TIMESTAMP NOT NULL,
  cursor_updated_at TIMESTAMP NULL,
  cursor_record_id VARCHAR NULL,
  completed BOOLEAN NOT NULL DEFAULT FALSE,
  rows_processed_total BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_watermark (
  source_table VARCHAR PRIMARY KEY,
  source_updated_at TIMESTAMP NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_record_entity_map_entity_id ON record_entity_map(entity_id);
CREATE INDEX IF NOT EXISTS idx_review_queue_status ON review_queue(status);
CREATE INDEX IF NOT EXISTS idx_unique_inmates_nik ON unique_inmates(canonical_nik);
CREATE INDEX IF NOT EXISTS idx_unique_inmates_nin ON unique_inmates(canonical_nomor_induk_nasional);

CREATE OR REPLACE VIEW entity_audit_view AS
WITH member_counts AS (
  SELECT entity_id, COUNT(*) AS member_count
  FROM record_entity_map
  GROUP BY entity_id
)
SELECT
  ui.entity_id,
  ui.cif_number,
  ui.canonical_record_id,
  rem.record_id AS member_record_id,
  rem.record_id = ui.canonical_record_id AS is_canonical_member,
  COALESCE(mc.member_count, 0) AS member_count,
  rem.decision,
  rem.best_match_probability,
  rem.source_updated_at,
  rem.linked_run_id,
  ui.created_run_id AS entity_created_run_id,
  ui.updated_at AS entity_updated_at,
  ui.canonical_nik,
  ui.canonical_nomor_induk_nasional,
  ui.canonical_nama_lengkap,
  ui.canonical_tanggal_lahir,
  ui.canonical_id_jenis_kelamin,
  ui.canonical_trace_json,
  ui.canonical_payload_json
FROM unique_inmates ui
LEFT JOIN record_entity_map rem ON rem.entity_id = ui.entity_id
LEFT JOIN member_counts mc ON mc.entity_id = ui.entity_id;
