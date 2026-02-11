from __future__ import annotations

import json
import re
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import mysql.connector
import pandas as pd

from inmate_dedupe.config import DedupeDuckDBConfig, SourceMysqlConfig


SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _assert_identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


class SourceMySQLReader:
    def __init__(self, cfg: SourceMysqlConfig):
        self.cfg = cfg

    def _connect(self):
        return mysql.connector.connect(
            host=self.cfg.host,
            port=self.cfg.port,
            user=self.cfg.user,
            password=self.cfg.password,
            database=self.cfg.database,
            autocommit=True,
        )

    def _normalized_custom_query(self) -> str | None:
        raw = self.cfg.custom_query
        if raw is None:
            return None
        query = str(raw).strip()
        if not query:
            return None
        while query.endswith(";"):
            query = query[:-1].strip()
        return query or None

    def _source_relation_sql(self) -> tuple[str, bool]:
        custom_query = self._normalized_custom_query()
        if custom_query:
            return f"({custom_query}) AS src", True
        table = _assert_identifier(self.cfg.source_table)
        return f"`{table}`", False

    @staticmethod
    def _column_ref(column: str, use_alias: bool) -> str:
        if use_alias:
            return f"`src`.`{column}`"
        return f"`{column}`"

    def validate_source_requirements(
        self,
        projection_columns: list[str],
        sample_rows: int = 500,
    ) -> dict[str, Any]:
        if sample_rows <= 0:
            raise ValueError("sample_rows must be > 0")

        source_relation_sql, use_alias = self._source_relation_sql()
        id_col = _assert_identifier(self.cfg.source_id_column)
        updated_col = (
            _assert_identifier(self.cfg.source_updated_at_column)
            if self.cfg.source_updated_at_column
            else None
        )
        for col in projection_columns:
            _assert_identifier(col)

        selected_cols = ", ".join(
            [f"{self._column_ref(col, use_alias)} AS `{col}`" for col in projection_columns]
        )
        id_ref = self._column_ref(id_col, use_alias)
        updated_ref = self._column_ref(updated_col, use_alias) if updated_col else None

        conn = self._connect()
        cursor = conn.cursor(dictionary=True, buffered=True)
        try:
            cursor.execute(f"SELECT {selected_cols} FROM {source_relation_sql} LIMIT 1")
            cursor.fetchall()

            sample_select = f"SELECT {id_ref} AS __record_id"
            if updated_ref is not None:
                sample_select += f", {updated_ref} AS __source_updated_at"
            sample_select += f" FROM {source_relation_sql}"

            if updated_ref is not None:
                sample_select += f" ORDER BY {updated_ref} ASC, {id_ref} ASC"
            else:
                sample_select += f" ORDER BY {id_ref} ASC"
            sample_select += " LIMIT %s"

            cursor.execute(sample_select, (sample_rows,))
            sample = pd.DataFrame(cursor.fetchall())

            issues: list[str] = []
            if sample.empty:
                issues.append("Source query returned no rows for sample window.")

            if "__record_id" in sample.columns:
                normalized_id = sample["__record_id"].map(lambda v: None if v is None else str(v).strip())
                null_or_blank_id = int((normalized_id.isna() | (normalized_id == "")).sum())
                duplicate_id = int(normalized_id.dropna().duplicated().sum())
            else:
                null_or_blank_id = sample_rows
                duplicate_id = 0
                issues.append(f"Missing required id column in result: {self.cfg.source_id_column}")

            null_updated = 0
            invalid_updated = 0
            if updated_col is None:
                issues.append("source_updated_at_column is null; incremental/bootstrap are not available.")
            elif "__source_updated_at" not in sample.columns:
                issues.append(f"Missing updated-at column in result: {self.cfg.source_updated_at_column}")
            else:
                updated_series = sample["__source_updated_at"]
                null_updated = int(updated_series.isna().sum())
                parsed_updated = pd.to_datetime(updated_series, errors="coerce")
                invalid_updated = int((~updated_series.isna() & parsed_updated.isna()).sum())

            incremental_ready = bool(updated_col is not None and "__source_updated_at" in sample.columns)
            if incremental_ready and invalid_updated > 0:
                incremental_ready = False
                issues.append(
                    f"Found {invalid_updated} non-null source_updated_at values that are not parseable as datetime in sample."
                )

            return {
                "ok": len(issues) == 0,
                "mode": "custom_query" if self._normalized_custom_query() else "table",
                "source_table": self.cfg.source_table,
                "source_id_column": self.cfg.source_id_column,
                "source_updated_at_column": self.cfg.source_updated_at_column,
                "projection_columns_count": len(projection_columns),
                "sample_rows_requested": sample_rows,
                "sample_rows_observed": int(len(sample)),
                "sample_null_or_blank_record_id": null_or_blank_id,
                "sample_duplicate_record_id": duplicate_id,
                "sample_null_source_updated_at": null_updated,
                "sample_invalid_source_updated_at": invalid_updated,
                "full_ready": True,
                "incremental_ready": incremental_ready,
                "bootstrap_ready": incremental_ready,
                "issues": issues,
            }
        finally:
            cursor.close()
            conn.close()

    def stream_source_rows(
        self,
        projection_columns: list[str],
        batch_size: int,
        since_ts: datetime | None = None,
    ) -> Iterator[pd.DataFrame]:
        source_relation_sql, use_alias = self._source_relation_sql()
        id_col = _assert_identifier(self.cfg.source_id_column)
        updated_col = (
            _assert_identifier(self.cfg.source_updated_at_column)
            if self.cfg.source_updated_at_column
            else None
        )
        for col in projection_columns:
            _assert_identifier(col)

        selected_cols = ", ".join(
            [f"{self._column_ref(col, use_alias)} AS `{col}`" for col in projection_columns]
        )
        sql = f"SELECT {selected_cols} FROM {source_relation_sql}"
        params: list[Any] = []
        if since_ts is not None:
            if not updated_col:
                raise ValueError("Incremental mode requires source_updated_at_column in source_mysql config")
            sql += f" WHERE {self._column_ref(updated_col, use_alias)} > %s"
            params.append(since_ts)

        if updated_col:
            sql += (
                f" ORDER BY {self._column_ref(updated_col, use_alias)} ASC,"
                f" {self._column_ref(id_col, use_alias)} ASC"
            )
        else:
            sql += f" ORDER BY {self._column_ref(id_col, use_alias)} ASC"

        conn = self._connect()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, tuple(params))
        try:
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield pd.DataFrame(rows)
        finally:
            cursor.close()
            conn.close()

    def fetch_source_slice(
        self,
        projection_columns: list[str],
        max_rows: int,
        cutoff_ts: datetime,
        cursor_updated_at: datetime | None = None,
        cursor_record_id: str | None = None,
    ) -> pd.DataFrame:
        if max_rows <= 0:
            raise ValueError("max_rows must be > 0")
        if not self.cfg.source_updated_at_column:
            raise ValueError("Bootstrap mode requires source_updated_at_column in source_mysql config")

        source_relation_sql, use_alias = self._source_relation_sql()
        id_col = _assert_identifier(self.cfg.source_id_column)
        updated_col = _assert_identifier(self.cfg.source_updated_at_column)
        for col in projection_columns:
            _assert_identifier(col)

        selected_cols = ", ".join(
            [f"{self._column_ref(col, use_alias)} AS `{col}`" for col in projection_columns]
        )
        updated_ref = self._column_ref(updated_col, use_alias)
        id_ref = self._column_ref(id_col, use_alias)
        sql = f"""
            SELECT {selected_cols}
              FROM {source_relation_sql}
             WHERE {updated_ref} IS NOT NULL
               AND {updated_ref} <= %s
        """
        params: list[Any] = [cutoff_ts]
        if cursor_updated_at is not None:
            sql += f"""
               AND (
                    {updated_ref} > %s
                    OR ({updated_ref} = %s AND {id_ref} > %s)
               )
            """
            params.extend([cursor_updated_at, cursor_updated_at, cursor_record_id or ""])

        sql += f"""
             ORDER BY {updated_ref} ASC, {id_ref} ASC
             LIMIT %s
        """
        params.append(max_rows)

        conn = self._connect()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, tuple(params))
        try:
            rows = cursor.fetchall()
            return pd.DataFrame(rows)
        finally:
            cursor.close()
            conn.close()


class DedupeDuckDBStore:
    def __init__(self, cfg: DedupeDuckDBConfig):
        self.cfg = cfg

    def _connect(self):
        return duckdb.connect(str(self.cfg.path))

    def _ensure_unique_inmates_columns(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute("ALTER TABLE unique_inmates ADD COLUMN IF NOT EXISTS cif_number VARCHAR")
        conn.execute("ALTER TABLE unique_inmates ADD COLUMN IF NOT EXISTS canonical_trace_json VARCHAR")

    def _ensure_state_tables(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_bootstrap_state (
              source_table VARCHAR PRIMARY KEY,
              cutoff_ts TIMESTAMP NOT NULL,
              cursor_updated_at TIMESTAMP NULL,
              cursor_record_id VARCHAR NULL,
              completed BOOLEAN NOT NULL DEFAULT FALSE,
              rows_processed_total BIGINT NOT NULL DEFAULT 0,
              updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_watermark (
              source_table VARCHAR PRIMARY KEY,
              source_updated_at TIMESTAMP NULL,
              updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _complete_run_tx(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        run_id: int,
        status: str,
        records_processed: int,
        auto_matches: int,
        review_candidates: int,
        new_entities: int,
        error_message: str | None = None,
    ) -> None:
        conn.execute(
            """
            UPDATE dedupe_run
               SET status = ?,
                   records_processed = ?,
                   auto_matches = ?,
                   review_candidates = ?,
                   new_entities = ?,
                   completed_at = CURRENT_TIMESTAMP,
                   error_message = ?
             WHERE run_id = ?
            """,
            [
                status,
                records_processed,
                auto_matches,
                review_candidates,
                new_entities,
                error_message,
                run_id,
            ],
        )

    def _upsert_unique_inmates_tx(self, conn: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self._ensure_unique_inmates_columns(conn)
        df = pd.DataFrame(rows)
        conn.register("tmp_unique_inmates", df)
        conn.execute(
            """
            DELETE FROM unique_inmates
             WHERE entity_id IN (SELECT entity_id FROM tmp_unique_inmates)
            """
        )
        conn.execute(
            """
            INSERT INTO unique_inmates (
              entity_id,
              cif_number,
              canonical_record_id,
              canonical_id_upt,
              canonical_nik,
              canonical_nomor_induk_nasional,
              canonical_nama_lengkap,
              canonical_alias_names,
              canonical_nama_kecil,
              canonical_tanggal_lahir,
              canonical_id_jenis_kelamin,
              canonical_alamat,
              canonical_kodepos,
              canonical_telepon,
              canonical_nm_ayah,
              canonical_nm_ibu,
              canonical_nm_istri_suami,
              canonical_payload_json,
              canonical_trace_json,
              created_run_id,
              created_at,
              updated_at
            )
            SELECT entity_id,
                   cif_number,
                   canonical_record_id,
                   canonical_id_upt,
                   canonical_nik,
                   canonical_nomor_induk_nasional,
                   canonical_nama_lengkap,
                   canonical_alias_names,
                   canonical_nama_kecil,
                   canonical_tanggal_lahir,
                   canonical_id_jenis_kelamin,
                   canonical_alamat,
                   canonical_kodepos,
                   canonical_telepon,
                   canonical_nm_ayah,
                   canonical_nm_ibu,
                   canonical_nm_istri_suami,
                   canonical_payload_json,
                   canonical_trace_json,
                   created_run_id,
                   COALESCE(created_at, CURRENT_TIMESTAMP),
                   CURRENT_TIMESTAMP
              FROM tmp_unique_inmates
            """
        )
        conn.unregister("tmp_unique_inmates")

    def _upsert_record_entity_map_tx(self, conn: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        df = pd.DataFrame(rows)
        conn.register("tmp_record_entity_map", df)
        conn.execute(
            """
            DELETE FROM record_entity_map
             WHERE record_id IN (SELECT record_id FROM tmp_record_entity_map)
            """
        )
        conn.execute(
            """
            INSERT INTO record_entity_map
            SELECT record_id,
                   entity_id,
                   source_updated_at,
                   best_match_probability,
                   decision,
                   linked_run_id,
                   CURRENT_TIMESTAMP
              FROM tmp_record_entity_map
            """
        )
        conn.unregister("tmp_record_entity_map")

    def _insert_review_candidates_tx(self, conn: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        df = pd.DataFrame(rows)
        conn.register("tmp_review_queue", df)
        start_review_id = int(conn.execute("SELECT COALESCE(MAX(review_id), 0) FROM review_queue").fetchone()[0])
        conn.execute(
            """
            INSERT INTO review_queue
            SELECT ? + row_number() OVER () AS review_id,
                   run_id,
                   left_record_id,
                   right_record_id,
                   candidate_entity_id,
                   match_probability,
                   'open' AS status,
                   NULL AS reviewer,
                   NULL AS notes,
                   CURRENT_TIMESTAMP,
                   NULL AS reviewed_at
              FROM (
                SELECT t.*
                  FROM tmp_review_queue t
                 WHERE NOT EXISTS (
                       SELECT 1
                         FROM review_queue r
                        WHERE r.run_id = t.run_id
                          AND COALESCE(r.left_record_id, '') = COALESCE(t.left_record_id, '')
                          AND COALESCE(r.right_record_id, '') = COALESCE(t.right_record_id, '')
                          AND COALESCE(CAST(r.candidate_entity_id AS VARCHAR), '') =
                              COALESCE(CAST(t.candidate_entity_id AS VARCHAR), '')
                     )
              ) s
            """,
            [start_review_id],
        )
        conn.unregister("tmp_review_queue")

    def _upsert_bootstrap_state_tx(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        source_table: str,
        cutoff_ts: datetime,
        cursor_updated_at: datetime | None,
        cursor_record_id: str | None,
        completed: bool,
        rows_processed_total: int,
    ) -> None:
        self._ensure_state_tables(conn)
        conn.execute("DELETE FROM pipeline_bootstrap_state WHERE source_table = ?", [source_table])
        conn.execute(
            """
            INSERT INTO pipeline_bootstrap_state
              (source_table, cutoff_ts, cursor_updated_at, cursor_record_id, completed, rows_processed_total, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                source_table,
                cutoff_ts,
                cursor_updated_at,
                cursor_record_id,
                completed,
                int(rows_processed_total),
            ],
        )

    def _set_watermark_if_newer_tx(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        source_table: str,
        candidate: datetime,
    ) -> None:
        self._ensure_state_tables(conn)
        existing = conn.execute(
            "SELECT source_updated_at FROM pipeline_watermark WHERE source_table = ?",
            [source_table],
        ).fetchone()
        if existing is None:
            conn.execute(
                """
                INSERT INTO pipeline_watermark (source_table, source_updated_at, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                """,
                [source_table, candidate],
            )
            return
        current = _as_datetime(existing[0])
        if current is None or candidate > current:
            conn.execute(
                """
                UPDATE pipeline_watermark
                   SET source_updated_at = ?,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE source_table = ?
                """,
                [candidate, source_table],
            )

    def apply_schema(self, sql_path: str | Path) -> None:
        sql_text = Path(sql_path).expanduser().read_text(encoding="utf-8")
        conn = self._connect()
        try:
            conn.execute(sql_text)
            self._ensure_unique_inmates_columns(conn)
            self._ensure_state_tables(conn)
            conn.execute(
                """
                UPDATE unique_inmates
                   SET cif_number = 'CIF' || lpad(CAST(entity_id AS VARCHAR), 12, '0')
                 WHERE cif_number IS NULL OR trim(cif_number) = ''
                """
            )
        finally:
            conn.close()

    def create_run(
        self,
        run_type: str,
        model_version: str,
        threshold_auto: float,
        threshold_review: float,
        source_table: str,
        source_since_ts: datetime | None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        conn = self._connect()
        try:
            run_id = int(conn.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM dedupe_run").fetchone()[0])
            conn.execute(
                """
                INSERT INTO dedupe_run
                  (run_id, run_type, status, model_version, source_table, source_since_ts,
                   threshold_auto, threshold_review, metadata_json)
                VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    run_type,
                    model_version,
                    source_table,
                    source_since_ts,
                    threshold_auto,
                    threshold_review,
                    json.dumps(metadata or {}),
                ],
            )
            return run_id
        finally:
            conn.close()

    def complete_run(
        self,
        run_id: int,
        status: str,
        records_processed: int,
        auto_matches: int,
        review_candidates: int,
        new_entities: int,
        error_message: str | None = None,
    ) -> None:
        conn = self._connect()
        try:
            self._complete_run_tx(
                conn,
                run_id=run_id,
                status=status,
                records_processed=records_processed,
                auto_matches=auto_matches,
                review_candidates=review_candidates,
                new_entities=new_entities,
                error_message=error_message,
            )
        finally:
            conn.close()

    def fetch_entity_index(self) -> pd.DataFrame:
        conn = self._connect()
        try:
            self._ensure_unique_inmates_columns(conn)
            return conn.execute(
                """
                SELECT entity_id,
                       cif_number,
                       canonical_record_id,
                       canonical_id_upt,
                       canonical_nik,
                       canonical_nomor_induk_nasional,
                       canonical_nama_lengkap,
                       canonical_alias_names,
                       canonical_nama_kecil,
                       canonical_tanggal_lahir,
                       canonical_id_jenis_kelamin,
                       canonical_alamat,
                       canonical_kodepos,
                       canonical_telepon,
                       canonical_nm_ayah,
                       canonical_nm_ibu,
                       canonical_nm_istri_suami,
                       canonical_payload_json,
                       canonical_trace_json,
                       created_run_id,
                       created_at
                  FROM unique_inmates
                """
            ).df()
        finally:
            conn.close()

    def reserve_entity_ids(self, count: int) -> list[int]:
        if count <= 0:
            return []
        conn = self._connect()
        try:
            start = int(conn.execute("SELECT COALESCE(MAX(entity_id), 0) + 1 FROM unique_inmates").fetchone()[0])
            return list(range(start, start + count))
        finally:
            conn.close()

    def upsert_unique_inmates(self, rows: list[dict[str, Any]]) -> None:
        conn = self._connect()
        try:
            self._upsert_unique_inmates_tx(conn, rows)
        finally:
            conn.close()

    def upsert_record_entity_map(self, rows: list[dict[str, Any]]) -> None:
        conn = self._connect()
        try:
            self._upsert_record_entity_map_tx(conn, rows)
        finally:
            conn.close()

    def insert_review_candidates(self, rows: list[dict[str, Any]]) -> None:
        conn = self._connect()
        try:
            self._insert_review_candidates_tx(conn, rows)
        finally:
            conn.close()

    def load_bootstrap_state(self, source_table: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            self._ensure_state_tables(conn)
            row = conn.execute(
                """
                SELECT cutoff_ts,
                       cursor_updated_at,
                       cursor_record_id,
                       completed,
                       rows_processed_total
                  FROM pipeline_bootstrap_state
                 WHERE source_table = ?
                """,
                [source_table],
            ).fetchone()
            if row is None:
                return None
            cutoff = _as_datetime(row[0])
            if cutoff is None:
                return None
            return {
                "cutoff_ts": cutoff,
                "cursor_updated_at": _as_datetime(row[1]),
                "cursor_record_id": row[2],
                "completed": bool(row[3]),
                "rows_processed_total": int(row[4]),
            }
        finally:
            conn.close()

    def load_watermark(self, source_table: str) -> datetime | None:
        conn = self._connect()
        try:
            self._ensure_state_tables(conn)
            row = conn.execute(
                """
                SELECT source_updated_at
                  FROM pipeline_watermark
                 WHERE source_table = ?
                """,
                [source_table],
            ).fetchone()
            if row is None:
                return None
            return _as_datetime(row[0])
        finally:
            conn.close()

    def save_watermark(self, source_table: str, source_updated_at: datetime) -> None:
        conn = self._connect()
        try:
            self._set_watermark_if_newer_tx(
                conn,
                source_table=source_table,
                candidate=source_updated_at,
            )
        finally:
            conn.close()

    def commit_bootstrap_batch(
        self,
        *,
        run_id: int,
        source_table: str,
        entity_rows: list[dict[str, Any]],
        map_rows: list[dict[str, Any]],
        review_rows: list[dict[str, Any]],
        records_processed: int,
        auto_matches: int,
        review_candidates: int,
        new_entities: int,
        cutoff_ts: datetime,
        cursor_updated_at: datetime | None,
        cursor_record_id: str | None,
        completed: bool,
        rows_processed_total: int,
        watermark_candidate: datetime | None,
    ) -> None:
        conn = self._connect()
        try:
            self._ensure_unique_inmates_columns(conn)
            self._ensure_state_tables(conn)
            conn.execute("BEGIN TRANSACTION")
            try:
                self._upsert_unique_inmates_tx(conn, entity_rows)
                self._upsert_record_entity_map_tx(conn, map_rows)
                self._insert_review_candidates_tx(conn, review_rows)
                self._upsert_bootstrap_state_tx(
                    conn,
                    source_table=source_table,
                    cutoff_ts=cutoff_ts,
                    cursor_updated_at=cursor_updated_at,
                    cursor_record_id=cursor_record_id,
                    completed=completed,
                    rows_processed_total=rows_processed_total,
                )
                if watermark_candidate is not None:
                    self._set_watermark_if_newer_tx(
                        conn,
                        source_table=source_table,
                        candidate=watermark_candidate,
                    )
                self._complete_run_tx(
                    conn,
                    run_id=run_id,
                    status="completed",
                    records_processed=records_processed,
                    auto_matches=auto_matches,
                    review_candidates=review_candidates,
                    new_entities=new_entities,
                    error_message=None,
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    if isinstance(timestamp, pd.Timestamp):
        return timestamp.to_pydatetime()
    if isinstance(timestamp, datetime):
        return timestamp
    return None
