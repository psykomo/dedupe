from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from inmate_dedupe.config import AppConfig
from inmate_dedupe.mysql_store import DedupeDuckDBStore, SourceMySQLReader
from inmate_dedupe.normalize import normalize_entity_frame, normalize_source_frame
from inmate_dedupe.splink_runner import run_full_dedupe, run_incremental_link


@dataclass
class RunSummary:
    run_id: int
    run_type: str
    records_processed: int
    auto_matches: int
    review_candidates: int
    new_entities: int
    scored_pairs: int


@dataclass
class BootstrapState:
    cutoff_ts: datetime
    cursor_updated_at: datetime | None
    cursor_record_id: str | None
    completed: bool
    rows_processed_total: int


@dataclass
class LinkBatchOutcome:
    auto_links: pd.DataFrame
    review_links: pd.DataFrame
    unmatched: pd.DataFrame
    auto_matches: int
    review_candidates: int
    new_entities: int
    scored_pairs: int
    entity_rows: list[dict[str, Any]]
    map_rows: list[dict[str, Any]]
    review_rows: list[dict[str, Any]]


CANONICAL_FIELD_MAP: tuple[tuple[str, str], ...] = (
    ("canonical_id_upt", "id_upt"),
    ("canonical_nik", "nik"),
    ("canonical_nomor_induk_nasional", "nomor_induk_nasional"),
    ("canonical_nama_lengkap", "nama_lengkap"),
    ("canonical_alias_names", "alias_names"),
    ("canonical_nama_kecil", "nama_kecil"),
    ("canonical_tanggal_lahir", "tanggal_lahir"),
    ("canonical_id_jenis_kelamin", "id_jenis_kelamin"),
    ("canonical_alamat", "alamat_combined"),
    ("canonical_kodepos", "kodepos"),
    ("canonical_telepon", "telepon_any"),
    ("canonical_nm_ayah", "nm_ayah"),
    ("canonical_nm_ibu", "nm_ibu"),
    ("canonical_nm_istri_suami", "nm_istri_suami"),
)

CIF_PREFIX = "CIF"
CIF_DIGITS = 12


class DedupePipeline:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.source_store = SourceMySQLReader(cfg.source_mysql)
        self.dedupe_store = DedupeDuckDBStore(cfg.dedupe_duckdb)

    def init_schema(self, sql_path: str | Path) -> None:
        self.dedupe_store.apply_schema(sql_path)

    def validate_source(self, sample_rows: int = 500) -> dict[str, Any]:
        projection = self._projection_columns()
        return self.source_store.validate_source_requirements(
            projection_columns=projection,
            sample_rows=sample_rows,
        )

    def _use_source_id_cursor_mode(self) -> bool:
        return not bool(self.cfg.source_mysql.source_updated_at_column)

    def run_full(self) -> RunSummary:
        run_id = self.dedupe_store.create_run(
            run_type="full",
            model_version=self.cfg.run.model_version,
            threshold_auto=self.cfg.thresholds.auto_match_probability,
            threshold_review=self.cfg.thresholds.review_match_probability,
            source_table=self.cfg.source_mysql.source_table,
            source_since_ts=None,
            metadata={"blocking_rules": self.cfg.blocking_rules.full},
        )
        try:
            frame, max_seen = self._extract_normalized_source(since_ts=None)
            if frame.empty:
                self.dedupe_store.complete_run(
                    run_id=run_id,
                    status="completed",
                    records_processed=0,
                    auto_matches=0,
                    review_candidates=0,
                    new_entities=0,
                )
                return RunSummary(run_id, "full", 0, 0, 0, 0, 0)

            result = run_full_dedupe(frame, self.cfg)
            clusters = result.clusters.copy()
            cluster_sizes = clusters.groupby("cluster_key")["record_id"].size().to_dict()
            unique_clusters = sorted(clusters["cluster_key"].unique().tolist())

            entity_ids = self.dedupe_store.reserve_entity_ids(len(unique_clusters))
            cluster_to_entity = dict(zip(unique_clusters, entity_ids))
            clusters["entity_id"] = clusters["cluster_key"].map(cluster_to_entity)

            canonical_lookup = frame.set_index("record_id").to_dict(orient="index")
            cluster_members = (
                clusters.groupby("cluster_key")["record_id"].apply(lambda s: [str(v) for v in s.tolist()]).to_dict()
            )
            entity_rows = []
            for cluster_key, members in cluster_members.items():
                member_rows = [_source_row_with_record_id(canonical_lookup[record_id], record_id) for record_id in members]
                canonical_values, canonical_trace, anchor_row = _build_survivor_from_members(member_rows)
                entity_rows.append(
                    {
                        "entity_id": int(cluster_to_entity[int(cluster_key)]),
                        "cif_number": _resolve_cif_number(int(cluster_to_entity[int(cluster_key)]), None),
                        **canonical_values,
                        "canonical_payload_json": json.dumps(_payload_from_source_row(anchor_row), ensure_ascii=True),
                        "canonical_trace_json": json.dumps(canonical_trace, ensure_ascii=True),
                        "created_run_id": run_id,
                        "created_at": None,
                    }
                )

            updated_at_lookup = frame.set_index("record_id")["source_updated_at"].to_dict()
            map_rows = []
            auto_matches = 0
            for row in clusters.itertuples(index=False):
                record_id = str(row.record_id)
                cluster_size = cluster_sizes[int(row.cluster_key)]
                decision = "auto_match" if cluster_size > 1 else "new_entity"
                if decision == "auto_match":
                    auto_matches += 1
                map_rows.append(
                    {
                        "record_id": record_id,
                        "entity_id": int(row.entity_id),
                        "source_updated_at": _as_datetime(updated_at_lookup.get(record_id)),
                        "best_match_probability": float(row.best_match_probability),
                        "decision": decision,
                        "linked_run_id": run_id,
                    }
                )

            review_pairs = (
                result.review_pairs.sort_values("match_probability", ascending=False)
                .head(self.cfg.run.review_candidate_limit)
                .copy()
            )
            review_rows = [
                {
                    "run_id": run_id,
                    "left_record_id": str(row.left_id),
                    "right_record_id": str(row.right_id),
                    "candidate_entity_id": None,
                    "match_probability": float(row.match_probability),
                }
                for row in review_pairs.itertuples(index=False)
            ]

            self.dedupe_store.upsert_unique_inmates(entity_rows)
            self.dedupe_store.upsert_record_entity_map(map_rows)
            self.dedupe_store.insert_review_candidates(review_rows)

            self.dedupe_store.complete_run(
                run_id=run_id,
                status="completed",
                records_processed=len(frame),
                auto_matches=auto_matches,
                review_candidates=len(review_rows),
                new_entities=len(entity_rows),
            )
            if self._use_source_id_cursor_mode():
                self._save_record_id_watermark(str(frame["record_id"].max()))
            elif max_seen is not None:
                self._save_watermark(max_seen)

            self._persist_run_artifact(run_id, "full_clusters.parquet", clusters)
            self._persist_run_artifact(run_id, "full_review_pairs.parquet", review_pairs)
            return RunSummary(
                run_id=run_id,
                run_type="full",
                records_processed=len(frame),
                auto_matches=auto_matches,
                review_candidates=len(review_rows),
                new_entities=len(entity_rows),
                scored_pairs=result.scored_pairs,
            )
        except Exception as exc:
            self.dedupe_store.complete_run(
                run_id=run_id,
                status="failed",
                records_processed=0,
                auto_matches=0,
                review_candidates=0,
                new_entities=0,
                error_message=str(exc)[:65535],
            )
            raise

    def run_incremental(self) -> RunSummary:
        if self._use_source_id_cursor_mode():
            return self._run_incremental_source_id()
        return self._run_incremental_updated_at()

    def _run_incremental_updated_at(self) -> RunSummary:
        since_ts = self._effective_incremental_since()
        run_id = self.dedupe_store.create_run(
            run_type="incremental",
            model_version=self.cfg.run.model_version,
            threshold_auto=self.cfg.thresholds.auto_match_probability,
            threshold_review=self.cfg.thresholds.review_match_probability,
            source_table=self.cfg.source_mysql.source_table,
            source_since_ts=since_ts,
            metadata={"blocking_rules": self.cfg.blocking_rules.incremental},
        )
        try:
            frame, max_seen = self._extract_normalized_source(since_ts=since_ts)
            if frame.empty:
                self.dedupe_store.complete_run(
                    run_id=run_id,
                    status="completed",
                    records_processed=0,
                    auto_matches=0,
                    review_candidates=0,
                    new_entities=0,
                )
                return RunSummary(run_id, "incremental", 0, 0, 0, 0, 0)

            outcome = self._link_records_to_entities(frame, run_id, run_type="incremental")
            if outcome.entity_rows:
                self.dedupe_store.upsert_unique_inmates(outcome.entity_rows)
            if outcome.map_rows:
                self.dedupe_store.upsert_record_entity_map(outcome.map_rows)
            if outcome.review_rows:
                self.dedupe_store.insert_review_candidates(outcome.review_rows)

            self.dedupe_store.complete_run(
                run_id=run_id,
                status="completed",
                records_processed=len(frame),
                auto_matches=outcome.auto_matches,
                review_candidates=outcome.review_candidates,
                new_entities=outcome.new_entities,
            )
            if max_seen is not None:
                self._save_watermark(max_seen)

            self._persist_run_artifact(run_id, "incremental_auto_links.parquet", outcome.auto_links)
            self._persist_run_artifact(run_id, "incremental_review_links.parquet", outcome.review_links)
            self._persist_run_artifact(run_id, "incremental_unmatched.parquet", outcome.unmatched)

            return RunSummary(
                run_id=run_id,
                run_type="incremental",
                records_processed=len(frame),
                auto_matches=outcome.auto_matches,
                review_candidates=outcome.review_candidates,
                new_entities=outcome.new_entities,
                scored_pairs=outcome.scored_pairs,
            )
        except Exception as exc:
            self.dedupe_store.complete_run(
                run_id=run_id,
                status="failed",
                records_processed=0,
                auto_matches=0,
                review_candidates=0,
                new_entities=0,
                error_message=str(exc)[:65535],
            )
            raise

    def _run_incremental_source_id(self) -> RunSummary:
        since_record_id = self._effective_incremental_since_record_id()
        run_id = self.dedupe_store.create_run(
            run_type="incremental",
            model_version=self.cfg.run.model_version,
            threshold_auto=self.cfg.thresholds.auto_match_probability,
            threshold_review=self.cfg.thresholds.review_match_probability,
            source_table=self.cfg.source_mysql.source_table,
            source_since_ts=None,
            metadata={
                "blocking_rules": self.cfg.blocking_rules.incremental,
                "cursor_mode": "source_id",
                "source_since_record_id": since_record_id,
            },
        )
        try:
            frame, last_record_id = self._extract_normalized_source_by_record_id(since_record_id=since_record_id)
            if frame.empty:
                self.dedupe_store.complete_run(
                    run_id=run_id,
                    status="completed",
                    records_processed=0,
                    auto_matches=0,
                    review_candidates=0,
                    new_entities=0,
                )
                return RunSummary(run_id, "incremental", 0, 0, 0, 0, 0)

            outcome = self._link_records_to_entities(frame, run_id, run_type="incremental")
            if outcome.entity_rows:
                self.dedupe_store.upsert_unique_inmates(outcome.entity_rows)
            if outcome.map_rows:
                self.dedupe_store.upsert_record_entity_map(outcome.map_rows)
            if outcome.review_rows:
                self.dedupe_store.insert_review_candidates(outcome.review_rows)

            self.dedupe_store.complete_run(
                run_id=run_id,
                status="completed",
                records_processed=len(frame),
                auto_matches=outcome.auto_matches,
                review_candidates=outcome.review_candidates,
                new_entities=outcome.new_entities,
            )
            if last_record_id:
                self._save_record_id_watermark(last_record_id)

            self._persist_run_artifact(run_id, "incremental_auto_links.parquet", outcome.auto_links)
            self._persist_run_artifact(run_id, "incremental_review_links.parquet", outcome.review_links)
            self._persist_run_artifact(run_id, "incremental_unmatched.parquet", outcome.unmatched)

            return RunSummary(
                run_id=run_id,
                run_type="incremental",
                records_processed=len(frame),
                auto_matches=outcome.auto_matches,
                review_candidates=outcome.review_candidates,
                new_entities=outcome.new_entities,
                scored_pairs=outcome.scored_pairs,
            )
        except Exception as exc:
            self.dedupe_store.complete_run(
                run_id=run_id,
                status="failed",
                records_processed=0,
                auto_matches=0,
                review_candidates=0,
                new_entities=0,
                error_message=str(exc)[:65535],
            )
            raise

    def run_bootstrap(self, max_rows: int | None = None) -> RunSummary:
        if self._use_source_id_cursor_mode():
            return self._run_bootstrap_source_id(max_rows=max_rows)
        return self._run_bootstrap_updated_at(max_rows=max_rows)

    def _run_bootstrap_updated_at(self, max_rows: int | None = None) -> RunSummary:

        batch_rows = max_rows or self.cfg.run.bootstrap_batch_rows
        if batch_rows <= 0:
            raise ValueError("bootstrap max_rows must be > 0")

        state = self._load_bootstrap_state()
        if state is None:
            state = BootstrapState(
                cutoff_ts=datetime.utcnow().replace(microsecond=0),
                cursor_updated_at=None,
                cursor_record_id=None,
                completed=False,
                rows_processed_total=0,
            )

        run_id = self.dedupe_store.create_run(
            run_type="bootstrap",
            model_version=self.cfg.run.model_version,
            threshold_auto=self.cfg.thresholds.auto_match_probability,
            threshold_review=self.cfg.thresholds.review_match_probability,
            source_table=self.cfg.source_mysql.source_table,
            source_since_ts=state.cursor_updated_at,
            metadata={
                "mode": "bootstrap",
                "cutoff_ts": state.cutoff_ts.isoformat(),
                "cursor_updated_at": state.cursor_updated_at.isoformat() if state.cursor_updated_at else None,
                "cursor_record_id": state.cursor_record_id,
                "batch_rows": batch_rows,
                "already_completed": state.completed,
            },
        )
        committed = False
        try:
            frame, last_updated_at, last_record_id = self._extract_normalized_source_slice(
                cutoff_ts=state.cutoff_ts,
                cursor_updated_at=state.cursor_updated_at,
                cursor_record_id=state.cursor_record_id,
                max_rows=batch_rows,
            )

            raw_rows = int(frame.attrs.get("raw_rows", 0))
            if raw_rows == 0:
                completed_state = BootstrapState(
                    cutoff_ts=state.cutoff_ts,
                    cursor_updated_at=state.cursor_updated_at,
                    cursor_record_id=state.cursor_record_id,
                    completed=True,
                    rows_processed_total=state.rows_processed_total,
                )
                self.dedupe_store.commit_bootstrap_batch(
                    run_id=run_id,
                    source_table=self.cfg.source_mysql.source_table,
                    entity_rows=[],
                    map_rows=[],
                    review_rows=[],
                    records_processed=0,
                    auto_matches=0,
                    review_candidates=0,
                    new_entities=0,
                    cutoff_ts=completed_state.cutoff_ts,
                    cursor_updated_at=completed_state.cursor_updated_at,
                    cursor_record_id=completed_state.cursor_record_id,
                    completed=completed_state.completed,
                    rows_processed_total=completed_state.rows_processed_total,
                    watermark_candidate=state.cutoff_ts,
                )
                committed = True
                self._save_bootstrap_state(completed_state)
                self._mirror_watermark_file(state.cutoff_ts)
                return RunSummary(run_id, "bootstrap", 0, 0, 0, 0, 0)

            if frame.empty:
                completed = raw_rows < batch_rows
                new_state = BootstrapState(
                    cutoff_ts=state.cutoff_ts,
                    cursor_updated_at=last_updated_at or state.cursor_updated_at,
                    cursor_record_id=last_record_id or state.cursor_record_id,
                    completed=completed,
                    rows_processed_total=state.rows_processed_total,
                )
                self.dedupe_store.commit_bootstrap_batch(
                    run_id=run_id,
                    source_table=self.cfg.source_mysql.source_table,
                    entity_rows=[],
                    map_rows=[],
                    review_rows=[],
                    records_processed=0,
                    auto_matches=0,
                    review_candidates=0,
                    new_entities=0,
                    cutoff_ts=new_state.cutoff_ts,
                    cursor_updated_at=new_state.cursor_updated_at,
                    cursor_record_id=new_state.cursor_record_id,
                    completed=new_state.completed,
                    rows_processed_total=new_state.rows_processed_total,
                    watermark_candidate=state.cutoff_ts if completed else None,
                )
                committed = True
                self._save_bootstrap_state(new_state)
                if completed:
                    self._mirror_watermark_file(state.cutoff_ts)
                return RunSummary(run_id, "bootstrap", 0, 0, 0, 0, 0)

            outcome = self._link_records_to_entities(frame, run_id, run_type="bootstrap")

            completed = len(frame) < batch_rows
            new_state = BootstrapState(
                cutoff_ts=state.cutoff_ts,
                cursor_updated_at=last_updated_at or state.cursor_updated_at,
                cursor_record_id=last_record_id or state.cursor_record_id,
                completed=completed,
                rows_processed_total=state.rows_processed_total + len(frame),
            )
            self._persist_run_artifact(run_id, "bootstrap_auto_links.parquet", outcome.auto_links)
            self._persist_run_artifact(run_id, "bootstrap_review_links.parquet", outcome.review_links)
            self._persist_run_artifact(run_id, "bootstrap_unmatched.parquet", outcome.unmatched)
            self.dedupe_store.commit_bootstrap_batch(
                run_id=run_id,
                source_table=self.cfg.source_mysql.source_table,
                entity_rows=outcome.entity_rows,
                map_rows=outcome.map_rows,
                review_rows=outcome.review_rows,
                records_processed=len(frame),
                auto_matches=outcome.auto_matches,
                review_candidates=outcome.review_candidates,
                new_entities=outcome.new_entities,
                cutoff_ts=new_state.cutoff_ts,
                cursor_updated_at=new_state.cursor_updated_at,
                cursor_record_id=new_state.cursor_record_id,
                completed=new_state.completed,
                rows_processed_total=new_state.rows_processed_total,
                watermark_candidate=state.cutoff_ts if completed else None,
            )
            committed = True
            self._save_bootstrap_state(new_state)
            if completed:
                self._mirror_watermark_file(state.cutoff_ts)

            return RunSummary(
                run_id=run_id,
                run_type="bootstrap",
                records_processed=len(frame),
                auto_matches=outcome.auto_matches,
                review_candidates=outcome.review_candidates,
                new_entities=outcome.new_entities,
                scored_pairs=outcome.scored_pairs,
            )
        except Exception as exc:
            if not committed:
                self.dedupe_store.complete_run(
                    run_id=run_id,
                    status="failed",
                    records_processed=0,
                    auto_matches=0,
                    review_candidates=0,
                    new_entities=0,
                    error_message=str(exc)[:65535],
                )
            raise

    def _run_bootstrap_source_id(self, max_rows: int | None = None) -> RunSummary:
        batch_rows = max_rows or self.cfg.run.bootstrap_batch_rows
        if batch_rows <= 0:
            raise ValueError("bootstrap max_rows must be > 0")

        state = self._load_bootstrap_state()
        if state is None:
            state = BootstrapState(
                cutoff_ts=datetime.utcnow().replace(microsecond=0),
                cursor_updated_at=None,
                cursor_record_id=None,
                completed=False,
                rows_processed_total=0,
            )

        run_id = self.dedupe_store.create_run(
            run_type="bootstrap",
            model_version=self.cfg.run.model_version,
            threshold_auto=self.cfg.thresholds.auto_match_probability,
            threshold_review=self.cfg.thresholds.review_match_probability,
            source_table=self.cfg.source_mysql.source_table,
            source_since_ts=None,
            metadata={
                "mode": "bootstrap",
                "cursor_mode": "source_id",
                "cursor_record_id": state.cursor_record_id,
                "batch_rows": batch_rows,
                "already_completed": state.completed,
            },
        )
        committed = False
        try:
            frame, last_record_id = self._extract_normalized_source_slice_by_record_id(
                cursor_record_id=state.cursor_record_id,
                max_rows=batch_rows,
            )

            raw_rows = int(frame.attrs.get("raw_rows", 0))
            if raw_rows == 0:
                completed_state = BootstrapState(
                    cutoff_ts=state.cutoff_ts,
                    cursor_updated_at=None,
                    cursor_record_id=state.cursor_record_id,
                    completed=True,
                    rows_processed_total=state.rows_processed_total,
                )
                self.dedupe_store.commit_bootstrap_batch(
                    run_id=run_id,
                    source_table=self.cfg.source_mysql.source_table,
                    entity_rows=[],
                    map_rows=[],
                    review_rows=[],
                    records_processed=0,
                    auto_matches=0,
                    review_candidates=0,
                    new_entities=0,
                    cutoff_ts=completed_state.cutoff_ts,
                    cursor_updated_at=None,
                    cursor_record_id=completed_state.cursor_record_id,
                    completed=completed_state.completed,
                    rows_processed_total=completed_state.rows_processed_total,
                    watermark_candidate=None,
                    watermark_record_id_candidate=completed_state.cursor_record_id,
                )
                committed = True
                self._save_bootstrap_state(completed_state)
                if completed_state.cursor_record_id:
                    self._save_record_id_watermark(completed_state.cursor_record_id)
                return RunSummary(run_id, "bootstrap", 0, 0, 0, 0, 0)

            if frame.empty:
                completed = raw_rows < batch_rows
                new_state = BootstrapState(
                    cutoff_ts=state.cutoff_ts,
                    cursor_updated_at=None,
                    cursor_record_id=last_record_id or state.cursor_record_id,
                    completed=completed,
                    rows_processed_total=state.rows_processed_total,
                )
                self.dedupe_store.commit_bootstrap_batch(
                    run_id=run_id,
                    source_table=self.cfg.source_mysql.source_table,
                    entity_rows=[],
                    map_rows=[],
                    review_rows=[],
                    records_processed=0,
                    auto_matches=0,
                    review_candidates=0,
                    new_entities=0,
                    cutoff_ts=new_state.cutoff_ts,
                    cursor_updated_at=None,
                    cursor_record_id=new_state.cursor_record_id,
                    completed=new_state.completed,
                    rows_processed_total=new_state.rows_processed_total,
                    watermark_candidate=None,
                    watermark_record_id_candidate=new_state.cursor_record_id if completed else None,
                )
                committed = True
                self._save_bootstrap_state(new_state)
                if completed and new_state.cursor_record_id:
                    self._save_record_id_watermark(new_state.cursor_record_id)
                return RunSummary(run_id, "bootstrap", 0, 0, 0, 0, 0)

            outcome = self._link_records_to_entities(frame, run_id, run_type="bootstrap")

            completed = len(frame) < batch_rows
            new_state = BootstrapState(
                cutoff_ts=state.cutoff_ts,
                cursor_updated_at=None,
                cursor_record_id=last_record_id or state.cursor_record_id,
                completed=completed,
                rows_processed_total=state.rows_processed_total + len(frame),
            )
            self._persist_run_artifact(run_id, "bootstrap_auto_links.parquet", outcome.auto_links)
            self._persist_run_artifact(run_id, "bootstrap_review_links.parquet", outcome.review_links)
            self._persist_run_artifact(run_id, "bootstrap_unmatched.parquet", outcome.unmatched)
            self.dedupe_store.commit_bootstrap_batch(
                run_id=run_id,
                source_table=self.cfg.source_mysql.source_table,
                entity_rows=outcome.entity_rows,
                map_rows=outcome.map_rows,
                review_rows=outcome.review_rows,
                records_processed=len(frame),
                auto_matches=outcome.auto_matches,
                review_candidates=outcome.review_candidates,
                new_entities=outcome.new_entities,
                cutoff_ts=new_state.cutoff_ts,
                cursor_updated_at=None,
                cursor_record_id=new_state.cursor_record_id,
                completed=new_state.completed,
                rows_processed_total=new_state.rows_processed_total,
                watermark_candidate=None,
                watermark_record_id_candidate=new_state.cursor_record_id if completed else None,
            )
            committed = True
            self._save_bootstrap_state(new_state)
            if completed and new_state.cursor_record_id:
                self._save_record_id_watermark(new_state.cursor_record_id)

            return RunSummary(
                run_id=run_id,
                run_type="bootstrap",
                records_processed=len(frame),
                auto_matches=outcome.auto_matches,
                review_candidates=outcome.review_candidates,
                new_entities=outcome.new_entities,
                scored_pairs=outcome.scored_pairs,
            )
        except Exception as exc:
            if not committed:
                self.dedupe_store.complete_run(
                    run_id=run_id,
                    status="failed",
                    records_processed=0,
                    auto_matches=0,
                    review_candidates=0,
                    new_entities=0,
                    error_message=str(exc)[:65535],
                )
            raise

    def _link_records_to_entities(self, frame: pd.DataFrame, run_id: int, run_type: str) -> LinkBatchOutcome:
        entity_raw = self.dedupe_store.fetch_entity_index()
        entity_index = normalize_entity_frame(entity_raw) if not entity_raw.empty else entity_raw
        result = run_incremental_link(frame, entity_index, self.cfg)

        source_lookup = frame.set_index("record_id")["source_updated_at"].to_dict()
        canonical_lookup = frame.set_index("record_id").to_dict(orient="index")
        existing_entity_lookup = (
            {int(entity_id): row for entity_id, row in entity_raw.set_index("entity_id").to_dict(orient="index").items()}
            if not entity_raw.empty
            else {}
        )

        review_record_ids = set(result.review_links["record_id"].astype(str).tolist())
        unmatched_ids = set(result.unmatched["record_id"].astype(str).tolist())
        unresolved_ids = sorted(review_record_ids | unmatched_ids)

        cluster_members: dict[int, list[str]] = {}
        record_to_cluster: dict[str, int] = {}
        intra_best_probability: dict[str, float] = {}
        intra_scored_pairs = 0
        if unresolved_ids:
            if run_type == "bootstrap" and len(unresolved_ids) > 1:
                unresolved_frame = frame[frame["record_id"].isin(unresolved_ids)].copy()
                (
                    cluster_members,
                    record_to_cluster,
                    intra_best_probability,
                    intra_scored_pairs,
                ) = self._cluster_bootstrap_residual_records(unresolved_frame)
            else:
                for idx, record_id in enumerate(unresolved_ids, start=1):
                    cluster_members[idx] = [record_id]
                    record_to_cluster[record_id] = idx
                    intra_best_probability[record_id] = 0.0

        cluster_keys = sorted(cluster_members.keys())
        provisional_entity_ids = self.dedupe_store.reserve_entity_ids(len(cluster_keys))
        cluster_to_entity_id = dict(zip(cluster_keys, provisional_entity_ids))
        provisional_map: dict[str, int] = {}
        cluster_size_by_record: dict[str, int] = {}
        cluster_anchor_by_key: dict[int, str] = {}

        new_entity_rows: list[dict[str, Any]] = []
        for cluster_key in cluster_keys:
            members = cluster_members[cluster_key]
            member_rows = [_source_row_with_record_id(canonical_lookup[record_id], record_id) for record_id in members]
            canonical_values, canonical_trace, anchor_row = _build_survivor_from_members(member_rows)
            anchor_record_id = str(canonical_values["canonical_record_id"])
            cluster_anchor_by_key[cluster_key] = anchor_record_id
            for record_id in members:
                provisional_map[record_id] = int(cluster_to_entity_id[cluster_key])
                cluster_size_by_record[record_id] = len(members)
            new_entity_rows.append(
                {
                    "entity_id": int(cluster_to_entity_id[cluster_key]),
                    "cif_number": _resolve_cif_number(int(cluster_to_entity_id[cluster_key]), None),
                    **canonical_values,
                    "canonical_payload_json": json.dumps(_payload_from_source_row(anchor_row), ensure_ascii=True),
                    "canonical_trace_json": json.dumps(canonical_trace, ensure_ascii=True),
                    "created_run_id": run_id,
                    "created_at": None,
                }
            )

        updated_entity_rows: list[dict[str, Any]] = []
        if existing_entity_lookup and not result.auto_links.empty:
            grouped_auto = result.auto_links.groupby("entity_id")["record_id"].apply(list).to_dict()
            for entity_id_raw, record_ids in grouped_auto.items():
                entity_id = int(entity_id_raw)
                existing = existing_entity_lookup.get(entity_id)
                if existing is None:
                    continue
                member_rows = []
                for record_id in record_ids:
                    record_id_str = str(record_id)
                    source_row = canonical_lookup.get(record_id_str)
                    if source_row is None:
                        continue
                    member_rows.append(_source_row_with_record_id(source_row, record_id_str))
                if not member_rows:
                    continue

                merged_values, merged_trace, changed = _merge_existing_canonical(existing, member_rows)
                if not changed:
                    continue

                updated_entity_rows.append(
                    {
                        "entity_id": entity_id,
                        "cif_number": _resolve_cif_number(entity_id, existing.get("cif_number")),
                        **merged_values,
                        "canonical_payload_json": existing.get("canonical_payload_json"),
                        "canonical_trace_json": json.dumps(merged_trace, ensure_ascii=True),
                        "created_run_id": int(existing.get("created_run_id") or run_id),
                        "created_at": _as_datetime(existing.get("created_at")),
                    }
                )

        map_rows = []
        intra_batch_auto_link_rows: list[dict[str, Any]] = []
        unmatched_output_rows: list[dict[str, Any]] = []
        for row in result.auto_links.itertuples(index=False):
            record_id = str(row.record_id)
            map_rows.append(
                {
                    "record_id": record_id,
                    "entity_id": int(row.entity_id),
                    "source_updated_at": _as_datetime(source_lookup.get(record_id)),
                    "best_match_probability": float(row.match_probability),
                    "decision": "auto_match",
                    "linked_run_id": run_id,
                }
            )
        for row in result.review_links.itertuples(index=False):
            record_id = str(row.record_id)
            provisional_entity_id = provisional_map.get(record_id)
            if provisional_entity_id is None:
                continue
            map_rows.append(
                {
                    "record_id": record_id,
                    "entity_id": int(provisional_entity_id),
                    "source_updated_at": _as_datetime(source_lookup.get(record_id)),
                    "best_match_probability": float(row.match_probability),
                    "decision": "needs_review",
                    "linked_run_id": run_id,
                }
            )
        for row in result.unmatched.itertuples(index=False):
            record_id = str(row.record_id)
            provisional_entity_id = provisional_map.get(record_id)
            if provisional_entity_id is None:
                continue
            cluster_key = record_to_cluster.get(record_id)
            cluster_size = cluster_size_by_record.get(record_id, 1)
            anchor_record_id = cluster_anchor_by_key.get(cluster_key, record_id) if cluster_key is not None else record_id
            decision = "new_entity"
            if run_type == "bootstrap" and cluster_size > 1 and record_id != anchor_record_id:
                decision = "auto_match"
                intra_batch_auto_link_rows.append(
                    {
                        "record_id": record_id,
                        "entity_id": int(provisional_entity_id),
                        "match_probability": float(intra_best_probability.get(record_id, 0.0)),
                    }
                )
            else:
                unmatched_output_rows.append({"record_id": record_id})
            map_rows.append(
                {
                    "record_id": record_id,
                    "entity_id": int(provisional_entity_id),
                    "source_updated_at": _as_datetime(source_lookup.get(record_id)),
                    "best_match_probability": float(intra_best_probability.get(record_id, 0.0)),
                    "decision": decision,
                    "linked_run_id": run_id,
                }
            )

        review_rows = [
            {
                "run_id": run_id,
                "left_record_id": str(row.record_id),
                "right_record_id": None,
                "candidate_entity_id": int(row.candidate_entity_id),
                "match_probability": float(row.match_probability),
            }
            for row in result.review_links.itertuples(index=False)
        ]

        auto_links = result.auto_links[["record_id", "entity_id", "match_probability"]].copy()
        if intra_batch_auto_link_rows:
            auto_links = pd.concat([auto_links, pd.DataFrame(intra_batch_auto_link_rows)], axis=0, ignore_index=True)
        unmatched_output = pd.DataFrame(unmatched_output_rows, columns=["record_id"])
        entity_rows = new_entity_rows + updated_entity_rows

        return LinkBatchOutcome(
            auto_links=auto_links,
            review_links=result.review_links,
            unmatched=unmatched_output,
            auto_matches=len(auto_links),
            review_candidates=len(review_rows),
            new_entities=len(new_entity_rows),
            scored_pairs=result.scored_pairs + intra_scored_pairs,
            entity_rows=entity_rows,
            map_rows=map_rows,
            review_rows=review_rows,
        )

    def _cluster_bootstrap_residual_records(
        self,
        residual_frame: pd.DataFrame,
    ) -> tuple[dict[int, list[str]], dict[str, int], dict[str, float], int]:
        if residual_frame.empty:
            return {}, {}, {}, 0

        result = run_full_dedupe(residual_frame, self.cfg)
        clusters = result.clusters.copy()
        clusters["record_id"] = clusters["record_id"].astype(str)

        cluster_members: dict[int, list[str]] = {}
        record_to_cluster: dict[str, int] = {}
        best_probability: dict[str, float] = {}
        for row in clusters.itertuples(index=False):
            cluster_key = int(row.cluster_key)
            record_id = str(row.record_id)
            cluster_members.setdefault(cluster_key, []).append(record_id)
            record_to_cluster[record_id] = cluster_key
            best_probability[record_id] = float(row.best_match_probability)
        for members in cluster_members.values():
            members.sort()

        return cluster_members, record_to_cluster, best_probability, result.scored_pairs

    def _extract_normalized_source(self, since_ts: datetime | None) -> tuple[pd.DataFrame, datetime | None]:
        projection = self._projection_columns()
        chunks: list[pd.DataFrame] = []
        max_seen: datetime | None = since_ts

        for raw in self.source_store.stream_source_rows(
            projection_columns=projection,
            batch_size=self.cfg.run.batch_size,
            since_ts=since_ts,
        ):
            cleaned = normalize_source_frame(raw, self.cfg)
            if cleaned.empty:
                continue
            chunks.append(cleaned)
            if self.cfg.source_mysql.source_updated_at_column:
                observed = cleaned["source_updated_at"].max()
                if pd.notna(observed):
                    candidate = _as_datetime(observed)
                    if candidate is not None and (max_seen is None or candidate > max_seen):
                        max_seen = candidate

        if not chunks:
            return pd.DataFrame(), max_seen
        frame = pd.concat(chunks, axis=0, ignore_index=True)
        frame = frame.drop_duplicates(subset=["record_id"], keep="last")
        return frame, max_seen

    def _extract_normalized_source_by_record_id(self, since_record_id: str | None) -> tuple[pd.DataFrame, str | None]:
        projection = self._projection_columns()
        chunks: list[pd.DataFrame] = []
        last_record_id: str | None = since_record_id
        id_col = self.cfg.source_mysql.source_id_column

        for raw in self.source_store.stream_source_rows(
            projection_columns=projection,
            batch_size=self.cfg.run.batch_size,
            since_record_id=since_record_id,
        ):
            cleaned = normalize_source_frame(raw, self.cfg)
            if cleaned.empty:
                continue
            chunks.append(cleaned)
            if id_col in raw.columns and len(raw) > 0:
                last_record_id = str(raw.iloc[-1][id_col])

        if not chunks:
            return pd.DataFrame(), last_record_id
        frame = pd.concat(chunks, axis=0, ignore_index=True)
        frame = frame.drop_duplicates(subset=["record_id"], keep="last")
        if not frame.empty:
            last_record_id = str(frame["record_id"].max())
        return frame, last_record_id

    def _extract_normalized_source_slice(
        self,
        cutoff_ts: datetime,
        cursor_updated_at: datetime | None,
        cursor_record_id: str | None,
        max_rows: int,
    ) -> tuple[pd.DataFrame, datetime | None, str | None]:
        projection = self._projection_columns()
        raw = self.source_store.fetch_source_slice(
            projection_columns=projection,
            max_rows=max_rows,
            cutoff_ts=cutoff_ts,
            cursor_updated_at=cursor_updated_at,
            cursor_record_id=cursor_record_id,
        )
        if raw.empty:
            empty = pd.DataFrame()
            empty.attrs["raw_rows"] = 0
            return empty, None, None

        cleaned = normalize_source_frame(raw, self.cfg)
        cleaned = cleaned.drop_duplicates(subset=["record_id"], keep="last")
        cleaned.attrs["raw_rows"] = len(raw)

        updated_col = self.cfg.source_mysql.source_updated_at_column
        id_col = self.cfg.source_mysql.source_id_column
        last_updated = _as_datetime(raw.iloc[-1][updated_col]) if updated_col else None
        last_record_id = str(raw.iloc[-1][id_col]) if id_col in raw.columns else None
        return cleaned, last_updated, last_record_id

    def _extract_normalized_source_slice_by_record_id(
        self,
        cursor_record_id: str | None,
        max_rows: int,
    ) -> tuple[pd.DataFrame, str | None]:
        projection = self._projection_columns()
        raw = self.source_store.fetch_source_slice_by_id(
            projection_columns=projection,
            max_rows=max_rows,
            cursor_record_id=cursor_record_id,
        )
        if raw.empty:
            empty = pd.DataFrame()
            empty.attrs["raw_rows"] = 0
            return empty, None

        cleaned = normalize_source_frame(raw, self.cfg)
        cleaned = cleaned.drop_duplicates(subset=["record_id"], keep="last")
        cleaned.attrs["raw_rows"] = len(raw)

        id_col = self.cfg.source_mysql.source_id_column
        last_record_id = str(raw.iloc[-1][id_col]) if id_col in raw.columns else None
        return cleaned, last_record_id

    def _projection_columns(self) -> list[str]:
        c = self.cfg.columns
        cols = [
            self.cfg.source_mysql.source_id_column,
            c.id_upt,
            c.nik,
            c.nomor_induk_nasional,
            c.nama_lengkap,
            c.nama_alias1,
            c.nama_alias2,
            c.nama_alias3,
            c.nama_kecil1,
            c.nama_kecil2,
            c.nama_kecil3,
            c.tanggal_lahir,
            c.id_jenis_kelamin,
            c.alamat,
            c.alamat_alternatif,
            c.kodepos,
            c.telepon,
            c.telephone_keluarga,
            c.nm_ayah,
            c.nm_ibu,
            c.nm_istri_suami,
        ]
        if self.cfg.source_mysql.source_updated_at_column:
            cols.append(self.cfg.source_mysql.source_updated_at_column)
        cols.extend(c.passthrough)
        # Preserve order while removing duplicates.
        return list(dict.fromkeys(cols))

    def _load_watermark(self) -> datetime | None:
        db_value = self.dedupe_store.load_watermark(self.cfg.source_mysql.source_table)
        if db_value is not None:
            return db_value
        path = self.cfg.run.watermark_file
        if not path.exists():
            return None
        payload = self._load_watermark_payload_file()
        value = payload.get("source_updated_at")
        if not value:
            return None
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        return _as_datetime(parsed)

    def _save_watermark(self, source_updated_at: datetime) -> None:
        self.dedupe_store.save_watermark(
            source_table=self.cfg.source_mysql.source_table,
            source_updated_at=source_updated_at,
        )
        self._mirror_watermark_file(source_updated_at)

    def _mirror_watermark_file(self, source_updated_at: datetime) -> None:
        payload = self._load_watermark_payload_file()
        payload["source_updated_at"] = source_updated_at.isoformat()
        self.cfg.run.watermark_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _set_watermark_if_newer(self, candidate: datetime) -> None:
        current = self._load_watermark()
        if current is None or candidate > current:
            self._save_watermark(candidate)

    def _load_record_id_watermark(self) -> str | None:
        db_value = self.dedupe_store.load_record_id_watermark(self.cfg.source_mysql.source_table)
        if db_value:
            return db_value
        payload = self._load_watermark_payload_file()
        value = payload.get("source_record_id")
        if value is None:
            return None
        token = str(value).strip()
        return token or None

    def _save_record_id_watermark(self, source_record_id: str) -> None:
        token = str(source_record_id).strip()
        if not token:
            return
        self.dedupe_store.save_record_id_watermark(
            source_table=self.cfg.source_mysql.source_table,
            source_record_id=token,
        )
        self._mirror_record_id_watermark_file(token)

    def _mirror_record_id_watermark_file(self, source_record_id: str) -> None:
        payload = self._load_watermark_payload_file()
        payload["source_record_id"] = source_record_id
        self.cfg.run.watermark_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _load_watermark_payload_file(self) -> dict[str, Any]:
        path = self.cfg.run.watermark_file
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        if isinstance(payload, dict):
            return payload
        return {}

    def _load_bootstrap_state(self) -> BootstrapState | None:
        db_state = self.dedupe_store.load_bootstrap_state(self.cfg.source_mysql.source_table)
        if db_state is not None:
            return BootstrapState(
                cutoff_ts=db_state["cutoff_ts"],
                cursor_updated_at=db_state["cursor_updated_at"],
                cursor_record_id=db_state["cursor_record_id"],
                completed=db_state["completed"],
                rows_processed_total=db_state["rows_processed_total"],
            )

        path = self.cfg.run.bootstrap_state_file
        if not path.exists():
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
        cutoff = _as_datetime(raw.get("cutoff_ts"))
        if cutoff is None:
            return None
        return BootstrapState(
            cutoff_ts=cutoff,
            cursor_updated_at=_as_datetime(raw.get("cursor_updated_at")),
            cursor_record_id=raw.get("cursor_record_id"),
            completed=bool(raw.get("completed", False)),
            rows_processed_total=int(raw.get("rows_processed_total", 0)),
        )

    def _save_bootstrap_state(self, state: BootstrapState) -> None:
        payload = {
            "cutoff_ts": state.cutoff_ts.isoformat(),
            "cursor_updated_at": state.cursor_updated_at.isoformat() if state.cursor_updated_at else None,
            "cursor_record_id": state.cursor_record_id,
            "completed": state.completed,
            "rows_processed_total": state.rows_processed_total,
        }
        # Legacy mirror for observability/backward compatibility; transaction-safe state is in DuckDB.
        self.cfg.run.bootstrap_state_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _effective_incremental_since(self) -> datetime | None:
        since_ts = self._load_watermark()
        bootstrap_state = self._load_bootstrap_state()
        if bootstrap_state and not bootstrap_state.completed:
            if since_ts is None or bootstrap_state.cutoff_ts > since_ts:
                return bootstrap_state.cutoff_ts
        return since_ts

    def _effective_incremental_since_record_id(self) -> str | None:
        since_id = self._load_record_id_watermark()
        bootstrap_state = self._load_bootstrap_state()
        if bootstrap_state and not bootstrap_state.completed and bootstrap_state.cursor_record_id:
            if since_id is None or bootstrap_state.cursor_record_id > since_id:
                return bootstrap_state.cursor_record_id
        return since_id

    def _persist_run_artifact(self, run_id: int, filename: str, frame: pd.DataFrame) -> None:
        run_dir = self.cfg.run.work_dir / f"run_{run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(run_dir / filename, index=False)


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


def _payload_from_source_row(row: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in row.items():
        if value is None:
            payload[key] = None
            continue
        try:
            if pd.isna(value):
                payload[key] = None
                continue
        except TypeError:
            pass
        if isinstance(value, pd.Timestamp):
            payload[key] = value.isoformat()
        elif hasattr(value, "isoformat") and callable(value.isoformat):
            payload[key] = value.isoformat()
        elif hasattr(value, "item") and callable(value.item):
            # Convert numpy/pandas scalar types to native Python types for JSON.
            item_value = value.item()
            payload[key] = None if pd.isna(item_value) else item_value
        else:
            payload[key] = value
    return payload


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _default_cif_number(entity_id: int) -> str:
    return f"{CIF_PREFIX}{int(entity_id):0{CIF_DIGITS}d}"


def _resolve_cif_number(entity_id: int, existing_value: Any) -> str:
    if _is_missing_value(existing_value):
        return _default_cif_number(entity_id)
    return str(existing_value).strip()


def _source_row_with_record_id(source_row: dict[str, Any], record_id: str) -> dict[str, Any]:
    row = dict(source_row)
    row["record_id"] = str(record_id)
    row["source_updated_at"] = _as_datetime(row.get("source_updated_at"))
    return row


def _trace_entry(
    *,
    source_field: str,
    source_record_id: str | None,
    source_updated_at: datetime | None,
    selection: str,
) -> dict[str, Any]:
    return {
        "source_field": source_field,
        "source_record_id": source_record_id,
        "source_updated_at": source_updated_at.isoformat() if source_updated_at else None,
        "selection": selection,
    }


def _build_survivor_from_members(
    member_rows: list[dict[str, Any]],
    anchor_record_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if not member_rows:
        raise ValueError("member_rows cannot be empty")

    rows_by_record_id = {str(row["record_id"]): row for row in member_rows}
    if not rows_by_record_id:
        raise ValueError("member_rows must include record_id")

    if anchor_record_id is None or anchor_record_id not in rows_by_record_id:
        anchor_record_id = sorted(rows_by_record_id.keys())[0]

    fallback_ids = sorted([rid for rid in rows_by_record_id.keys() if rid != anchor_record_id])
    fallback_ids.sort(
        key=lambda rid: _as_datetime(rows_by_record_id[rid].get("source_updated_at")) or datetime.min,
        reverse=True,
    )
    ordered_ids = [anchor_record_id] + fallback_ids

    canonical_values: dict[str, Any] = {"canonical_record_id": anchor_record_id}
    canonical_trace: dict[str, Any] = {
        "__strategy": "anchor_then_latest_non_null",
        "__anchor_record_id": anchor_record_id,
        "canonical_record_id": _trace_entry(
            source_field="record_id",
            source_record_id=anchor_record_id,
            source_updated_at=_as_datetime(rows_by_record_id[anchor_record_id].get("source_updated_at")),
            selection="anchor",
        ),
    }

    for canonical_field, source_field in CANONICAL_FIELD_MAP:
        selected_id: str | None = None
        selected_value: Any = None
        selected_updated_at: datetime | None = None
        selected_from_anchor = False

        for candidate_id in ordered_ids:
            candidate_row = rows_by_record_id[candidate_id]
            candidate_value = candidate_row.get(source_field)
            if _is_missing_value(candidate_value):
                continue
            selected_id = candidate_id
            selected_value = candidate_value
            selected_updated_at = _as_datetime(candidate_row.get("source_updated_at"))
            selected_from_anchor = candidate_id == anchor_record_id
            break

        if selected_id is None:
            canonical_values[canonical_field] = None
            canonical_trace[canonical_field] = _trace_entry(
                source_field=source_field,
                source_record_id=None,
                source_updated_at=None,
                selection="missing_all_members",
            )
            continue

        canonical_values[canonical_field] = selected_value
        canonical_trace[canonical_field] = _trace_entry(
            source_field=source_field,
            source_record_id=selected_id,
            source_updated_at=selected_updated_at,
            selection="anchor" if selected_from_anchor else "latest_non_null_fallback",
        )

    return canonical_values, canonical_trace, rows_by_record_id[anchor_record_id]


def _load_trace_payload(raw_trace: Any, anchor_record_id: str) -> dict[str, Any]:
    trace_payload: dict[str, Any]
    if isinstance(raw_trace, dict):
        trace_payload = raw_trace
    elif isinstance(raw_trace, str):
        try:
            parsed = json.loads(raw_trace)
            trace_payload = parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            trace_payload = {}
    else:
        trace_payload = {}

    trace_payload["__strategy"] = trace_payload.get("__strategy", "anchor_then_latest_non_null")
    trace_payload["__anchor_record_id"] = anchor_record_id
    if "canonical_record_id" not in trace_payload:
        trace_payload["canonical_record_id"] = _trace_entry(
            source_field="record_id",
            source_record_id=anchor_record_id,
            source_updated_at=None,
            selection="anchor",
        )
    return trace_payload


def _merge_existing_canonical(
    existing_entity: dict[str, Any],
    new_member_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    if not new_member_rows:
        return {"canonical_record_id": str(existing_entity["canonical_record_id"])}, {}, False

    anchor_record_id = str(existing_entity.get("canonical_record_id") or "").strip()
    if not anchor_record_id:
        anchor_record_id = sorted(str(row["record_id"]) for row in new_member_rows)[0]
    trace_payload = _load_trace_payload(existing_entity.get("canonical_trace_json"), anchor_record_id)

    candidate_rows = {str(row["record_id"]): row for row in new_member_rows}
    candidate_ids = sorted(candidate_rows.keys())
    candidate_ids.sort(
        key=lambda rid: _as_datetime(candidate_rows[rid].get("source_updated_at")) or datetime.min,
        reverse=True,
    )

    merged_values: dict[str, Any] = {"canonical_record_id": anchor_record_id}
    changed = False
    for canonical_field, source_field in CANONICAL_FIELD_MAP:
        existing_value = existing_entity.get(canonical_field)
        if not _is_missing_value(existing_value):
            merged_values[canonical_field] = existing_value
            if canonical_field not in trace_payload:
                trace_payload[canonical_field] = _trace_entry(
                    source_field=source_field,
                    source_record_id=anchor_record_id,
                    source_updated_at=None,
                    selection="preexisting_value",
                )
            continue

        selected_id: str | None = None
        selected_value: Any = None
        selected_updated_at: datetime | None = None
        for candidate_id in candidate_ids:
            candidate_row = candidate_rows[candidate_id]
            candidate_value = candidate_row.get(source_field)
            if _is_missing_value(candidate_value):
                continue
            selected_id = candidate_id
            selected_value = candidate_value
            selected_updated_at = _as_datetime(candidate_row.get("source_updated_at"))
            break

        if selected_id is None:
            merged_values[canonical_field] = existing_value
            if canonical_field not in trace_payload:
                trace_payload[canonical_field] = _trace_entry(
                    source_field=source_field,
                    source_record_id=None,
                    source_updated_at=None,
                    selection="missing_all_members",
                )
            continue

        merged_values[canonical_field] = selected_value
        trace_payload[canonical_field] = _trace_entry(
            source_field=source_field,
            source_record_id=selected_id,
            source_updated_at=selected_updated_at,
            selection="filled_from_new_member",
        )
        changed = True

    return merged_values, trace_payload, changed
