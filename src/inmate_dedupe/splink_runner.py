from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from inmate_dedupe.config import AppConfig

try:
    from splink import Linker as SplinkLinker
    from splink.internals.duckdb.database_api import DuckDBAPI
except Exception:
    SplinkLinker = None
    DuckDBAPI = None
    SPLINK_V4 = False
else:
    SPLINK_V4 = True

try:
    from splink.duckdb.linker import DuckDBLinker
except Exception as exc:  # pragma: no cover - import guard for local environments
    DuckDBLinker = None
    IMPORT_ERROR_V3 = exc
else:  # pragma: no cover - import guard for local environments
    IMPORT_ERROR_V3 = None

IMPORT_ERROR = IMPORT_ERROR_V3


MODEL_COLUMNS = [
    "id_upt",
    "nik",
    "nomor_induk_nasional",
    "nama_lengkap",
    "alias_names",
    "nama_kecil",
    "tanggal_lahir",
    "id_jenis_kelamin",
    "alamat_combined",
    "kodepos",
    "telepon_any",
    "nm_ayah",
    "nm_ibu",
    "nm_istri_suami",
    "nama_prefix",
    "nama_lengkap_soundex",
    "dob_year",
]


@dataclass
class FullRunResult:
    clusters: pd.DataFrame
    review_pairs: pd.DataFrame
    scored_pairs: int


@dataclass
class IncrementalRunResult:
    auto_links: pd.DataFrame
    review_links: pd.DataFrame
    unmatched: pd.DataFrame
    scored_pairs: int


def _settings_dict(
    cfg: AppConfig,
    link_type: str,
    blocking_rules: list[str],
    unique_id_column_name: str,
    source_dataset_column_name: str | None = None,
) -> dict[str, Any]:
    comparisons = [
        {
            "output_column_name": "nik",
            "comparison_levels": [
                {"sql_condition": "nik_l IS NULL OR nik_r IS NULL", "is_null_level": True, "label_for_charts": "Null"},
                {"sql_condition": "nik_l = nik_r", "label_for_charts": "Exact"},
                {"sql_condition": "ELSE", "label_for_charts": "No match"},
            ],
        },
        {
            "output_column_name": "nomor_induk_nasional",
            "comparison_levels": [
                {
                    "sql_condition": "nomor_induk_nasional_l IS NULL OR nomor_induk_nasional_r IS NULL",
                    "is_null_level": True,
                    "label_for_charts": "Null",
                },
                {"sql_condition": "nomor_induk_nasional_l = nomor_induk_nasional_r", "label_for_charts": "Exact"},
                {"sql_condition": "ELSE", "label_for_charts": "No match"},
            ],
        },
        {
            "output_column_name": "nama_lengkap",
            "comparison_levels": [
                {
                    "sql_condition": "nama_lengkap_l IS NULL OR nama_lengkap_r IS NULL",
                    "is_null_level": True,
                    "label_for_charts": "Null",
                },
                {"sql_condition": "nama_lengkap_l = nama_lengkap_r", "label_for_charts": "Exact"},
                {"sql_condition": "nama_lengkap_soundex_l = nama_lengkap_soundex_r", "label_for_charts": "Phonetic"},
                {
                    "sql_condition": "levenshtein(CAST(nama_lengkap_l AS VARCHAR), CAST(nama_lengkap_r AS VARCHAR)) <= 2",
                    "label_for_charts": "Levenshtein <= 2",
                },
                {"sql_condition": "ELSE", "label_for_charts": "No match"},
            ],
        },
        {
            "output_column_name": "alias_names",
            "comparison_levels": [
                {
                    "sql_condition": "alias_names_l IS NULL OR alias_names_r IS NULL",
                    "is_null_level": True,
                    "label_for_charts": "Null",
                },
                {"sql_condition": "alias_names_l = alias_names_r", "label_for_charts": "Exact"},
                {
                    "sql_condition": "levenshtein(CAST(alias_names_l AS VARCHAR), CAST(alias_names_r AS VARCHAR)) <= 3",
                    "label_for_charts": "Levenshtein <= 3",
                },
                {"sql_condition": "ELSE", "label_for_charts": "No match"},
            ],
        },
        {
            "output_column_name": "tanggal_lahir",
            "comparison_levels": [
                {
                    "sql_condition": "tanggal_lahir_l IS NULL OR tanggal_lahir_r IS NULL",
                    "is_null_level": True,
                    "label_for_charts": "Null",
                },
                {"sql_condition": "tanggal_lahir_l = tanggal_lahir_r", "label_for_charts": "Exact"},
                {
                    "sql_condition": "abs(date_diff('day', tanggal_lahir_l, tanggal_lahir_r)) <= 31",
                    "label_for_charts": "Within 31 days",
                },
                {"sql_condition": "ELSE", "label_for_charts": "No match"},
            ],
        },
        {
            "output_column_name": "id_jenis_kelamin",
            "comparison_levels": [
                {
                    "sql_condition": "id_jenis_kelamin_l IS NULL OR id_jenis_kelamin_r IS NULL",
                    "is_null_level": True,
                    "label_for_charts": "Null",
                },
                {"sql_condition": "id_jenis_kelamin_l = id_jenis_kelamin_r", "label_for_charts": "Exact"},
                {"sql_condition": "ELSE", "label_for_charts": "No match"},
            ],
        },
        {
            "output_column_name": "telepon_any",
            "comparison_levels": [
                {
                    "sql_condition": "telepon_any_l IS NULL OR telepon_any_r IS NULL",
                    "is_null_level": True,
                    "label_for_charts": "Null",
                },
                {"sql_condition": "telepon_any_l = telepon_any_r", "label_for_charts": "Exact"},
                {"sql_condition": "ELSE", "label_for_charts": "No match"},
            ],
        },
        {
            "output_column_name": "alamat_combined",
            "comparison_levels": [
                {
                    "sql_condition": "alamat_combined_l IS NULL OR alamat_combined_r IS NULL",
                    "is_null_level": True,
                    "label_for_charts": "Null",
                },
                {"sql_condition": "alamat_combined_l = alamat_combined_r", "label_for_charts": "Exact"},
                {
                    "sql_condition": "levenshtein(CAST(alamat_combined_l AS VARCHAR), CAST(alamat_combined_r AS VARCHAR)) <= 5",
                    "label_for_charts": "Levenshtein <= 5",
                },
                {"sql_condition": "ELSE", "label_for_charts": "No match"},
            ],
        },
        {
            "output_column_name": "nm_ayah",
            "comparison_levels": [
                {"sql_condition": "nm_ayah_l IS NULL OR nm_ayah_r IS NULL", "is_null_level": True, "label_for_charts": "Null"},
                {"sql_condition": "nm_ayah_l = nm_ayah_r", "label_for_charts": "Exact"},
                {
                    "sql_condition": "levenshtein(CAST(nm_ayah_l AS VARCHAR), CAST(nm_ayah_r AS VARCHAR)) <= 2",
                    "label_for_charts": "Levenshtein <= 2",
                },
                {"sql_condition": "ELSE", "label_for_charts": "No match"},
            ],
        },
        {
            "output_column_name": "nm_ibu",
            "comparison_levels": [
                {"sql_condition": "nm_ibu_l IS NULL OR nm_ibu_r IS NULL", "is_null_level": True, "label_for_charts": "Null"},
                {"sql_condition": "nm_ibu_l = nm_ibu_r", "label_for_charts": "Exact"},
                {
                    "sql_condition": "levenshtein(CAST(nm_ibu_l AS VARCHAR), CAST(nm_ibu_r AS VARCHAR)) <= 2",
                    "label_for_charts": "Levenshtein <= 2",
                },
                {"sql_condition": "ELSE", "label_for_charts": "No match"},
            ],
        },
    ]

    settings: dict[str, Any] = {
        "link_type": link_type,
        "unique_id_column_name": unique_id_column_name,
        "blocking_rules_to_generate_predictions": blocking_rules,
        "comparisons": comparisons,
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": False,
    }
    if source_dataset_column_name is not None:
        settings["source_dataset_column_name"] = source_dataset_column_name
    return settings


def _standardize_predictions(pred_df: pd.DataFrame, unique_id_column_name: str) -> pd.DataFrame:
    left_col = f"{unique_id_column_name}_l"
    right_col = f"{unique_id_column_name}_r"
    required = [left_col, right_col, "match_probability"]
    missing = [c for c in required if c not in pred_df.columns]
    if missing:
        raise RuntimeError(f"Splink predictions missing expected columns: {missing}")
    out = pred_df[[left_col, right_col, "match_probability"]].copy()
    out.columns = ["left_id", "right_id", "match_probability"]
    out["left_id"] = out["left_id"].astype(str)
    out["right_id"] = out["right_id"].astype(str)
    out["match_probability"] = pd.to_numeric(out["match_probability"], errors="coerce").fillna(0.0)
    return out


def _union_find_clusters(record_ids: pd.Series, edges: pd.DataFrame) -> pd.DataFrame:
    parent: dict[str, str] = {}
    rank: dict[str, int] = {}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            parent[ra] = rb
        elif rank[ra] > rank[rb]:
            parent[rb] = ra
        else:
            parent[rb] = ra
            rank[ra] += 1

    ids = [str(x) for x in record_ids.dropna().astype(str).tolist()]
    for rid in ids:
        parent[rid] = rid
        rank[rid] = 0

    best_probability = {rid: 0.0 for rid in ids}
    for row in edges.itertuples(index=False):
        left = str(row.left_id)
        right = str(row.right_id)
        prob = float(row.match_probability)
        if left in parent and right in parent:
            union(left, right)
            best_probability[left] = max(best_probability[left], prob)
            best_probability[right] = max(best_probability[right], prob)

    roots = {rid: find(rid) for rid in ids}
    ordered_roots = sorted(set(roots.values()))
    cluster_id_map = {root: idx + 1 for idx, root in enumerate(ordered_roots)}

    records = []
    for rid in ids:
        records.append(
            {
                "record_id": rid,
                "cluster_key": cluster_id_map[roots[rid]],
                "best_match_probability": best_probability[rid],
            }
        )
    return pd.DataFrame(records)


def _ensure_import_ready() -> None:
    if SPLINK_V4:
        return
    if DuckDBLinker is None:
        raise RuntimeError(
            "Splink DuckDB backend is not importable. "
            f"Install compatible versions first. Import error: {IMPORT_ERROR}"
        )


def _create_linker(data: pd.DataFrame, settings: dict[str, Any]) -> Any:
    if SPLINK_V4 and SplinkLinker is not None and DuckDBAPI is not None:
        return SplinkLinker(data, settings, db_api=DuckDBAPI())
    if DuckDBLinker is None:
        raise RuntimeError(
            "Splink DuckDB backend is not importable. "
            f"Install compatible versions first. Import error: {IMPORT_ERROR}"
        )
    return DuckDBLinker(data, settings)


def _train_model(linker: Any, cfg: AppConfig, blocking_rules: list[str]) -> None:
    if SPLINK_V4 and hasattr(linker, "training"):
        trainer = linker.training
        if hasattr(trainer, "estimate_u_using_random_sampling"):
            trainer.estimate_u_using_random_sampling(max_pairs=cfg.run.max_training_pairs)
        if cfg.deterministic_rules and hasattr(trainer, "estimate_probability_two_random_records_match"):
            try:
                trainer.estimate_probability_two_random_records_match(cfg.deterministic_rules, recall=0.7)
            except Exception:
                pass
        if hasattr(trainer, "estimate_parameters_using_expectation_maximisation"):
            for rule in blocking_rules[:3]:
                try:
                    trainer.estimate_parameters_using_expectation_maximisation(rule)
                except Exception:
                    # Some blocking rules may produce no training pairs.
                    continue
        return

    if hasattr(linker, "estimate_u_using_random_sampling"):
        linker.estimate_u_using_random_sampling(max_pairs=cfg.run.max_training_pairs)
    if cfg.deterministic_rules and hasattr(linker, "estimate_probability_two_random_records_match"):
        linker.estimate_probability_two_random_records_match(cfg.deterministic_rules)
    if hasattr(linker, "estimate_parameters_using_expectation_maximisation"):
        for rule in blocking_rules[:3]:
            try:
                linker.estimate_parameters_using_expectation_maximisation(rule)
            except Exception:
                continue


def _predict(linker: Any) -> Any:
    if SPLINK_V4 and hasattr(linker, "inference"):
        return linker.inference.predict()
    return linker.predict()


def run_full_dedupe(frame: pd.DataFrame, cfg: AppConfig) -> FullRunResult:
    _ensure_import_ready()
    settings = _settings_dict(
        cfg=cfg,
        link_type="dedupe_only",
        blocking_rules=cfg.blocking_rules.full,
        unique_id_column_name="record_id",
    )
    linker = _create_linker(frame, settings)
    _train_model(linker, cfg, cfg.blocking_rules.full)

    predictions = _predict(linker)
    pred_df = _standardize_predictions(predictions.as_pandas_dataframe(), unique_id_column_name="record_id")

    review = pred_df[
        (pred_df["match_probability"] >= cfg.thresholds.review_match_probability)
        & (pred_df["match_probability"] < cfg.thresholds.auto_match_probability)
    ].copy()

    auto_edges = pred_df[pred_df["match_probability"] >= cfg.thresholds.auto_match_probability].copy()
    clusters = _union_find_clusters(frame["record_id"], auto_edges)
    return FullRunResult(clusters=clusters, review_pairs=review, scored_pairs=len(pred_df))


def _parse_record_token(token: str) -> str | None:
    if token.startswith("record::"):
        return token.split("::", 1)[1]
    return None


def _parse_entity_token(token: str) -> int | None:
    if token.startswith("entity::"):
        return int(token.split("::", 1)[1])
    return None


def run_incremental_link(new_records: pd.DataFrame, entity_index: pd.DataFrame, cfg: AppConfig) -> IncrementalRunResult:
    _ensure_import_ready()
    if new_records.empty:
        return IncrementalRunResult(
            auto_links=pd.DataFrame(columns=["record_id", "entity_id", "match_probability"]),
            review_links=pd.DataFrame(columns=["record_id", "candidate_entity_id", "match_probability"]),
            unmatched=pd.DataFrame(columns=["record_id"]),
            scored_pairs=0,
        )

    if entity_index.empty:
        return IncrementalRunResult(
            auto_links=pd.DataFrame(columns=["record_id", "entity_id", "match_probability"]),
            review_links=pd.DataFrame(columns=["record_id", "candidate_entity_id", "match_probability"]),
            unmatched=new_records[["record_id"]].copy(),
            scored_pairs=0,
        )

    new_frame = new_records.copy()
    new_frame["link_id"] = new_frame["record_id"].map(lambda x: f"record::{x}")
    new_frame["source_dataset"] = "new"

    entity_frame = entity_index.copy()
    entity_frame["link_id"] = entity_frame["record_id"]
    entity_frame["source_dataset"] = "entity"

    model_cols = ["link_id", "source_dataset"] + MODEL_COLUMNS
    combined = pd.concat(
        [new_frame[model_cols], entity_frame[model_cols]],
        axis=0,
        ignore_index=True,
    )

    settings = _settings_dict(
        cfg=cfg,
        link_type="link_only",
        blocking_rules=cfg.blocking_rules.incremental,
        unique_id_column_name="link_id",
        source_dataset_column_name="source_dataset",
    )
    linker = _create_linker(combined, settings)
    _train_model(linker, cfg, cfg.blocking_rules.incremental)

    predictions = _predict(linker)
    pred_df = _standardize_predictions(predictions.as_pandas_dataframe(), unique_id_column_name="link_id")
    scored_pairs = len(pred_df)

    rows: list[dict[str, Any]] = []
    for row in pred_df.itertuples(index=False):
        left = str(row.left_id)
        right = str(row.right_id)
        prob = float(row.match_probability)
        record_id = _parse_record_token(left)
        entity_id = _parse_entity_token(right)
        if record_id is None or entity_id is None:
            record_id = _parse_record_token(right)
            entity_id = _parse_entity_token(left)
        if record_id is None or entity_id is None:
            continue
        rows.append(
            {
                "record_id": str(record_id),
                "candidate_entity_id": int(entity_id),
                "match_probability": prob,
            }
        )

    if not rows:
        return IncrementalRunResult(
            auto_links=pd.DataFrame(columns=["record_id", "entity_id", "match_probability"]),
            review_links=pd.DataFrame(columns=["record_id", "candidate_entity_id", "match_probability"]),
            unmatched=new_records[["record_id"]].copy(),
            scored_pairs=scored_pairs,
        )

    candidates = pd.DataFrame(rows)
    best = (
        candidates.sort_values(["record_id", "match_probability"], ascending=[True, False])
        .drop_duplicates(subset=["record_id"], keep="first")
        .reset_index(drop=True)
    )

    auto = best[best["match_probability"] >= cfg.thresholds.auto_match_probability].copy()
    auto = auto.rename(columns={"candidate_entity_id": "entity_id"})

    review = best[
        (best["match_probability"] >= cfg.thresholds.review_match_probability)
        & (best["match_probability"] < cfg.thresholds.auto_match_probability)
    ].copy()

    claimed = set(auto["record_id"].astype(str).tolist()) | set(review["record_id"].astype(str).tolist())
    unmatched = new_records[~new_records["record_id"].isin(claimed)][["record_id"]].copy()

    return IncrementalRunResult(
        auto_links=auto[["record_id", "entity_id", "match_probability"]],
        review_links=review[["record_id", "candidate_entity_id", "match_probability"]],
        unmatched=unmatched,
        scored_pairs=scored_pairs,
    )
