from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class SourceMysqlConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    source_table: str
    custom_query: str | None
    source_id_column: str
    source_updated_at_column: str | None


@dataclass(frozen=True)
class DedupeDuckDBConfig:
    path: Path


@dataclass(frozen=True)
class ColumnConfig:
    id_upt: str
    nik: str
    nomor_induk_nasional: str
    nama_lengkap: str
    nama_alias1: str
    nama_alias2: str
    nama_alias3: str
    nama_kecil1: str
    nama_kecil2: str
    nama_kecil3: str
    tanggal_lahir: str
    id_jenis_kelamin: str
    alamat: str
    alamat_alternatif: str
    kodepos: str
    telepon: str
    telephone_keluarga: str
    nm_ayah: str
    nm_ibu: str
    nm_istri_suami: str
    passthrough: list[str]


@dataclass(frozen=True)
class RunConfig:
    model_version: str
    batch_size: int
    work_dir: Path
    watermark_file: Path
    bootstrap_state_file: Path
    bootstrap_batch_rows: int
    review_candidate_limit: int
    max_training_pairs: int


@dataclass(frozen=True)
class ThresholdConfig:
    auto_match_probability: float
    review_match_probability: float


@dataclass(frozen=True)
class BlockingRules:
    full: list[str]
    incremental: list[str]


@dataclass(frozen=True)
class AppConfig:
    source_mysql: SourceMysqlConfig
    dedupe_duckdb: DedupeDuckDBConfig
    columns: ColumnConfig
    run: RunConfig
    thresholds: ThresholdConfig
    blocking_rules: BlockingRules
    deterministic_rules: list[str]


def _required(mapping: dict[str, Any], key: str) -> Any:
    if key not in mapping:
        raise ValueError(f"Missing required config key: {key}")
    return mapping[key]


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).expanduser().resolve()
    with config_path.open("r", encoding="utf-8") as fp:
        raw = yaml.safe_load(fp)

    source_mysql = _required(raw, "source_mysql")
    dedupe_duckdb = _required(raw, "dedupe_duckdb")
    cols = _required(raw, "columns")
    run = _required(raw, "run")
    thresholds = _required(raw, "thresholds")
    blocking = _required(raw, "blocking_rules")
    deterministic = raw.get("deterministic_rules", [])

    cfg = AppConfig(
        source_mysql=SourceMysqlConfig(
            host=_required(source_mysql, "host"),
            port=int(_required(source_mysql, "port")),
            user=_required(source_mysql, "user"),
            password=_required(source_mysql, "password"),
            database=_required(source_mysql, "database"),
            source_table=_required(source_mysql, "source_table"),
            custom_query=source_mysql.get("custom_query"),
            source_id_column=_required(source_mysql, "source_id_column"),
            source_updated_at_column=source_mysql.get("source_updated_at_column"),
        ),
        dedupe_duckdb=DedupeDuckDBConfig(
            path=Path(_required(dedupe_duckdb, "path")).expanduser().resolve()
        ),
        columns=ColumnConfig(
            id_upt=_required(cols, "id_upt"),
            nik=_required(cols, "nik"),
            nomor_induk_nasional=_required(cols, "nomor_induk_nasional"),
            nama_lengkap=_required(cols, "nama_lengkap"),
            nama_alias1=_required(cols, "nama_alias1"),
            nama_alias2=_required(cols, "nama_alias2"),
            nama_alias3=_required(cols, "nama_alias3"),
            nama_kecil1=_required(cols, "nama_kecil1"),
            nama_kecil2=_required(cols, "nama_kecil2"),
            nama_kecil3=_required(cols, "nama_kecil3"),
            tanggal_lahir=_required(cols, "tanggal_lahir"),
            id_jenis_kelamin=_required(cols, "id_jenis_kelamin"),
            alamat=_required(cols, "alamat"),
            alamat_alternatif=_required(cols, "alamat_alternatif"),
            kodepos=_required(cols, "kodepos"),
            telepon=_required(cols, "telepon"),
            telephone_keluarga=_required(cols, "telephone_keluarga"),
            nm_ayah=_required(cols, "nm_ayah"),
            nm_ibu=_required(cols, "nm_ibu"),
            nm_istri_suami=_required(cols, "nm_istri_suami"),
            passthrough=list(cols.get("passthrough", [])),
        ),
        run=RunConfig(
            model_version=_required(run, "model_version"),
            batch_size=int(_required(run, "batch_size")),
            work_dir=Path(_required(run, "work_dir")).expanduser().resolve(),
            watermark_file=Path(_required(run, "watermark_file")).expanduser().resolve(),
            bootstrap_state_file=Path(
                run.get("bootstrap_state_file", "./work/bootstrap_state.json")
            )
            .expanduser()
            .resolve(),
            bootstrap_batch_rows=int(run.get("bootstrap_batch_rows", 200_000)),
            review_candidate_limit=int(run.get("review_candidate_limit", 200_000)),
            max_training_pairs=int(run.get("max_training_pairs", 5_000_000)),
        ),
        thresholds=ThresholdConfig(
            auto_match_probability=float(_required(thresholds, "auto_match_probability")),
            review_match_probability=float(_required(thresholds, "review_match_probability")),
        ),
        blocking_rules=BlockingRules(
            full=list(_required(blocking, "full")),
            incremental=list(_required(blocking, "incremental")),
        ),
        deterministic_rules=list(deterministic),
    )

    if cfg.thresholds.review_match_probability >= cfg.thresholds.auto_match_probability:
        raise ValueError("review_match_probability must be lower than auto_match_probability")
    if cfg.run.batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if cfg.run.bootstrap_batch_rows <= 0:
        raise ValueError("bootstrap_batch_rows must be > 0")
    if not cfg.blocking_rules.full or not cfg.blocking_rules.incremental:
        raise ValueError("At least one blocking rule is required for full and incremental runs")
    if not cfg.source_mysql.source_updated_at_column:
        # Incremental/bootstrap will use source_id cursor mode.
        pass

    cfg.run.work_dir.mkdir(parents=True, exist_ok=True)
    cfg.run.watermark_file.parent.mkdir(parents=True, exist_ok=True)
    cfg.run.bootstrap_state_file.parent.mkdir(parents=True, exist_ok=True)
    cfg.dedupe_duckdb.path.parent.mkdir(parents=True, exist_ok=True)
    return cfg
