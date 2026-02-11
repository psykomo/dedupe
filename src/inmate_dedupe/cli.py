from __future__ import annotations

import argparse
import json
from pathlib import Path

from inmate_dedupe.config import load_config
from inmate_dedupe.pipeline import DedupePipeline


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inmate dedupe pipeline (Splink + DuckDB)")
    parser.add_argument("--config", required=True, help="Path to config YAML")

    sub = parser.add_subparsers(dest="command", required=True)

    init_schema = sub.add_parser("init-schema", help="Apply DuckDB schema DDL")
    init_schema.add_argument(
        "--sql",
        default="sql/duckdb_schema.sql",
        help="Path to schema SQL file",
    )

    sub.add_parser("full", help="Run a full historical dedupe pass")
    sub.add_parser("incremental", help="Run incremental dedupe from watermark")
    validate_source = sub.add_parser("validate-source", help="Validate source connectivity/query/column readiness")
    validate_source.add_argument(
        "--sample-rows",
        type=int,
        default=500,
        help="Number of rows to inspect for quality checks",
    )
    bootstrap = sub.add_parser("bootstrap", help="Backfill historical data incrementally in batches")
    bootstrap.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Override bootstrap batch size for this run",
    )
    return parser


def main() -> None:
    parser = _parser()
    args = parser.parse_args()

    cfg = load_config(args.config)
    pipeline = DedupePipeline(cfg)

    if args.command == "init-schema":
        pipeline.init_schema(Path(args.sql))
        print(json.dumps({"status": "ok", "action": "init-schema", "sql": str(args.sql)}, indent=2))
        return

    if args.command == "full":
        summary = pipeline.run_full()
        print(json.dumps(summary.__dict__, indent=2, default=str))
        return

    if args.command == "incremental":
        summary = pipeline.run_incremental()
        print(json.dumps(summary.__dict__, indent=2, default=str))
        return

    if args.command == "validate-source":
        report = pipeline.validate_source(sample_rows=args.sample_rows)
        print(json.dumps(report, indent=2, default=str))
        if not bool(report.get("ok", False)):
            raise SystemExit(2)
        return

    if args.command == "bootstrap":
        summary = pipeline.run_bootstrap(max_rows=args.max_rows)
        print(json.dumps(summary.__dict__, indent=2, default=str))
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
