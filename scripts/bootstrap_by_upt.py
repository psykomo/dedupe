#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from inmate_dedupe.config import load_config
from inmate_dedupe.pipeline import DedupePipeline


@dataclass
class UptRunTotals:
    upt: str
    batches: int
    records_processed: int
    auto_matches: int
    review_candidates: int
    new_entities: int
    final_run_id: int | None


def _parse_upt(value: str) -> int:
    token = value.strip()
    if not token.isdigit():
        raise ValueError(f"Invalid UPT code: {value!r}")
    upt = int(token)
    if upt < 1 or upt > 999:
        raise ValueError(f"UPT must be between 001 and 999, got: {token}")
    return upt


def _load_upt_range(args: argparse.Namespace) -> list[int]:
    if args.upt_list is not None:
        upts: list[int] = []
        for raw in args.upt_list.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            upts.append(_parse_upt(line))
        if not upts:
            raise ValueError(f"No UPT values found in {args.upt_list}")
        return sorted(dict.fromkeys(upts))

    if args.upt_start is None or args.upt_end is None:
        raise ValueError("Use either --upt-list OR both --upt-start and --upt-end")

    start = _parse_upt(str(args.upt_start))
    end = _parse_upt(str(args.upt_end))
    if end < start:
        raise ValueError(f"upt_end must be >= upt_start ({start:03d}), got {end:03d}")
    return list(range(start, end + 1))


def _render_config(template_text: str, upt: int) -> str:
    if upt >= 999:
        raise ValueError("UPT 999 is not supported by this range template. Use 001-998.")
    upt_code = f"{upt:03d}"
    next_upt_code = f"{upt + 1:03d}"
    rendered = template_text.replace("__UPT__", upt_code).replace("__UPT_NEXT__", next_upt_code)
    missing = [token for token in ("__UPT__", "__UPT_NEXT__") if token in rendered]
    if missing:
        raise ValueError(f"Template still contains unresolved placeholders: {missing}")
    return rendered


def _run_bootstrap_until_done(
    config_path: Path,
    max_rows: int | None,
    upt_code: str,
) -> UptRunTotals:
    cfg = load_config(config_path)
    pipeline = DedupePipeline(cfg)

    totals = UptRunTotals(
        upt=upt_code,
        batches=0,
        records_processed=0,
        auto_matches=0,
        review_candidates=0,
        new_entities=0,
        final_run_id=None,
    )

    while True:
        summary = pipeline.run_bootstrap(max_rows=max_rows)
        totals.batches += 1
        totals.records_processed += int(summary.records_processed)
        totals.auto_matches += int(summary.auto_matches)
        totals.review_candidates += int(summary.review_candidates)
        totals.new_entities += int(summary.new_entities)
        totals.final_run_id = int(summary.run_id)
        print(
            json.dumps(
                {
                    "upt": upt_code,
                    "batch": totals.batches,
                    "run_id": summary.run_id,
                    "records_processed": summary.records_processed,
                    "auto_matches": summary.auto_matches,
                    "review_candidates": summary.review_candidates,
                    "new_entities": summary.new_entities,
                    "scored_pairs": summary.scored_pairs,
                },
                indent=2,
                default=str,
            )
        )
        # Prefer authoritative completion signal from DuckDB bootstrap state.
        state = pipeline.dedupe_store.load_bootstrap_state(cfg.source_mysql.source_table)
        if state is not None and bool(state.get("completed", False)):
            break
        # Fallback safety: a zero-row run is terminal.
        if int(summary.records_processed) == 0:
            break

    return totals


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run bootstrap per UPT using one template config and auto-rendered per-UPT configs.",
    )
    parser.add_argument(
        "--template-config",
        type=Path,
        required=True,
        help="Template YAML with __UPT__ and __UPT_NEXT__ placeholders",
    )
    parser.add_argument(
        "--render-dir",
        type=Path,
        default=Path("./work/upt_configs"),
        help="Directory to write rendered per-UPT config files",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Override bootstrap batch size for every bootstrap call",
    )
    parser.add_argument(
        "--init-schema-sql",
        type=Path,
        default=Path("sql/duckdb_schema.sql"),
        help="Schema SQL path used before processing each DuckDB path.",
    )
    parser.add_argument(
        "--skip-init-schema",
        action="store_true",
        help="Skip schema initialization step.",
    )
    parser.add_argument(
        "--upt-list",
        type=Path,
        default=None,
        help="Optional file with one UPT per line (example: 001, 073, 600).",
    )
    parser.add_argument("--upt-start", type=int, default=None, help="UPT start (inclusive), e.g. 1")
    parser.add_argument("--upt-end", type=int, default=None, help="UPT end (inclusive), e.g. 600")
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue with next UPT when one UPT fails",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render configs only, do not execute bootstrap",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    template_path = args.template_config.expanduser().resolve()
    if not template_path.exists():
        raise FileNotFoundError(f"Template config not found: {template_path}")
    template_text = template_path.read_text(encoding="utf-8")

    upts = _load_upt_range(args)
    render_dir = args.render_dir.expanduser().resolve()
    render_dir.mkdir(parents=True, exist_ok=True)

    results: list[UptRunTotals] = []
    failures: list[dict[str, str]] = []
    initialized_duckdb_paths: set[str] = set()

    for upt in upts:
        upt_code = f"{upt:03d}"
        print(f"[UPT {upt_code}] rendering config")
        try:
            rendered = _render_config(template_text, upt)
            config_path = render_dir / f"config.upt.{upt_code}.yaml"
            config_path.write_text(rendered, encoding="utf-8")
            print(f"[UPT {upt_code}] config={config_path}")

            if args.dry_run:
                continue

            cfg = load_config(config_path)
            dedupe_path = str(cfg.dedupe_duckdb.path)
            if not args.skip_init_schema and dedupe_path not in initialized_duckdb_paths:
                pipeline = DedupePipeline(cfg)
                pipeline.init_schema(args.init_schema_sql)
                initialized_duckdb_paths.add(dedupe_path)
                print(f"[UPT {upt_code}] schema initialized for {dedupe_path}")

            totals = _run_bootstrap_until_done(
                config_path=config_path,
                max_rows=args.max_rows,
                upt_code=upt_code,
            )
            results.append(totals)
            print(
                json.dumps(
                    {
                        "upt": upt_code,
                        "status": "completed",
                        **asdict(totals),
                    },
                    indent=2,
                    default=str,
                )
            )
        except Exception as exc:
            message = str(exc)
            failures.append({"upt": upt_code, "error": message})
            print(json.dumps({"upt": upt_code, "status": "failed", "error": message}, indent=2), file=sys.stderr)
            if not args.continue_on_error:
                break

    summary = {
        "status": "dry_run" if args.dry_run else ("completed" if not failures else "completed_with_errors"),
        "template_config": str(template_path),
        "render_dir": str(render_dir),
        "upt_requested": len(upts),
        "upt_completed": len(results),
        "upt_failed": len(failures),
        "records_processed_total": int(sum(r.records_processed for r in results)),
        "auto_matches_total": int(sum(r.auto_matches for r in results)),
        "review_candidates_total": int(sum(r.review_candidates for r in results)),
        "new_entities_total": int(sum(r.new_entities for r in results)),
        "failures": failures,
    }
    print(json.dumps(summary, indent=2, default=str))
    return 1 if failures and not args.continue_on_error else 0


if __name__ == "__main__":
    raise SystemExit(main())
