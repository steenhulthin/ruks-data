from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def load_json(path: Path) -> dict:
    require(path.exists(), f"Missing JSON file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def run_data_checks(repo_root: Path) -> None:
    manifest_path = repo_root / "data" / "manifests" / "latest.json"
    summary_path = repo_root / "site" / "data" / "latest-summary.json"
    history_path = repo_root / "data" / "history" / "releases.csv"

    manifest = load_json(manifest_path)
    summary = load_json(summary_path)

    require(manifest["source_sheet"] == "Hovedresultater", "Manifest source_sheet should be Hovedresultater")
    require(manifest["source_row_count"] > 100_000, "Source row count is unexpectedly low")
    require(manifest["observation_count"] > 2_000_000, "Observation count is unexpectedly low")
    require(len(summary["diseases"]) >= 9, "Expected at least 9 diseases in summary")
    require(len(summary["series"]) >= len(summary["diseases"]) * 2, "Expected multiple series per disease")
    require(summary["release_tag"] == manifest["release_tag"], "Summary and manifest release tags differ")
    require(summary["source_row_count"] == manifest["source_row_count"], "Summary and manifest row counts differ")
    require(summary["observation_count"] == manifest["observation_count"], "Summary and manifest observation counts differ")

    dated_manifest_path = repo_root / "data" / "manifests" / f"{manifest['release_tag']}.json"
    require(dated_manifest_path.exists(), f"Missing dated manifest: {dated_manifest_path}")

    for artifact_name in manifest["artifacts"]:
        artifact_path = repo_root / "artifacts" / "releases" / "assets" / artifact_name
        require(artifact_path.exists(), f"Missing artifact: {artifact_path}")
        require(artifact_path.stat().st_size > 0, f"Artifact is empty: {artifact_path}")

    for artifact_name in manifest.get("latest_artifacts", []):
        artifact_path = repo_root / "artifacts" / "releases" / "assets" / artifact_name
        require(artifact_path.exists(), f"Missing latest-alias artifact: {artifact_path}")
        require(artifact_path.stat().st_size > 0, f"Latest-alias artifact is empty: {artifact_path}")

    sqlite_path = repo_root / "artifacts" / "releases" / "assets" / manifest["artifacts"][-1]
    connection = sqlite3.connect(sqlite_path)
    try:
        fact_count = connection.execute("select count(*) from fact_observation").fetchone()[0]
        disease_count = connection.execute("select count(*) from dim_disease").fetchone()[0]
        geography_count = connection.execute("select count(*) from dim_geography").fetchone()[0]
        measure_count = connection.execute("select count(*) from dim_measure").fetchone()[0]
        sex_count = connection.execute("select count(*) from dim_sex").fetchone()[0]
        age_group_count = connection.execute("select count(*) from dim_age_group").fetchone()[0]
        unit_count = connection.execute("select count(*) from dim_unit").fetchone()[0]
    finally:
        connection.close()

    require(fact_count == manifest["observation_count"], "SQLite fact count does not match manifest observation count")
    require(disease_count >= 9, "SQLite disease dimension is unexpectedly small")
    require(geography_count >= 104, "SQLite geography dimension is unexpectedly small")
    require(measure_count == 2, "Expected exactly 2 measures")
    require(sex_count == 4, "Expected exactly 4 sex categories")
    require(age_group_count == 20, "Expected exactly 20 age groups")
    require(unit_count == 4, "Expected exactly 4 unit variants")

    require(history_path.exists(), f"Missing history file: {history_path}")
    with history_path.open(encoding="utf-8", newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if any(row.values())]
    require(rows, "History file should contain at least one release row")
    require(any(row["source_sha256"] == manifest["source_sha256"] for row in rows), "Latest source hash missing from history file")

    for series in summary["series"]:
        years = [item["year"] for item in series["values"]]
        require(years == sorted(years), f"Series years are not sorted for {series['disease']} / {series['measure_code']}")
        require(len(set(years)) == len(years), f"Duplicate years found in summary series for {series['disease']} / {series['measure_code']}")


def run_site_checks(repo_root: Path) -> None:
    docs_root = repo_root / "docs"
    require((docs_root / "index.html").exists(), "Rendered index.html is missing")
    require((docs_root / "usage.html").exists(), "Rendered usage.html is missing")
    require((docs_root / "methodology.html").exists(), "Rendered methodology.html is missing")
    require((docs_root / "credits.html").exists(), "Rendered credits.html is missing")
    require((docs_root / "site" / "data" / "latest-summary.json").exists(), "Rendered site data JSON is missing")
    require(not (docs_root / "TODO.html").exists(), "Unexpected TODO page rendered into docs/")
    require(not (docs_root / "artifacts").exists(), "Unexpected artifacts content rendered into docs/")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run smoke tests for generated RUKS outputs.")
    parser.add_argument("--repo-root", default=Path.cwd(), type=Path)
    parser.add_argument("--check-site", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    run_data_checks(repo_root)
    if args.check_site:
        run_site_checks(repo_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
