# RUKS Data

This repository publishes open, documented, reproducible outputs for the Danish workbook:

- [Register for Udvalgte Kroniske Sygdomme og Svære Psykiske Lidelser (RUKS) 2010-2025](https://cdn1.gopublic.dk/sundhedsdatastyrelsen/Media/638999138200017589/Register%20for%20Udvalgte%20Kroniske%20Sygdomme%20og%20Sv%C3%A6re%20Psykiske%20Lidelser%20(RUKS)%202010-2025%20(udgivet%2028.%20november%202025).XLSX)

The initial implementation focuses on the `Hovedresultater` sheet and turns the workbook into:

- a normalized long-format dataset
- a SQLite database with a small star schema
- lightweight JSON for a GitHub Pages site
- version manifests and a text history log in Git

## Stack

- Python
- `uv`
- `openpyxl`
- `polars`
- `sqlite3`
- Quarto
- GitHub Actions

## Repository strategy

To keep the repository practical to clone while still preserving change history:

- small, text-friendly metadata stays in Git
- site-facing JSON stays in Git
- large generated artifacts are written to `artifacts/` and meant for GitHub Releases
- Git history is preserved through manifests and `data/history/releases.csv`

This follows GitHub's repository and Pages guidance:

- [Repository limits](https://docs.github.com/en/enterprise-cloud@latest/repositories/creating-and-managing-repositories/repository-limits)
- [About large files on GitHub](https://docs.github.com/en/repositories/working-with-files/managing-large-files/about-large-files-on-github)
- [GitHub Pages limits](https://docs.github.com/en/pages/getting-started-with-github-pages/github-pages-limits)

## Data notes

The `Hovedresultater` table contains many blank cells for fine-grained municipality, sex, age, and year combinations. A likely explanation is disclosure control for small cells. Sundhedsdatastyrelsen describes this as "diskretionering" and gives `<5` as an example for suppressed small counts:

- [Beskyttelse af sundhedsdata](https://sundhedsdatastyrelsen.dk/borger/om-sundhedsdata/beskyttelse-af-sundhedsdata)

The current pipeline preserves empty workbook cells as missing values. A follow-up task is to verify whether every blank in this publication should be interpreted as suppression rather than another form of missingness.

## Layout

- `src/ruks_data/`: Python package with the pipeline
- `scripts/run_pipeline.py`: local and CI entry point
- `data/manifests/`: version manifests tracked in Git
- `data/history/releases.csv`: text history of source snapshots
- `site/data/`: lightweight JSON copied into the Quarto site
- `artifacts/`: ignored local build directory for SQLite/Parquet/CSV release assets
- `.github/workflows/update_ruks.yml`: scheduled update workflow

## Local usage

```bash
uv sync
uv run python scripts/run_pipeline.py
quarto render
```

`quarto render` requires a local Quarto installation. On Ubuntu, Quarto is typically not available as a standard `apt` package named `quarto`, so install it from the official Quarto downloads page or the official Linux tarball instructions:

- https://quarto.org/docs/download/
- https://quarto.org/docs/download/tarball.html

## Current scope

Version 1 covers:

- workbook download
- workbook metadata extraction
- transformation of `Hovedresultater`
- release-ready artifacts and site summary data
- a minimal Quarto-based publication site

The workbook sheets `Dokumentation` and `Opmærksomhedspunkter` are not yet parsed into structured outputs.
