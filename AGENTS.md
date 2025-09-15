# Repository Guidelines

## Project Structure & Module Organization
- `import.py`: Used to import event CSV data into the Victorametrics DB
- `sanitize.py`: Used to sanitize the CSV data from PII.
- `tests/`: Pytest suite (files like `test_*.py`). Test data under `tests/data/`.
- `dashboards/`: Grafana Dashboards


## Build, Test, and Development Commands
- Create env: `poetry install --dev`
- Style/lint (ruff): `ruff check --fix` and `ruff format`.
- Run tests directly: `pytest -v` or focused: `pytest -k keyword`.

## Coding Style & Naming Conventions
- Formatter/linter: Ruff (configured via `.pre-commit-config.yaml`).
- Indentation: 4 spaces. Line length: 140 (see `pyproject.toml`).
- Naming: modules/functions/variables `snake_case`; classes `CamelCase`; tests `test_*.py`.
- Keep imports sorted (isort) and unused code removed (ruff enforces).

## Testing Guidelines
- Framework: Pytest with coverage (`coverage run -m pytest` via tox).
- Location: place new tests in `tests/` alongside related module area.
- Conventions: name files `test_<unit>.py`; use fixtures from `tests/conftest.py`.
- Quick check: `pytest -q`; coverage report via `tox` or `coverage report -m`.

## Commit & Pull Request Guidelines
- Commits: imperative mood, concise scope (e.g., `fix: handle NULL types`). Reference issues (`#123`) when relevant.
- PRs: include clear description, steps to reproduce/verify, and screenshots or snippets for CLI output when helpful. Use the PR template.
- Ensure CI passes (tests + ruff). Re-run `tox -e style` before requesting review.

## Changelog Guidelines
- **REQUIRED**: Update `CHANGELOG.md` for ALL changes before submitting a PR.
- Add changes under the `[Unreleased]` section.
- Categorize changes under the appropriate section:
  - `Added` for new features
  - `Changed` for changes in existing functionality
  - `Deprecated` for soon-to-be removed features
  - `Removed` for now removed features
  - `Fixed` for any bug fixes
  - `Security` for vulnerability fixes
- Format entries as bullet points with clear, user-focused descriptions
- Include PR numbers in parentheses at the end of entries: `(#123)`
- Example entries:
  ```markdown
  ### Added
  - New command line option for verbose output (#45)

  ### Fixed
  - Handle missing timestamps in CSV imports (#52)
  ```

## Security & Configuration Tips
- Do not commit local databases or secrets. Use files under `tests/data/` for fixtures.
