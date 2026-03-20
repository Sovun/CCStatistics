# CC Statistics Aggregator — Project Rules

## Critical Data Safety Rules

**NEVER modify, overwrite, or delete engineer source sheet files in Google Drive.**
The application is read-only with respect to all engineer sheets. It only writes to the output spreadsheet in the "Aggregated Info" subfolder.

- Engineer sheets are identified by being direct children of `DRIVE_FOLDER_ID`
- The only file the app may write to is the aggregated output sheet inside the "Aggregated Info" subfolder
- If you are ever unsure whether an operation writes to a source file, do not proceed

## Architecture

Pipeline flow (read-only → aggregate → write output only):
```
Google Drive folder
  ├── Engineer Sheet 1  (READ ONLY)
  ├── Engineer Sheet 2  (READ ONLY)
  └── Aggregated Info/
        └── CC Statistics Aggregated  (WRITE — output only)
```

Modules:
- `src/auth.py` — Google OAuth2
- `src/config.py` — env config, column synonyms, validate_config()
- `src/drive_client.py` — Drive API: list sheets, find subfolder, get/create output sheet
- `src/sheets_reader.py` — Sheets API: read engineer data (read-only)
- `src/aggregator.py` — pure pandas aggregation, no I/O
- `src/comment_analyzer.py` — Claude API for qualitative comment analysis
- `src/output_writer.py` — Sheets API: write to aggregated output only
- `src/main.py` — pipeline entry point

## Google Drive Setup

- `DRIVE_FOLDER_ID` — the Shared Drive folder containing all engineer sheets
- The "Aggregated Info" subfolder must already exist in that folder (the app does not create it)
- The output spreadsheet ("CC Statistics Aggregated") is created automatically if absent

## Column Mapping

Engineer sheets may use different column names. The flexible mapping is in `src/config.py` under `COLUMN_SYNONYMS`. Required columns are `task`, `estimated_hours`, `actual_hours`. All other columns are optional.

When adding new synonyms, edit only `COLUMN_SYNONYMS` in `src/config.py` — do not modify reader logic.

## Running

```bash
python3 -m src.main
```

First run opens a browser for Google OAuth. Token is cached in `token.json`.

## Testing

```bash
python3 -m pytest -v
```

All tests use mocks — no real API calls are made during testing.

## Files Never to Commit

- `.env` (contains API keys)
- `credentials.json` (Google OAuth client secret)
- `token.json` (Google OAuth token)
