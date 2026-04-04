# CC Statistics Aggregator — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Python CLI tool that reads engineer Claude Code usage sheets from Google Drive, aggregates statistics into a master table, and uses Claude AI to analyze free-text comments and generate insights — all written back into a "Aggregated Info" subfolder in Drive.

**Architecture:** A pipeline of discrete modules: Google Drive/Sheets API for I/O, pandas for data aggregation, and the Anthropic Claude API for qualitative comment analysis. The tool discovers engineer sheets automatically by scanning a configured Drive folder, normalizes their schema, merges data, and writes two output sheets — a statistics table and an AI-generated insights report.

**Tech Stack:** Python 3.11+, `google-api-python-client`, `google-auth-oauthlib`, `pandas`, `anthropic`, `python-dotenv`, `pytest`

---

## Assumptions about Engineer Sheet Schema

Each engineer's Google Sheet is expected to have at least one tab with columns roughly matching:
- Date
- Task name
- Task description
- Estimated hours (without AI)
- Actual hours (with Claude Code)
- Deviation (actual time divided by non-ai estimate)
- Comments / notes (free text)

---

### Task 1: Project Scaffold

**Files:**
- Create: `src/__init__.py`
- Create: `src/config.py`
- Create: `tests/__init__.py`
- Create: `requirements.txt`
- Create: `.env.example`
- Create: `pyproject.toml`

**Step 1: Create directory structure**

```bash
mkdir -p src tests docs/plans
touch src/__init__.py tests/__init__.py
```

**Step 2: Create `requirements.txt`**

```
google-api-python-client==2.118.0
google-auth-httplib2==0.2.0
google-auth-oauthlib==1.2.0
anthropic==0.40.0
pandas==2.2.0
python-dotenv==1.0.1
pytest==8.1.0
pytest-mock==3.14.0
```

**Step 3: Create `.env.example`**

```
# Google API credentials (path to your downloaded OAuth2 JSON)
GOOGLE_CREDENTIALS_FILE=credentials.json

# Token cache file (auto-created on first run)
GOOGLE_TOKEN_FILE=token.json

# The Google Drive folder ID containing engineer sheets
# Find it in the URL: https://drive.google.com/drive/u/0/folders/0AM9FhYb2sN8XUk9PVA
DRIVE_FOLDER_ID=https://drive.google.com/drive/u/0/folders/0AM9FhYb2sN8XUk9PVA

# The subfolder name where aggregated output goes (must exist in Drive)
AGGREGATED_FOLDER_NAME=Aggregated Info

# Anthropic API key for comment analysis
ANTHROPIC_API_KEY=your_anthropic_key_here

# Claude model to use for analysis
CLAUDE_MODEL=claude-sonnet-4-6
```

**Step 4: Create `src/config.py`**

```python
import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
GOOGLE_TOKEN_FILE = os.getenv("GOOGLE_TOKEN_FILE", "token.json")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "")
AGGREGATED_FOLDER_NAME = os.getenv("AGGREGATED_FOLDER_NAME", "Aggregated Info")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")

# Synonyms for canonical column names.
# The reader will match sheet headers (case-insensitive) against these lists.
COLUMN_SYNONYMS = {
    "task": ["task", "task name", "description", "feature", "story", "ticket", "name"],
    "estimated_hours": [
        "estimated hours", "estimate", "estimated", "without ai", "no ai",
        "manual estimate", "planned hours", "original estimate", "non-ai estimate",
    ],
    "actual_hours": [
        "actual hours", "actual", "with claude", "with ai", "claude hours",
        "real hours", "time spent", "hours spent",
    ],
    "date": ["date", "week", "sprint", "period", "created", "completed"],
    "engineer": ["engineer", "author", "name", "person", "dev", "developer"],
    "comments": [
        "comments", "comment", "notes", "note", "feedback", "observations",
        "what was good", "what was bad", "remarks", "review",
    ],
}

# Required canonical columns (rows missing all of these are dropped)
REQUIRED_COLUMNS = {"task", "estimated_hours", "actual_hours"}
```

**Step 5: Create `pyproject.toml`**

```toml
[project]
name = "cc-statistics-aggregator"
version = "0.1.0"
requires-python = ">=3.11"

[tool.pytest.ini_options]
testpaths = ["tests"]
```

**Step 6: Install dependencies**

```bash
pip install -r requirements.txt
```

**Step 7: Commit**

```bash
git init
git add .
git commit -m "chore: project scaffold with config and dependencies"
```

---

### Task 2: Google Auth Module

**Files:**
- Create: `src/auth.py`
- Create: `tests/test_auth.py`

**Step 1: Write the failing test**

```python
# tests/test_auth.py
import pytest
from unittest.mock import patch, MagicMock
from src.auth import get_google_credentials

def test_get_google_credentials_returns_credentials(tmp_path):
    """get_google_credentials returns a valid Credentials object."""
    mock_creds = MagicMock()
    mock_creds.valid = True

    with patch("src.auth.Credentials") as MockCreds, \
         patch("src.auth.os.path.exists", return_value=True), \
         patch("builtins.open", MagicMock()):
        MockCreds.from_authorized_user_file.return_value = mock_creds
        result = get_google_credentials(
            credentials_file="creds.json",
            token_file=str(tmp_path / "token.json"),
        )
    assert result == mock_creds
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/test_auth.py -v
```
Expected: FAIL — `ImportError: cannot import name 'get_google_credentials'`

**Step 3: Implement `src/auth.py`**

```python
import os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]


def get_google_credentials(
    credentials_file: str = "credentials.json",
    token_file: str = "token.json",
) -> Credentials:
    """Return valid Google OAuth2 credentials, refreshing or re-authorizing as needed."""
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_file, "w") as f:
            f.write(creds.to_json())

    return creds
```

**Step 4: Run test to verify it passes**

```bash
pytest tests/test_auth.py -v
```
Expected: PASS

**Step 5: Commit**

```bash
git add src/auth.py tests/test_auth.py
git commit -m "feat: Google OAuth2 auth module"
```

---

### Task 3: Google Drive Client

**Files:**
- Create: `src/drive_client.py`
- Create: `tests/test_drive_client.py`

**Step 1: Write the failing tests**

```python
# tests/test_drive_client.py
import pytest
from unittest.mock import MagicMock
from src.drive_client import DriveClient

@pytest.fixture
def mock_creds():
    return MagicMock()

@pytest.fixture
def client(mock_creds):
    return DriveClient(mock_creds)

def test_list_sheets_in_folder_returns_file_list(client):
    """list_sheets_in_folder returns list of {id, name} dicts for Google Sheets."""
    mock_service = MagicMock()
    client._service = mock_service

    mock_service.files().list().execute.return_value = {
        "files": [
            {"id": "abc123", "name": "Alice Stats"},
            {"id": "def456", "name": "Bob Stats"},
        ]
    }

    result = client.list_sheets_in_folder("folder123")
    assert result == [
        {"id": "abc123", "name": "Alice Stats"},
        {"id": "def456", "name": "Bob Stats"},
    ]

def test_find_subfolder_returns_folder_id(client):
    """find_subfolder returns the Drive folder ID for a named subfolder."""
    mock_service = MagicMock()
    client._service = mock_service

    mock_service.files().list().execute.return_value = {
        "files": [{"id": "sub999", "name": "Aggregated Info"}]
    }

    result = client.find_subfolder("parent123", "Aggregated Info")
    assert result == "sub999"

def test_find_subfolder_raises_when_not_found(client):
    mock_service = MagicMock()
    client._service = mock_service
    mock_service.files().list().execute.return_value = {"files": []}

    with pytest.raises(ValueError, match="Subfolder 'Aggregated Info' not found"):
        client.find_subfolder("parent123", "Aggregated Info")
```

**Step 2: Run to verify failure**

```bash
pytest tests/test_drive_client.py -v
```
Expected: FAIL — `ImportError`

**Step 3: Implement `src/drive_client.py`**

```python
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials


class DriveClient:
    def __init__(self, creds: Credentials):
        self._service = build("drive", "v3", credentials=creds)

    def list_sheets_in_folder(self, folder_id: str) -> list[dict]:
        """Return [{id, name}] for all Google Sheets files in the given folder."""
        query = (
            f"'{folder_id}' in parents"
            " and mimeType='application/vnd.google-apps.spreadsheet'"
            " and trashed=false"
        )
        response = (
            self._service.files()
            .list(q=query, fields="files(id, name)", pageSize=100)
            .execute()
        )
        return response.get("files", [])

    def find_subfolder(self, parent_folder_id: str, subfolder_name: str) -> str:
        """Return the Drive folder ID for a named subfolder. Raises ValueError if not found."""
        query = (
            f"'{parent_folder_id}' in parents"
            " and mimeType='application/vnd.google-apps.folder'"
            f" and name='{subfolder_name}'"
            " and trashed=false"
        )
        response = (
            self._service.files()
            .list(q=query, fields="files(id, name)")
            .execute()
        )
        files = response.get("files", [])
        if not files:
            raise ValueError(f"Subfolder '{subfolder_name}' not found in folder {parent_folder_id}")
        return files[0]["id"]
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_drive_client.py -v
```
Expected: PASS (3 tests)

**Step 5: Commit**

```bash
git add src/drive_client.py tests/test_drive_client.py
git commit -m "feat: Drive client — list sheets and find subfolder"
```

---

### Task 4: Sheets Reader

**Files:**
- Create: `src/sheets_reader.py`
- Create: `tests/test_sheets_reader.py`

The reader fetches all tabs from a spreadsheet, finds one that has recognizable stat columns, and returns a normalized `pandas.DataFrame` with canonical column names.

**Step 1: Write the failing tests**

```python
# tests/test_sheets_reader.py
import pandas as pd
import pytest
from unittest.mock import MagicMock
from src.sheets_reader import SheetsReader

@pytest.fixture
def mock_creds():
    return MagicMock()

@pytest.fixture
def reader(mock_creds):
    return SheetsReader(mock_creds)

def _make_service(reader, values):
    mock_service = MagicMock()
    reader._service = mock_service
    mock_service.spreadsheets().values().get().execute.return_value = {"values": values}
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Sheet1"}}]
    }
    return mock_service

def test_read_sheet_returns_dataframe(reader):
    """read_sheet returns a DataFrame with canonical column names."""
    values = [
        ["Task", "Estimated Hours", "Actual Hours", "Date", "Comments"],
        ["Build login", "8", "4", "2024-01-10", "Claude was great for boilerplate"],
        ["Fix bug #42", "2", "1", "2024-01-11", ""],
    ]
    _make_service(reader, values)

    df = reader.read_sheet("spreadsheet123", engineer_name="Alice")
    assert list(df.columns) == ["task", "estimated_hours", "actual_hours", "date", "comments", "engineer", "source_sheet"]
    assert len(df) == 2
    assert df.iloc[0]["task"] == "Build login"
    assert df.iloc[0]["estimated_hours"] == 8.0
    assert df.iloc[0]["engineer"] == "Alice"

def test_read_sheet_handles_missing_optional_columns(reader):
    """read_sheet works when optional columns (date, comments) are absent."""
    values = [
        ["Task Name", "Estimate", "Actual"],
        ["Auth module", "5", "3"],
    ]
    _make_service(reader, values)

    df = reader.read_sheet("spreadsheet123", engineer_name="Bob")
    assert "task" in df.columns
    assert "estimated_hours" in df.columns
    assert "actual_hours" in df.columns
    assert df.iloc[0]["task"] == "Auth module"

def test_read_sheet_raises_on_unrecognizable_schema(reader):
    """read_sheet raises ValueError when no required columns can be mapped."""
    values = [
        ["Foo", "Bar", "Baz"],
        ["x", "y", "z"],
    ]
    _make_service(reader, values)

    with pytest.raises(ValueError, match="Could not find required columns"):
        reader.read_sheet("spreadsheet123", engineer_name="Charlie")
```

**Step 2: Run to verify failure**

```bash
pytest tests/test_sheets_reader.py -v
```
Expected: FAIL — `ImportError`

**Step 3: Implement `src/sheets_reader.py`**

```python
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from src.config import COLUMN_SYNONYMS, REQUIRED_COLUMNS


class SheetsReader:
    def __init__(self, creds: Credentials):
        self._service = build("sheets", "v4", credentials=creds)

    def _get_sheet_tabs(self, spreadsheet_id: str) -> list[str]:
        meta = self._service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        return [s["properties"]["title"] for s in meta.get("sheets", [])]

    def _map_headers(self, headers: list[str]) -> dict[str, str]:
        """Map raw header strings to canonical column names using synonym config."""
        mapping = {}
        for raw in headers:
            normalized = raw.strip().lower()
            for canonical, synonyms in COLUMN_SYNONYMS.items():
                if normalized in synonyms and canonical not in mapping.values():
                    mapping[raw] = canonical
                    break
        return mapping

    def _read_tab(self, spreadsheet_id: str, tab: str) -> list[list]:
        result = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=tab)
            .execute()
        )
        return result.get("values", [])

    def read_sheet(self, spreadsheet_id: str, engineer_name: str) -> pd.DataFrame:
        """Read the first recognizable tab and return a normalized DataFrame."""
        tabs = self._get_sheet_tabs(spreadsheet_id)

        for tab in tabs:
            rows = self._read_tab(spreadsheet_id, tab)
            if len(rows) < 2:
                continue

            headers = rows[0]
            mapping = self._map_headers(headers)
            canonical_cols = set(mapping.values())

            if not REQUIRED_COLUMNS.issubset(canonical_cols):
                continue

            # Build DataFrame from rows
            data_rows = rows[1:]
            # Pad short rows
            max_len = len(headers)
            data_rows = [r + [""] * (max_len - len(r)) for r in data_rows]

            df = pd.DataFrame(data_rows, columns=headers)
            df = df.rename(columns=mapping)
            # Keep only canonical columns that exist
            keep = [c for c in df.columns if c in COLUMN_SYNONYMS]
            df = df[keep].copy()

            # Coerce numeric columns
            for col in ("estimated_hours", "actual_hours"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Drop rows where all required columns are empty/NaN
            df = df.dropna(subset=list(REQUIRED_COLUMNS & set(df.columns)), how="all")
            df = df[df["task"].str.strip().ne("")]

            df["engineer"] = engineer_name
            df["source_sheet"] = tab
            return df

        raise ValueError(
            f"Could not find required columns {REQUIRED_COLUMNS} in any tab of spreadsheet {spreadsheet_id}"
        )
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_sheets_reader.py -v
```
Expected: PASS (3 tests)

**Step 5: Commit**

```bash
git add src/sheets_reader.py tests/test_sheets_reader.py
git commit -m "feat: sheets reader with flexible column mapping"
```

---

### Task 5: Statistics Aggregator

**Files:**
- Create: `src/aggregator.py`
- Create: `tests/test_aggregator.py`

**Step 1: Write the failing tests**

```python
# tests/test_aggregator.py
import pandas as pd
import pytest
from src.aggregator import aggregate_stats

@pytest.fixture
def sample_frames():
    return [
        pd.DataFrame({
            "task": ["Task A", "Task B"],
            "estimated_hours": [8.0, 4.0],
            "actual_hours": [4.0, 2.0],
            "engineer": ["Alice", "Alice"],
            "date": ["2024-01-10", "2024-01-11"],
            "comments": ["Great!", ""],
            "source_sheet": ["Sheet1", "Sheet1"],
        }),
        pd.DataFrame({
            "task": ["Task C"],
            "estimated_hours": [6.0],
            "actual_hours": [5.0],
            "engineer": ["Bob"],
            "date": ["2024-01-12"],
            "comments": ["Took longer than expected"],
            "source_sheet": ["Sheet1"],
        }),
    ]

def test_aggregate_stats_combines_all_rows(sample_frames):
    result = aggregate_stats(sample_frames)
    assert len(result["all_tasks"]) == 3

def test_aggregate_stats_calculates_time_saved(sample_frames):
    result = aggregate_stats(sample_frames)
    df = result["all_tasks"]
    assert "hours_saved" in df.columns
    assert df.iloc[0]["hours_saved"] == 4.0

def test_aggregate_stats_calculates_efficiency_ratio(sample_frames):
    result = aggregate_stats(sample_frames)
    df = result["all_tasks"]
    assert "efficiency_ratio" in df.columns
    # Task A: 4/8 = 0.5, meaning 50% of estimated time used
    assert abs(df.iloc[0]["efficiency_ratio"] - 0.5) < 0.01

def test_aggregate_stats_produces_summary(sample_frames):
    result = aggregate_stats(sample_frames)
    summary = result["summary"]
    assert summary["total_tasks"] == 3
    assert summary["total_estimated_hours"] == 18.0
    assert summary["total_actual_hours"] == 11.0
    assert abs(summary["overall_efficiency_ratio"] - (11.0 / 18.0)) < 0.01

def test_aggregate_stats_collects_comments(sample_frames):
    result = aggregate_stats(sample_frames)
    comments = result["comments"]
    assert len(comments) == 2  # Only non-empty comments
    assert any("Great!" in c for c in comments)
```

**Step 2: Run to verify failure**

```bash
pytest tests/test_aggregator.py -v
```
Expected: FAIL — `ImportError`

**Step 3: Implement `src/aggregator.py`**

```python
import pandas as pd


def aggregate_stats(frames: list[pd.DataFrame]) -> dict:
    """Combine engineer DataFrames and compute derived metrics.

    Returns a dict with keys:
    - all_tasks: DataFrame of all rows with added metrics
    - summary: dict of overall totals
    - comments: list of non-empty comment strings with metadata
    """
    if not frames:
        return {"all_tasks": pd.DataFrame(), "summary": {}, "comments": []}

    combined = pd.concat(frames, ignore_index=True)

    # Derived columns
    combined["hours_saved"] = combined["estimated_hours"] - combined["actual_hours"]
    combined["efficiency_ratio"] = combined.apply(
        lambda r: r["actual_hours"] / r["estimated_hours"]
        if r["estimated_hours"] and r["estimated_hours"] > 0
        else None,
        axis=1,
    )

    # Summary stats
    total_estimated = combined["estimated_hours"].sum()
    total_actual = combined["actual_hours"].sum()
    summary = {
        "total_tasks": len(combined),
        "total_estimated_hours": round(total_estimated, 2),
        "total_actual_hours": round(total_actual, 2),
        "total_hours_saved": round(total_estimated - total_actual, 2),
        "overall_efficiency_ratio": round(total_actual / total_estimated, 4) if total_estimated else 0,
        "engineers": sorted(combined["engineer"].dropna().unique().tolist()),
    }

    # Collect non-empty comments with metadata
    comments = []
    if "comments" in combined.columns:
        for _, row in combined.iterrows():
            comment = str(row.get("comments", "")).strip()
            if comment:
                comments.append({
                    "text": comment,
                    "task": row.get("task", ""),
                    "engineer": row.get("engineer", ""),
                    "date": row.get("date", ""),
                })

    return {"all_tasks": combined, "summary": summary, "comments": comments}
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_aggregator.py -v
```
Expected: PASS (5 tests)

**Step 5: Commit**

```bash
git add src/aggregator.py tests/test_aggregator.py
git commit -m "feat: statistics aggregator with time-saved and efficiency metrics"
```

---

### Task 6: Comment Analyzer (Claude AI)

**Files:**
- Create: `src/comment_analyzer.py`
- Create: `tests/test_comment_analyzer.py`

This module sends all collected comments to Claude and asks for a structured analysis: top benefits, recurring pain points, patterns, and engineer recommendations.

**Step 1: Write the failing tests**

```python
# tests/test_comment_analyzer.py
import pytest
from unittest.mock import MagicMock, patch
from src.comment_analyzer import analyze_comments


def test_analyze_comments_returns_analysis_dict():
    """analyze_comments returns a dict with required analysis keys."""
    comments = [
        {"text": "Claude was amazing for boilerplate code", "engineer": "Alice", "task": "Auth module", "date": "2024-01-10"},
        {"text": "Struggled with complex business logic", "engineer": "Bob", "task": "Pricing rules", "date": "2024-01-11"},
    ]
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="""
## Benefits
- Great for boilerplate

## Pain Points
- Complex logic harder

## Recommendations
- Use for repetitive tasks
""")]

    with patch("src.comment_analyzer.anthropic.Anthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages.create.return_value = mock_response
        result = analyze_comments(comments, api_key="test-key", model="claude-haiku-4-5-20251001")

    assert "raw_analysis" in result
    assert "Benefits" in result["raw_analysis"]


def test_analyze_comments_returns_empty_on_no_comments():
    """analyze_comments returns an empty result dict when no comments given."""
    result = analyze_comments([], api_key="test-key")
    assert result == {"raw_analysis": "", "comment_count": 0}


def test_analyze_comments_builds_correct_prompt():
    """analyze_comments includes all comment texts in the Claude prompt."""
    comments = [
        {"text": "Super fast code generation", "engineer": "Alice", "task": "Task A", "date": ""},
    ]
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="## Analysis\n- good")]

    with patch("src.comment_analyzer.anthropic.Anthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages.create.return_value = mock_response
        analyze_comments(comments, api_key="test-key")

        call_args = instance.messages.create.call_args
        prompt_text = call_args.kwargs["messages"][0]["content"]
        assert "Super fast code generation" in prompt_text
```

**Step 2: Run to verify failure**

```bash
pytest tests/test_comment_analyzer.py -v
```
Expected: FAIL — `ImportError`

**Step 3: Implement `src/comment_analyzer.py`**

```python
import anthropic
from src.config import CLAUDE_MODEL


_ANALYSIS_PROMPT = """You are analyzing feedback from software engineers about their experience using Claude Code AI assistant for development tasks.

Below are comments collected from {engineer_count} engineers across {comment_count} tasks.

---
{comments_block}
---

Please analyze these comments and produce a structured report with the following sections:

## Summary
A 2-3 sentence overall summary of the team's Claude Code experience.

## Key Benefits
Bullet list of the most frequently cited benefits and positive patterns.

## Pain Points & Challenges
Bullet list of recurring difficulties, frustrations, or limitations observed.

## Usage Patterns
Observations about which types of tasks Claude Code helped most vs least.

## Recommendations
Actionable suggestions for the team to get more value from Claude Code based on this feedback.

## Notable Quotes
3-5 direct quotes that best capture the team's sentiment (positive and negative).

Keep the analysis concrete, grounded in the actual comments, and actionable.
"""


def analyze_comments(
    comments: list[dict],
    api_key: str,
    model: str = CLAUDE_MODEL,
) -> dict:
    """Send engineer comments to Claude for qualitative analysis.

    Args:
        comments: List of dicts with keys: text, engineer, task, date
        api_key: Anthropic API key
        model: Claude model ID to use

    Returns:
        Dict with 'raw_analysis' (markdown string) and 'comment_count'
    """
    if not comments:
        return {"raw_analysis": "", "comment_count": 0}

    comments_block = "\n\n".join(
        f"[{c.get('engineer', 'Unknown')} | {c.get('task', '')} | {c.get('date', '')}]\n{c['text']}"
        for c in comments
    )

    engineer_names = {c.get("engineer", "") for c in comments if c.get("engineer")}
    prompt = _ANALYSIS_PROMPT.format(
        engineer_count=len(engineer_names),
        comment_count=len(comments),
        comments_block=comments_block,
    )

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    raw_analysis = response.content[0].text
    return {"raw_analysis": raw_analysis, "comment_count": len(comments)}
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_comment_analyzer.py -v
```
Expected: PASS (3 tests)

**Step 5: Commit**

```bash
git add src/comment_analyzer.py tests/test_comment_analyzer.py
git commit -m "feat: Claude AI comment analyzer for qualitative insights"
```

---

### Task 7: Output Writer (Google Sheets)

**Files:**
- Create: `src/output_writer.py`
- Create: `tests/test_output_writer.py`

Writes two sheets into a target spreadsheet in the "Aggregated Info" Drive folder:
1. **"Statistics"** — the full aggregated task table
2. **"Insights"** — the Claude-generated analysis in markdown-like rows

**Step 1: Write the failing tests**

```python
# tests/test_output_writer.py
import pandas as pd
import pytest
from unittest.mock import MagicMock, call
from src.output_writer import OutputWriter


@pytest.fixture
def mock_creds():
    return MagicMock()


@pytest.fixture
def writer(mock_creds):
    return OutputWriter(mock_creds)


def _attach_mock_service(writer):
    mock_service = MagicMock()
    writer._service = mock_service
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Sheet1", "sheetId": 0}}]
    }
    mock_service.spreadsheets().batchUpdate().execute.return_value = {}
    mock_service.spreadsheets().values().update().execute.return_value = {}
    return mock_service


def test_write_statistics_calls_update_with_dataframe_values(writer):
    """write_statistics pushes DataFrame rows to the target sheet."""
    mock_service = _attach_mock_service(writer)

    df = pd.DataFrame({
        "task": ["Task A"],
        "estimated_hours": [8.0],
        "actual_hours": [4.0],
        "hours_saved": [4.0],
        "efficiency_ratio": [0.5],
        "engineer": ["Alice"],
        "date": ["2024-01-10"],
        "comments": [""],
    })

    writer.write_statistics("spreadsheet123", df, sheet_name="Statistics")

    mock_service.spreadsheets().values().update.assert_called()
    call_kwargs = mock_service.spreadsheets().values().update.call_args.kwargs
    assert call_kwargs["range"] == "Statistics!A1"
    assert call_kwargs["body"]["values"][0] == list(df.columns)


def test_write_insights_writes_markdown_rows(writer):
    """write_insights splits markdown text into rows and pushes to sheet."""
    mock_service = _attach_mock_service(writer)

    analysis = {"raw_analysis": "## Benefits\n- Fast\n## Pain Points\n- Slow", "comment_count": 5}
    writer.write_insights("spreadsheet123", analysis, sheet_name="Insights")

    mock_service.spreadsheets().values().update.assert_called()
```

**Step 2: Run to verify failure**

```bash
pytest tests/test_output_writer.py -v
```
Expected: FAIL — `ImportError`

**Step 3: Implement `src/output_writer.py`**

```python
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from datetime import datetime


class OutputWriter:
    def __init__(self, creds: Credentials):
        self._service = build("sheets", "v4", credentials=creds)

    def _ensure_sheet_exists(self, spreadsheet_id: str, sheet_name: str) -> None:
        """Create the named sheet tab if it doesn't already exist."""
        meta = self._service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        existing = [s["properties"]["title"] for s in meta.get("sheets", [])]
        if sheet_name not in existing:
            self._service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
            ).execute()

    def _clear_and_write(self, spreadsheet_id: str, sheet_name: str, values: list[list]) -> None:
        self._ensure_sheet_exists(spreadsheet_id, sheet_name)
        self._service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1:ZZ100000",
        ).execute()
        self._service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()

    def write_statistics(
        self,
        spreadsheet_id: str,
        df: pd.DataFrame,
        sheet_name: str = "Statistics",
    ) -> None:
        """Write the aggregated statistics DataFrame to a sheet tab."""
        df = df.fillna("")
        header = list(df.columns)
        rows = df.values.tolist()
        rows = [[str(v) if v != "" else "" for v in row] for row in rows]
        self._clear_and_write(spreadsheet_id, sheet_name, [header] + rows)

    def write_summary_row(
        self,
        spreadsheet_id: str,
        summary: dict,
        sheet_name: str = "Summary",
    ) -> None:
        """Write key-value summary stats to a dedicated tab."""
        generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        rows = [
            ["Metric", "Value"],
            ["Generated at", generated_at],
            ["Total tasks", summary.get("total_tasks", "")],
            ["Total estimated hours", summary.get("total_estimated_hours", "")],
            ["Total actual hours", summary.get("total_actual_hours", "")],
            ["Total hours saved", summary.get("total_hours_saved", "")],
            ["Overall efficiency ratio", summary.get("overall_efficiency_ratio", "")],
            ["Engineers", ", ".join(summary.get("engineers", []))],
        ]
        self._clear_and_write(spreadsheet_id, sheet_name, rows)

    def write_insights(
        self,
        spreadsheet_id: str,
        analysis: dict,
        sheet_name: str = "Insights",
    ) -> None:
        """Write the Claude-generated insights markdown as rows."""
        raw = analysis.get("raw_analysis", "")
        comment_count = analysis.get("comment_count", 0)
        generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        header_rows = [
            [f"CC Statistics — AI-Generated Insights"],
            [f"Generated at: {generated_at}"],
            [f"Based on {comment_count} engineer comments"],
            [""],
        ]
        content_rows = [[line] for line in raw.split("\n")]
        self._clear_and_write(spreadsheet_id, sheet_name, header_rows + content_rows)
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_output_writer.py -v
```
Expected: PASS (2 tests)

**Step 5: Commit**

```bash
git add src/output_writer.py tests/test_output_writer.py
git commit -m "feat: output writer for statistics and insights sheets"
```

---

### Task 8: Drive File Creator

**Files:**
- Modify: `src/drive_client.py`
- Modify: `tests/test_drive_client.py`

We need to be able to create a new Google Sheet (or find an existing one by name) in the "Aggregated Info" subfolder.

**Step 1: Add failing tests to `tests/test_drive_client.py`**

```python
def test_get_or_create_sheet_returns_existing_id(client):
    """Returns existing sheet ID if a sheet with that name already exists."""
    mock_service = MagicMock()
    client._service = mock_service
    mock_service.files().list().execute.return_value = {
        "files": [{"id": "existing123", "name": "CC Statistics Aggregated"}]
    }

    result = client.get_or_create_sheet("folder456", "CC Statistics Aggregated")
    assert result == "existing123"
    # Should NOT call files().create()
    mock_service.files().create.assert_not_called()


def test_get_or_create_sheet_creates_new_when_absent(client):
    """Creates a new Google Sheet when none exists with that name."""
    mock_service = MagicMock()
    client._service = mock_service
    mock_service.files().list().execute.return_value = {"files": []}
    mock_service.files().create().execute.return_value = {"id": "new789"}

    result = client.get_or_create_sheet("folder456", "CC Statistics Aggregated")
    assert result == "new789"
    mock_service.files().create.assert_called()
```

**Step 2: Run to verify failure**

```bash
pytest tests/test_drive_client.py -v
```
Expected: FAIL on the 2 new tests

**Step 3: Add `get_or_create_sheet` to `src/drive_client.py`**

```python
    def get_or_create_sheet(self, folder_id: str, sheet_name: str) -> str:
        """Return the spreadsheet ID for a named sheet in folder, creating it if absent."""
        query = (
            f"'{folder_id}' in parents"
            " and mimeType='application/vnd.google-apps.spreadsheet'"
            f" and name='{sheet_name}'"
            " and trashed=false"
        )
        response = (
            self._service.files()
            .list(q=query, fields="files(id, name)")
            .execute()
        )
        files = response.get("files", [])
        if files:
            return files[0]["id"]

        # Create new spreadsheet in the target folder
        metadata = {
            "name": sheet_name,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id],
        }
        created = self._service.files().create(body=metadata, fields="id").execute()
        return created["id"]
```

**Step 4: Run all drive tests to verify pass**

```bash
pytest tests/test_drive_client.py -v
```
Expected: PASS (5 tests)

**Step 5: Commit**

```bash
git add src/drive_client.py tests/test_drive_client.py
git commit -m "feat: drive client get-or-create sheet in folder"
```

---

### Task 9: Main Entry Point

**Files:**
- Create: `src/main.py`
- Create: `tests/test_main.py`

**Step 1: Write the failing integration test**

```python
# tests/test_main.py
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd


def test_run_pipeline_end_to_end():
    """run_pipeline calls all modules in correct order and writes output."""
    mock_df = pd.DataFrame({
        "task": ["Task A"],
        "estimated_hours": [8.0],
        "actual_hours": [4.0],
        "engineer": ["Alice"],
        "date": ["2024-01-10"],
        "comments": ["Great!"],
        "source_sheet": ["Sheet1"],
    })

    with patch("src.main.get_google_credentials") as mock_auth, \
         patch("src.main.DriveClient") as MockDrive, \
         patch("src.main.SheetsReader") as MockReader, \
         patch("src.main.aggregate_stats") as mock_agg, \
         patch("src.main.analyze_comments") as mock_analyze, \
         patch("src.main.OutputWriter") as MockWriter:

        mock_auth.return_value = MagicMock()

        drive_instance = MockDrive.return_value
        drive_instance.list_sheets_in_folder.return_value = [
            {"id": "sheet1", "name": "Alice Stats"}
        ]
        drive_instance.find_subfolder.return_value = "subfolder123"
        drive_instance.get_or_create_sheet.return_value = "output_sheet_id"

        reader_instance = MockReader.return_value
        reader_instance.read_sheet.return_value = mock_df

        mock_agg.return_value = {
            "all_tasks": mock_df,
            "summary": {"total_tasks": 1},
            "comments": [{"text": "Great!", "engineer": "Alice", "task": "Task A", "date": ""}],
        }
        mock_analyze.return_value = {"raw_analysis": "## Benefits\n- Good", "comment_count": 1}

        writer_instance = MockWriter.return_value

        from src.main import run_pipeline
        run_pipeline(
            folder_id="folder123",
            aggregated_folder_name="Aggregated Info",
            output_sheet_name="CC Statistics Aggregated",
        )

        mock_agg.assert_called_once()
        mock_analyze.assert_called_once()
        writer_instance.write_statistics.assert_called_once()
        writer_instance.write_summary_row.assert_called_once()
        writer_instance.write_insights.assert_called_once()
```

**Step 2: Run to verify failure**

```bash
pytest tests/test_main.py -v
```
Expected: FAIL — `ImportError`

**Step 3: Implement `src/main.py`**

```python
import sys
from src.config import (
    GOOGLE_CREDENTIALS_FILE,
    GOOGLE_TOKEN_FILE,
    DRIVE_FOLDER_ID,
    AGGREGATED_FOLDER_NAME,
    ANTHROPIC_API_KEY,
    CLAUDE_MODEL,
)
from src.auth import get_google_credentials
from src.drive_client import DriveClient
from src.sheets_reader import SheetsReader
from src.aggregator import aggregate_stats
from src.comment_analyzer import analyze_comments
from src.output_writer import OutputWriter


OUTPUT_SHEET_NAME = "CC Statistics Aggregated"


def run_pipeline(
    folder_id: str,
    aggregated_folder_name: str = "Aggregated Info",
    output_sheet_name: str = OUTPUT_SHEET_NAME,
    credentials_file: str = GOOGLE_CREDENTIALS_FILE,
    token_file: str = GOOGLE_TOKEN_FILE,
    anthropic_api_key: str = ANTHROPIC_API_KEY,
    claude_model: str = CLAUDE_MODEL,
) -> None:
    print("Authenticating with Google...")
    creds = get_google_credentials(credentials_file, token_file)

    drive = DriveClient(creds)
    reader = SheetsReader(creds)
    writer = OutputWriter(creds)

    print(f"Listing engineer sheets in folder {folder_id}...")
    sheets = drive.list_sheets_in_folder(folder_id)
    print(f"Found {len(sheets)} engineer sheet(s).")

    frames = []
    for sheet in sheets:
        print(f"  Reading: {sheet['name']} ({sheet['id']})...")
        try:
            df = reader.read_sheet(sheet["id"], engineer_name=sheet["name"])
            frames.append(df)
            print(f"    -> {len(df)} task rows loaded.")
        except ValueError as e:
            print(f"    -> Skipped: {e}")

    if not frames:
        print("No data found. Exiting.")
        return

    print("Aggregating statistics...")
    result = aggregate_stats(frames)
    summary = result["summary"]
    print(f"  Total tasks: {summary['total_tasks']}")
    print(f"  Total estimated hours: {summary['total_estimated_hours']}")
    print(f"  Total actual hours: {summary['total_actual_hours']}")
    print(f"  Total hours saved: {summary['total_hours_saved']}")

    print(f"Analyzing {len(result['comments'])} comments with Claude AI...")
    analysis = analyze_comments(result["comments"], api_key=anthropic_api_key, model=claude_model)

    print(f"Finding '{aggregated_folder_name}' subfolder...")
    subfolder_id = drive.find_subfolder(folder_id, aggregated_folder_name)

    print(f"Getting or creating output sheet '{output_sheet_name}'...")
    output_sheet_id = drive.get_or_create_sheet(subfolder_id, output_sheet_name)
    print(f"  Sheet ID: {output_sheet_id}")

    print("Writing Statistics tab...")
    writer.write_statistics(output_sheet_id, result["all_tasks"])

    print("Writing Summary tab...")
    writer.write_summary_row(output_sheet_id, summary)

    print("Writing Insights tab...")
    writer.write_insights(output_sheet_id, analysis)

    print("\nDone! Output written to Google Drive.")
    print(f"https://docs.google.com/spreadsheets/d/{output_sheet_id}")


if __name__ == "__main__":
    if not DRIVE_FOLDER_ID:
        print("ERROR: DRIVE_FOLDER_ID is not set in .env file.")
        sys.exit(1)
    run_pipeline(folder_id=DRIVE_FOLDER_ID)
```

**Step 4: Run all tests**

```bash
pytest -v
```
Expected: PASS (all tests across all modules)

**Step 5: Commit**

```bash
git add src/main.py tests/test_main.py
git commit -m "feat: main pipeline entry point wiring all modules"
```

---

### Task 10: End-to-End Setup Verification

This task has no automated test — it's about setting up real credentials and doing a live smoke test.

**Step 1: Create a Google Cloud project and enable APIs**

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create a new project (or use existing)
3. Enable **Google Drive API** and **Google Sheets API**
4. Go to **Credentials** → **Create Credentials** → **OAuth client ID**
5. Application type: **Desktop app**
6. Download the JSON as `credentials.json` and place it in the project root

**Step 2: Copy `.env.example` to `.env` and fill in values**

```bash
cp .env.example .env
```

Edit `.env`:
- Set `DRIVE_FOLDER_ID` to the ID from your Drive folder URL
- Set `ANTHROPIC_API_KEY` to your Anthropic key
- Leave other values as defaults

**Step 3: Run the pipeline**

```bash
python -m src.main
```

On first run, a browser window will open for Google OAuth. Authorize the app.

Expected output:
```
Authenticating with Google...
Listing engineer sheets in folder <id>...
Found N engineer sheet(s).
  Reading: Alice Stats (<id>)...
    -> M task rows loaded.
...
Done! Output written to Google Drive.
https://docs.google.com/spreadsheets/d/<output_id>
```

**Step 4: Verify output in Google Drive**

Open the link printed. Check that:
- "Statistics" tab has all task rows with correct columns
- "Summary" tab has the totals
- "Insights" tab has Claude's analysis

**Step 5: Final commit**

```bash
git add .env.example README.md  # do NOT commit .env
git commit -m "chore: add setup instructions and final smoke test"
```

---

## Running All Tests

```bash
pytest -v
```

All 16+ tests should pass without any real API calls (everything is mocked).

## File Tree at Completion

```
CCStatistics/
├── docs/
│   └── plans/
│       └── 2026-03-20-cc-statistics-aggregator.md
├── src/
│   ├── __init__.py
│   ├── auth.py
│   ├── config.py
│   ├── drive_client.py
│   ├── sheets_reader.py
│   ├── aggregator.py
│   ├── comment_analyzer.py
│   ├── output_writer.py
│   └── main.py
├── tests/
│   ├── __init__.py
│   ├── test_auth.py
│   ├── test_drive_client.py
│   ├── test_sheets_reader.py
│   ├── test_aggregator.py
│   ├── test_comment_analyzer.py
│   ├── test_output_writer.py
│   └── test_main.py
├── .env              (not committed)
├── .env.example
├── .gitignore
├── pyproject.toml
└── requirements.txt
```
