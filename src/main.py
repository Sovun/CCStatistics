import sys
import os

# Re-exec using the project venv's Python if we're running outside it.
_venv_python = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".venv", "bin", "python3")
)
if os.path.exists(_venv_python) and os.path.abspath(sys.executable) != _venv_python:
    os.execv(_venv_python, [_venv_python, "-m", "src.main"] + sys.argv[1:])

from datetime import datetime, timezone, date, timedelta
from googleapiclient.errors import HttpError
from src.config import (
    GOOGLE_CREDENTIALS_FILE,
    GOOGLE_TOKEN_FILE,
    DRIVE_FOLDER_ID,
    AGGREGATED_FOLDER_NAME,
    ANTHROPIC_API_KEY,
    CLAUDE_MODEL,
    validate_config,
)
from src.auth import get_google_credentials
from src.drive_client import DriveClient
from src.sheets_reader import SheetsReader
from src.aggregator import aggregate_stats
from src.comment_analyzer import analyze_comments, pick_sprint_winner
from src.output_writer import OutputWriter


def _get_sprint_tasks(df) -> list[dict]:
    """Return tasks from the current week that have a task description.

    Tries to parse the 'date' column to filter to Mon-Sun of the current week.
    Falls back to all tasks with descriptions if no dates can be parsed or no
    weekly matches are found.
    """
    import pandas as pd

    if df is None or df.empty or "task_description" not in df.columns:
        return []

    has_desc = df["task_description"].astype(str).str.strip().ne("").fillna(False)
    candidates = df[has_desc].copy()
    if candidates.empty:
        return []

    # Attempt current-week filtering
    if "date" in candidates.columns:
        today = date.today()
        week_start = today - timedelta(days=today.weekday())  # Monday
        week_end = week_start + timedelta(days=6)             # Sunday

        _DATE_FMTS = ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d.%m.%Y", "%Y/%m/%d", "%d-%m-%Y")

        def _parse(s: str) -> "date | None":
            for fmt in _DATE_FMTS:
                try:
                    return datetime.strptime(str(s).strip(), fmt).date()
                except ValueError:
                    pass
            return None

        parsed = candidates["date"].apply(_parse)
        in_week = parsed.apply(lambda d: d is not None and week_start <= d <= week_end)
        weekly = candidates[in_week]
        if not weekly.empty:
            candidates = weekly

    fields = ["task", "task_description", "engineer", "date", "hours_saved", "comments"]
    available = [f for f in fields if f in candidates.columns]
    return candidates[available].fillna("").astype(str).to_dict("records")


def _output_sheet_name() -> str:
    date_str = datetime.now(timezone.utc).strftime("%Y.%m.%d")
    return f"{date_str} CC Statistics Aggregated"


def run_pipeline(
    folder_id: str,
    aggregated_folder_name: str = AGGREGATED_FOLDER_NAME,
    output_sheet_name: str = None,
    credentials_file: str = GOOGLE_CREDENTIALS_FILE,
    token_file: str = GOOGLE_TOKEN_FILE,
    anthropic_api_key: str = ANTHROPIC_API_KEY,
    claude_model: str = CLAUDE_MODEL,
) -> None:
    if output_sheet_name is None:
        output_sheet_name = _output_sheet_name()
    validate_config()

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
        except (ValueError, HttpError, Exception) as e:
            print(f"    -> Skipped: {e}")

    print("Aggregating statistics...")
    result = aggregate_stats(frames)
    summary = result["summary"]
    if summary:
        print(f"  Total tasks:            {summary.get('total_tasks', 0)}")
        print(f"  Total estimated hours:  {summary.get('total_estimated_hours', 0)}")
        print(f"  Total actual hours:     {summary.get('total_actual_hours', 0)}")
        print(f"  Total hours saved:      {summary.get('total_hours_saved', 0)}")

    comments = result["comments"]

    print(f"Finding '{aggregated_folder_name}' subfolder...")
    try:
        subfolder_id = drive.find_subfolder(folder_id, aggregated_folder_name)
    except ValueError as e:
        print(f"\nERROR: {e}")
        print(
            f"Please ensure the '{aggregated_folder_name}' subfolder exists "
            f"inside the Drive folder {folder_id}."
        )
        sys.exit(1)

    print(f"Analyzing {len(comments)} comment(s) with Claude AI...")
    try:
        analysis = analyze_comments(
            comments, api_key=anthropic_api_key, model=claude_model
        )
    except (RuntimeError, ValueError) as e:
        print(f"  WARNING: Comment analysis failed: {e}")
        print("  Insights tab will be skipped.")
        analysis = {"raw_analysis": "", "comment_count": len(comments)}

    sprint_tasks = _get_sprint_tasks(result["all_tasks"])
    print(f"Selecting 'Claude Code win of the sprint' from {len(sprint_tasks)} eligible task(s)...")
    try:
        sprint_winner = pick_sprint_winner(
            sprint_tasks, api_key=anthropic_api_key, model=claude_model
        )
        if sprint_winner:
            print(f"  Winner: \"{sprint_winner['task']}\" by {sprint_winner['engineer']}")
            print(f"  Cost: ~${sprint_winner['cost_usd']:.4f}")
        else:
            print("  No eligible tasks found — sprint winner section will be skipped.")
    except (RuntimeError, ValueError) as e:
        print(f"  WARNING: Sprint winner selection failed: {e}")
        sprint_winner = None

    analysis["sprint_winner"] = sprint_winner

    print(f"Getting or creating output sheet '{output_sheet_name}'...")
    output_sheet_id = drive.get_or_create_sheet(subfolder_id, output_sheet_name)

    print("Writing Statistics tab...")
    writer.write_statistics(output_sheet_id, result["all_tasks"])

    print("Writing Summary tab...")
    writer.write_summary_row(output_sheet_id, summary)

    print("Writing Insights tab...")
    writer.write_insights(output_sheet_id, analysis)

    print("Removing default Sheet1 if present...")
    writer.delete_sheet_if_exists(output_sheet_id, "Sheet1")

    print("\nDone!")
    print(f"https://docs.google.com/spreadsheets/d/{output_sheet_id}")


if __name__ == "__main__":
    if not DRIVE_FOLDER_ID:
        print("ERROR: DRIVE_FOLDER_ID is not set in .env file.")
        print("Copy .env.example to .env and fill in the required values.")
        sys.exit(1)
    run_pipeline(folder_id=DRIVE_FOLDER_ID)
