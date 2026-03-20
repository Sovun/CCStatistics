import os
import re
from dotenv import load_dotenv

load_dotenv()

def _extract_folder_id(value: str) -> str:
    """Extract just the folder ID from a full Drive URL or return as-is if already an ID."""
    match = re.search(r'/folders/([a-zA-Z0-9_-]+)', value)
    if match:
        return match.group(1)
    return value.strip()

GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
GOOGLE_TOKEN_FILE = os.getenv("GOOGLE_TOKEN_FILE", "token.json")
DRIVE_FOLDER_ID = _extract_folder_id(os.getenv("DRIVE_FOLDER_ID", ""))
AGGREGATED_FOLDER_NAME = os.getenv("AGGREGATED_FOLDER_NAME", "Aggregated Info")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")

# Synonyms for canonical column names.
# The reader will match sheet headers (case-insensitive) against these lists.
COLUMN_SYNONYMS = {
    "task": ["task", "task name", "name"],
    "task_description": ["task description", "description", "details", "feature", "story", "ticket"],
    "estimated_hours": [
        "estimated hours", "estimate", "estimated", "without ai", "no ai",
        "manual estimate", "planned hours", "original estimate", "non-ai estimate",
    ],
    "actual_hours": [
        "actual hours", "actual", "with claude", "with ai", "claude hours",
        "real hours", "time spent", "hours spent",
    ],
    "deviation": [
        "deviation", "ratio", "actual/estimate", "efficiency", "ai ratio",
        "time ratio", "deviation (%)", "deviation(%)",
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
