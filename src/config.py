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
    "task": [
        "task", "task name",
    ],
    "task_description": [
        "task description", "description", "details", "feature", "story", "ticket",
    ],
    "estimated_hours": [
        # Actual header used in sheets (matched case-insensitively):
        "estimate task via standard flow (without ai)",
        "estimate task via standard flow (without ai) ",  # trailing space variant
        # Common shorter variants engineers may use:
        "estimated hours", "estimated (without ai)", "estimate without ai",
        "estimate", "estimated", "without ai", "no ai",
        "manual estimate", "planned hours", "original estimate", "non-ai estimate",
        "standard flow estimate", "standard estimate",
    ],
    "actual_hours": [
        # Actual header used in sheets:
        "actual time spent with ai",
        "actual time spent with ai ",  # trailing space variant
        # Common shorter variants:
        "actual hours", "actual time", "actual", "time spent with ai",
        "with claude", "with ai", "claude hours",
        "real hours", "time spent", "hours spent", "ai time",
    ],
    "deviation": [
        "deviation", "ratio", "actual/estimate", "efficiency", "ai ratio",
        "time ratio", "deviation (%)", "deviation(%)", "ai deviation",
    ],
    "date": ["date", "week", "sprint", "period", "created", "completed"],
    "engineer": ["engineer", "author", "person", "dev", "developer"],
    "comments": [
        "comments", "comment", "notes", "note", "feedback", "observations",
        "what was good", "what was bad", "remarks", "review",
    ],
}

# Required canonical columns (rows missing all of these are dropped)
REQUIRED_COLUMNS = {"task", "estimated_hours", "actual_hours"}


def validate_config() -> None:
    """Raise EnvironmentError if required config values are missing."""
    missing = []
    if not DRIVE_FOLDER_ID:
        missing.append("DRIVE_FOLDER_ID")
    if not ANTHROPIC_API_KEY:
        missing.append("ANTHROPIC_API_KEY")
    if missing:
        raise EnvironmentError(
            f"Required environment variables not set: {', '.join(missing)}. "
            "Copy .env.example to .env and fill in the values."
        )
