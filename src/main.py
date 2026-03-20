import sys
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
from src.comment_analyzer import analyze_comments
from src.output_writer import OutputWriter


OUTPUT_SHEET_NAME = "CC Statistics Aggregated"


def run_pipeline(
    folder_id: str,
    aggregated_folder_name: str = AGGREGATED_FOLDER_NAME,
    output_sheet_name: str = OUTPUT_SHEET_NAME,
    credentials_file: str = GOOGLE_CREDENTIALS_FILE,
    token_file: str = GOOGLE_TOKEN_FILE,
    anthropic_api_key: str = ANTHROPIC_API_KEY,
    claude_model: str = CLAUDE_MODEL,
) -> None:
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
        except ValueError as e:
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

    print(f"Getting or creating output sheet '{output_sheet_name}'...")
    output_sheet_id = drive.get_or_create_sheet(subfolder_id, output_sheet_name)

    print("Writing Statistics tab...")
    writer.write_statistics(output_sheet_id, result["all_tasks"])

    print("Writing Summary tab...")
    writer.write_summary_row(output_sheet_id, summary)

    print("Writing Insights tab...")
    writer.write_insights(output_sheet_id, analysis)

    print("\nDone!")
    print(f"https://docs.google.com/spreadsheets/d/{output_sheet_id}")


if __name__ == "__main__":
    if not DRIVE_FOLDER_ID:
        print("ERROR: DRIVE_FOLDER_ID is not set in .env file.")
        print("Copy .env.example to .env and fill in the required values.")
        sys.exit(1)
    run_pipeline(folder_id=DRIVE_FOLDER_ID)
