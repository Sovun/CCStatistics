import pandas as pd
from datetime import datetime, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials


class OutputWriter:
    def __init__(self, creds: Credentials):
        self._service = build("sheets", "v4", credentials=creds)

    @staticmethod
    def _quote_sheet_name(name: str) -> str:
        """Wrap sheet name in single quotes for A1 notation, escaping internal quotes."""
        return "'" + name.replace("'", "''") + "'"

    def _ensure_sheet_exists(self, spreadsheet_id: str, sheet_name: str) -> None:
        """Create the named sheet tab if it does not already exist."""
        meta = self._service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        existing = [s["properties"]["title"] for s in meta.get("sheets", [])]
        if sheet_name not in existing:
            self._service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
            ).execute()

    def _clear_and_write(
        self, spreadsheet_id: str, sheet_name: str, values: list[list]
    ) -> None:
        self._ensure_sheet_exists(spreadsheet_id, sheet_name)
        quoted = self._quote_sheet_name(sheet_name)
        try:
            self._service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"{quoted}!A1:ZZ100000",
            ).execute()
            self._service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{quoted}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": values},
            ).execute()
        except HttpError as e:
            raise RuntimeError(
                f"Failed to write to sheet '{sheet_name}' in spreadsheet {spreadsheet_id}: {e}"
            ) from e

    def write_statistics(
        self,
        spreadsheet_id: str,
        df: pd.DataFrame,
        sheet_name: str = "Statistics",
    ) -> None:
        """Write the aggregated statistics DataFrame to a sheet tab."""
        df = df.fillna("")
        header = list(df.columns)
        rows = [[str(v) if v != "" else "" for v in row] for row in df.values.tolist()]
        self._clear_and_write(spreadsheet_id, sheet_name, [header] + rows)

    def write_summary_row(
        self,
        spreadsheet_id: str,
        summary: dict,
        sheet_name: str = "Summary",
    ) -> None:
        """Write key-value summary stats to a dedicated tab."""
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        engineers = summary.get("engineers", [])
        if isinstance(engineers, list):
            engineers_str = ", ".join(str(e) for e in engineers)
        else:
            engineers_str = str(engineers) if engineers is not None else ""
        rows = [
            ["Metric", "Value"],
            ["Generated at", generated_at],
            ["total_tasks", summary.get("total_tasks", "")],
            ["total_estimated_hours", summary.get("total_estimated_hours", "")],
            ["total_actual_hours", summary.get("total_actual_hours", "")],
            ["total_hours_saved", summary.get("total_hours_saved", "")],
            ["overall_efficiency_ratio", summary.get("overall_efficiency_ratio", "")],
            ["engineers", engineers_str],
        ]
        self._clear_and_write(spreadsheet_id, sheet_name, rows)

    def write_insights(
        self,
        spreadsheet_id: str,
        analysis: dict,
        sheet_name: str = "Insights",
    ) -> None:
        """Write Claude-generated insights markdown as rows."""
        raw = analysis.get("raw_analysis", "")
        comment_count = analysis.get("comment_count", 0)
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        header_rows = [
            ["CC Statistics — AI-Generated Insights"],
            [f"Generated at: {generated_at}"],
            [f"Based on {comment_count} engineer comments"],
            [""],
        ]
        content_rows = [[line] for line in raw.split("\n")]
        self._clear_and_write(spreadsheet_id, sheet_name, header_rows + content_rows)
