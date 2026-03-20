import pandas as pd
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from src.config import COLUMN_SYNONYMS, REQUIRED_COLUMNS


class SheetsReader:
    def __init__(self, creds: Credentials):
        self._service = build("sheets", "v4", credentials=creds)

    def _get_sheet_tabs(self, spreadsheet_id: str) -> list:
        meta = self._service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        return [s["properties"]["title"] for s in meta.get("sheets", [])]

    def _map_headers(self, headers: list) -> dict:
        """Map raw header strings to canonical column names using COLUMN_SYNONYMS."""
        mapping: dict = {}
        assigned_canonical: set = set()
        for raw in headers:
            normalized = raw.strip().lower()
            for canonical, synonyms in COLUMN_SYNONYMS.items():
                if normalized in synonyms and canonical not in assigned_canonical:
                    mapping[raw] = canonical
                    assigned_canonical.add(canonical)
                    break
        return mapping

    def _read_tab_values(self, spreadsheet_id: str, tab: str) -> list[list]:
        safe_range = "'" + tab.replace("'", "''") + "'"
        result = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=safe_range)
            .execute()
        )
        return result.get("values", [])

    def read_sheet(self, spreadsheet_id: str, engineer_name: str) -> pd.DataFrame:
        """Read the first recognizable tab and return a normalized DataFrame.

        Canonical columns: task, task_description, estimated_hours, actual_hours,
                           deviation, date, comments, engineer, source_sheet
        """
        tabs = self._get_sheet_tabs(spreadsheet_id)

        for tab in tabs:
            rows = self._read_tab_values(spreadsheet_id, tab)
            if len(rows) < 2:
                continue

            headers = rows[0]
            mapping = self._map_headers(headers)
            mapped_canonical = set(mapping.values())

            if not REQUIRED_COLUMNS.issubset(mapped_canonical):
                continue

            # Pad short rows to header length and truncate rows wider than header
            n_cols = len(headers)
            data_rows = [(r + [""] * n_cols)[:n_cols] for r in rows[1:]]

            df = pd.DataFrame(data_rows, columns=headers)
            df = df.rename(columns=mapping)

            # Keep only recognized canonical columns
            canonical_cols = [c for c in df.columns if c in COLUMN_SYNONYMS]
            df = df[canonical_cols].copy()

            # Coerce numeric columns
            for col in ("estimated_hours", "actual_hours", "deviation"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Drop rows with empty task name
            df = df[df["task"].str.strip().ne("")]
            df = df.dropna(subset=["task"])
            df = df.reset_index(drop=True)

            df["engineer"] = engineer_name
            df["source_sheet"] = tab
            return df

        raise ValueError(
            f"Could not find required columns {REQUIRED_COLUMNS} in any tab of "
            f"spreadsheet {spreadsheet_id}"
        )
