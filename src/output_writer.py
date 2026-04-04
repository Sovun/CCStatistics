import re
import pandas as pd
from datetime import datetime, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials

# Preferred column order for the Statistics tab.
# Columns not in this list are appended at the end (except source_sheet which is dropped).
_STATS_COLUMN_ORDER = [
    "date", "engineer", "task", "task_description",
    "estimated_hours", "actual_hours", "deviation", "hours_saved", "comments",
]

# Pixel widths for each column in the Statistics tab
_STATS_COLUMN_WIDTHS = {
    "date": 95,
    "engineer": 140,
    "task": 180,
    "task_description": 220,
    "estimated_hours": 110,
    "actual_hours": 110,
    "deviation": 90,
    "hours_saved": 100,
    "comments": 320,
}

# RGB color helpers
_NAVY = {"red": 0.12, "green": 0.22, "blue": 0.40}
_BLUE = {"red": 0.22, "green": 0.46, "blue": 0.69}
_LIGHT_BLUE_TINT = {"red": 0.94, "green": 0.96, "blue": 0.99}
_LIGHT_GRAY = {"red": 0.93, "green": 0.93, "blue": 0.93}
_VERY_LIGHT_GRAY = {"red": 0.96, "green": 0.96, "blue": 0.96}
_WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}
_DARK_GRAY = {"red": 0.2, "green": 0.2, "blue": 0.2}
_MED_GRAY = {"red": 0.45, "green": 0.45, "blue": 0.45}


class OutputWriter:
    def __init__(self, creds: Credentials):
        self._service = build("sheets", "v4", credentials=creds)

    @staticmethod
    def _quote_sheet_name(name: str) -> str:
        """Wrap sheet name in single quotes for A1 notation, escaping internal quotes."""
        return "'" + name.replace("'", "''") + "'"

    def _get_sheet_id(self, spreadsheet_id: str, sheet_name: str) -> "int | None":
        """Return the numeric sheetId for a named tab, or None if not found."""
        meta = self._service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for s in meta.get("sheets", []):
            if s["properties"]["title"] == sheet_name:
                return s["properties"]["sheetId"]
        return None

    def _delete_bandings_on_sheet(self, spreadsheet_id: str, sheet_id: int) -> None:
        """Remove all existing banded ranges on a sheet (needed before re-adding banding)."""
        meta = self._service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        delete_requests = []
        for s in meta.get("sheets", []):
            if s["properties"]["sheetId"] == sheet_id:
                for br in s.get("bandedRanges", []):
                    delete_requests.append(
                        {"deleteBanding": {"bandedRangeId": br["bandedRangeId"]}}
                    )
        if delete_requests:
            self._service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body={"requests": delete_requests}
            ).execute()

    def _ensure_sheet_exists(self, spreadsheet_id: str, sheet_name: str) -> None:
        """Create the named sheet tab if it does not already exist."""
        if self._get_sheet_id(spreadsheet_id, sheet_name) is None:
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

    # -------------------------------------------------------------------------
    # Statistics tab
    # -------------------------------------------------------------------------

    def write_statistics(
        self,
        spreadsheet_id: str,
        df: pd.DataFrame,
        sheet_name: str = "Statistics",
    ) -> None:
        """Write aggregated statistics with formatting, ordered columns, and a totals row."""
        # 1. Reorder columns (drop source_sheet — not useful in output)
        df = df.drop(columns=["source_sheet"], errors="ignore")
        ordered = [c for c in _STATS_COLUMN_ORDER if c in df.columns]
        extras = [c for c in df.columns if c not in _STATS_COLUMN_ORDER]
        df = df[ordered + extras].copy()

        # 2. Format deviation as percentage string for display (e.g. 0.5 → "50%")
        if "deviation" in df.columns:
            df["deviation"] = df["deviation"].apply(
                lambda v: f"{v:.0%}" if pd.notna(v) and isinstance(v, float) else ""
            )

        df = df.fillna("")
        header = list(df.columns)
        rows = [[str(v) if v != "" else "" for v in row] for row in df.values.tolist()]

        # 3. Build totals row
        totals = ["TOTAL / AVERAGE"] + [""] * (len(header) - 1)
        for i, col in enumerate(header):
            if col in ("estimated_hours", "actual_hours", "hours_saved"):
                try:
                    vals = [float(r[i]) for r in rows if r[i] != ""]
                    totals[i] = str(round(sum(vals), 2)) if vals else ""
                except (ValueError, TypeError):
                    pass
            elif col == "deviation":
                try:
                    # Average of raw ratios (rows still have "50%" strings here,
                    # so parse them back)
                    vals = [
                        float(r[i].rstrip("%")) / 100
                        for r in rows
                        if r[i] not in ("", None)
                    ]
                    totals[i] = f"{sum(vals)/len(vals):.0%}" if vals else ""
                except (ValueError, TypeError):
                    pass

        # 4. Write data
        self._clear_and_write(spreadsheet_id, sheet_name, [header] + rows + [totals])

        # 5. Apply formatting
        sheet_id = self._get_sheet_id(spreadsheet_id, sheet_name)
        if sheet_id is not None:
            self._format_statistics(spreadsheet_id, sheet_id, len(rows), header)

    def _format_statistics(
        self,
        spreadsheet_id: str,
        sheet_id: int,
        n_data_rows: int,
        columns: list[str],
    ) -> None:
        self._delete_bandings_on_sheet(spreadsheet_id, sheet_id)
        totals_row = n_data_rows + 1  # 0-indexed: header=0, data=1..n, totals=n+1
        n_cols = len(columns)
        requests = [
            # Freeze header row
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
            # Header: blue background, white bold text, centered — only data columns
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": n_cols,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _BLUE,
                            "textFormat": {
                                "bold": True,
                                "foregroundColor": _WHITE,
                                "fontSize": 10,
                            },
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)",
                }
            },
            # Totals row: light gray background, bold text — only data columns
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": totals_row,
                        "endRowIndex": totals_row + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": n_cols,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _LIGHT_GRAY,
                            "textFormat": {"bold": True},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }
            },
            # Alternating row colors for data rows — only data columns
            {
                "addBanding": {
                    "bandedRange": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": totals_row,
                            "startColumnIndex": 0,
                            "endColumnIndex": n_cols,
                        },
                        "rowProperties": {
                            "firstBandColor": _WHITE,
                            "secondBandColor": _LIGHT_BLUE_TINT,
                        },
                    }
                }
            },
        ]

        # Column widths
        for i, col in enumerate(columns):
            width = _STATS_COLUMN_WIDTHS.get(col, 120)
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": i,
                            "endIndex": i + 1,
                        },
                        "properties": {"pixelSize": width},
                        "fields": "pixelSize",
                    }
                }
            )

        # Wrap text for comments and task_description columns
        for wrap_col in ("comments", "task_description"):
            if wrap_col in columns:
                idx = columns.index(wrap_col)
                requests.append(
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": 1,
                                "startColumnIndex": idx,
                                "endColumnIndex": idx + 1,
                            },
                            "cell": {
                                "userEnteredFormat": {"wrapStrategy": "WRAP"}
                            },
                            "fields": "userEnteredFormat.wrapStrategy",
                        }
                    }
                )

        self._service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()

    # -------------------------------------------------------------------------
    # Summary tab
    # -------------------------------------------------------------------------

    def write_summary_row(
        self,
        spreadsheet_id: str,
        summary: dict,
        sheet_name: str = "Summary",
    ) -> None:
        """Write key-value summary stats with section grouping and formatting."""
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        engineers = summary.get("engineers", [])
        engineers_str = ", ".join(str(e) for e in engineers) if isinstance(engineers, list) else str(engineers or "")

        def pct(v):
            return f"{v:.0%}" if isinstance(v, float) else str(v)

        tt = summary.get("total_tasks", 0)

        # Build rows as (label, value, row_type): "title" | "section" | "data"
        spec = [
            ("CC Statistics — Summary", "", "title"),
            ("OVERVIEW", "", "section"),
            ("Generated at", generated_at, "data"),
            ("Engineers with data", str(summary.get("engineers_with_data", len(engineers))), "data"),
            ("Total tasks analyzed", str(tt), "data"),
            ("TIME SAVINGS", "", "section"),
            ("Total estimated hours (without AI)", f"{summary.get('total_estimated_hours', '')} h", "data"),
            ("Total actual hours (with AI)", f"{summary.get('total_actual_hours', '')} h", "data"),
            ("Total hours saved", f"{summary.get('total_hours_saved', '')} h", "data"),
            ("Equivalent work days saved", f"{summary.get('equiv_days_saved', '')} days", "data"),
            ("Time savings", f"{summary.get('time_savings_pct', '')}%", "data"),
            ("Speed multiplier", f"{summary.get('speed_multiplier', '')}×  faster with AI", "data"),
            ("PER-TASK AVERAGES", "", "section"),
            ("Avg estimated per task (without AI)", f"{summary.get('avg_estimated_per_task', '')} h", "data"),
            ("Avg actual per task (with AI)", f"{summary.get('avg_actual_per_task', '')} h", "data"),
            ("Avg hours saved per task", f"{summary.get('avg_hours_saved_per_task', '')} h", "data"),
            ("AI EFFECTIVENESS", "", "section"),
            ("Overall deviation (actual / estimated)", pct(summary.get("overall_efficiency_ratio", "")), "data"),
            ("Tasks significantly faster (>25% saved)", f"{summary.get('tasks_significantly_faster', '')} of {tt}", "data"),
            ("Tasks with no AI benefit (actual ≥ estimated)", f"{summary.get('tasks_no_benefit', '')} of {tt}", "data"),
            ("ENGINEERS", "", "section"),
            ("Engineers", engineers_str, "data"),
        ]

        data_rows = [[label, value] for label, value, _ in spec]
        title_indices = [i for i, (_, _, t) in enumerate(spec) if t == "title"]
        section_indices = [i for i, (_, _, t) in enumerate(spec) if t == "section"]
        engineers_row_index = len(spec) - 1  # last row

        self._clear_and_write(spreadsheet_id, sheet_name, data_rows)

        sheet_id = self._get_sheet_id(spreadsheet_id, sheet_name)
        if sheet_id is not None:
            self._format_summary(spreadsheet_id, sheet_id, len(data_rows), title_indices, section_indices, engineers_row_index)

    def _format_summary(
        self,
        spreadsheet_id: str,
        sheet_id: int,
        n_rows: int,
        title_indices: list,
        section_indices: list,
        engineers_row_index: int,
    ) -> None:
        self._delete_bandings_on_sheet(spreadsheet_id, sheet_id)
        requests = [
            # Column A width: 310px
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                    "properties": {"pixelSize": 310},
                    "fields": "pixelSize",
                }
            },
            # Column B width: 380px
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
                    "properties": {"pixelSize": 380},
                    "fields": "pixelSize",
                }
            },
            # Reset columns C-G to default width (removes any 1px hidden columns from old runs)
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 7},
                    "properties": {"pixelSize": 100},
                    "fields": "pixelSize",
                }
            },
            # All rows: standard 21px height
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": n_rows},
                    "properties": {"pixelSize": 21},
                    "fields": "pixelSize",
                }
            },
            # All data rows: white background with left padding
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startColumnIndex": 0, "endColumnIndex": 2},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _WHITE,
                            "padding": {"left": 12},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,padding)",
                }
            },
            # Column A: bold metric labels
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startColumnIndex": 0, "endColumnIndex": 1},
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold",
                }
            },
            # Engineers cell: wrap text
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": engineers_row_index,
                        "endRowIndex": engineers_row_index + 1,
                        "startColumnIndex": 1,
                        "endColumnIndex": 2,
                    },
                    "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                    "fields": "userEnteredFormat.wrapStrategy",
                }
            },
        ]

        # Title rows — navy background, white bold 14pt, 46px, merge A-B
        for r in title_indices:
            requests += [
                {
                    "mergeCells": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": r, "endRowIndex": r + 1,
                            "startColumnIndex": 0, "endColumnIndex": 2,
                        },
                        "mergeType": "MERGE_ALL",
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": r, "endRowIndex": r + 1,
                            "startColumnIndex": 0, "endColumnIndex": 2,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": _NAVY,
                                "textFormat": {"bold": True, "foregroundColor": _WHITE, "fontSize": 14},
                                "verticalAlignment": "MIDDLE",
                                "padding": {"top": 6, "bottom": 6, "left": 14},
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment,padding)",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": r, "endIndex": r + 1},
                        "properties": {"pixelSize": 46},
                        "fields": "pixelSize",
                    }
                },
            ]

        # Section header rows — blue background, white bold 10pt, spans A-B
        for r in section_indices:
            requests += [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": r, "endRowIndex": r + 1,
                            "startColumnIndex": 0, "endColumnIndex": 2,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": _BLUE,
                                "textFormat": {"bold": True, "foregroundColor": _WHITE, "fontSize": 10},
                                "verticalAlignment": "MIDDLE",
                                "padding": {"top": 3, "bottom": 3, "left": 12},
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment,padding)",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": r, "endIndex": r + 1},
                        "properties": {"pixelSize": 26},
                        "fields": "pixelSize",
                    }
                },
            ]

        self._service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()

    # -------------------------------------------------------------------------
    # Insights tab
    # -------------------------------------------------------------------------

    @staticmethod
    def _parse_markdown_runs(text: str):
        """Parse **bold** and *italic* markdown into (plain_text, textFormatRuns or None).

        textFormatRuns format follows the Sheets API spec: each run applies from its
        startIndex to the startIndex of the next run (or end of string).
        """
        pattern = re.compile(r'\*\*(.+?)\*\*|\*([^*]+)\*')
        segments = []
        last = 0
        for m in pattern.finditer(text):
            if m.start() > last:
                segments.append((text[last:m.start()], False, False))
            if m.group(1) is not None:  # **bold**
                segments.append((m.group(1), True, False))
            else:  # *italic*
                segments.append((m.group(2), False, True))
            last = m.end()
        if last < len(text):
            segments.append((text[last:], False, False))

        if not segments or not any(b or i for _, b, i in segments):
            return text, None

        plain = ""
        runs = []
        for seg_text, bold, italic in segments:
            if not seg_text:
                continue
            fmt = {}
            if bold:
                fmt["bold"] = True
            if italic:
                fmt["italic"] = True
            runs.append({"startIndex": len(plain), "format": fmt})
            plain += seg_text

        return plain, runs if runs else None

    @staticmethod
    def _extract_conclusion(raw: str) -> "tuple[str, str]":
        """Extract the trailing conclusion paragraph from Claude's analysis output.

        Returns (cleaned_raw, conclusion_text). The conclusion is the last block of
        consecutive non-empty, non-header, non-bullet lines at the end of the text.
        """
        lines = raw.split("\n")
        i = len(lines) - 1
        # Skip trailing empty lines
        while i >= 0 and not lines[i].strip():
            i -= 1
        # Collect trailing body lines (not headers, not bullets, not horizontal rules)
        conclusion_parts = []
        while i >= 0:
            stripped = lines[i].strip()
            if not stripped:
                break
            if (stripped.startswith("#") or stripped.startswith("-")
                    or stripped.startswith("*") or re.fullmatch(r"-{3,}", stripped)):
                break
            conclusion_parts.insert(0, stripped)
            i -= 1
        if not conclusion_parts:
            return raw, ""
        conclusion = " ".join(conclusion_parts)
        cleaned_lines = lines[:i + 1]
        while cleaned_lines and not cleaned_lines[-1].strip():
            cleaned_lines.pop()
        return "\n".join(cleaned_lines), conclusion

    def write_insights(
        self,
        spreadsheet_id: str,
        analysis: dict,
        sheet_name: str = "Insights",
    ) -> None:
        """Write Claude-generated insights with rich text formatting (bold/italic in cells)."""
        raw = analysis.get("raw_analysis", "")
        comment_count = analysis.get("comment_count", 0)
        elapsed = analysis.get("elapsed_seconds")
        input_tokens = analysis.get("input_tokens", 0)
        output_tokens = analysis.get("output_tokens", 0)
        cost_usd = analysis.get("cost_usd")
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        # Build generation stats line
        stats_parts = [f"Generated at: {generated_at}"]
        if elapsed is not None:
            stats_parts.append(f"Duration: {elapsed:.1f}s")
        if input_tokens or output_tokens:
            stats_parts.append(f"Tokens: {input_tokens:,} in / {output_tokens:,} out")
        if cost_usd is not None:
            stats_parts.append(f"Cost: ~${cost_usd:.4f}")
        stats_line = "  \u2022  ".join(stats_parts)

        # Extract trailing conclusion paragraph from the analysis body
        raw, conclusion = self._extract_conclusion(raw)
        scope_line = conclusion if conclusion else f"Based on {comment_count} engineer comments"

        # Parse markdown lines into typed items
        items = [
            {"text": "CC Statistics — AI-Generated Insights", "type": "title"},
            {"text": stats_line, "type": "meta"},
            {"text": scope_line, "type": "meta"},
        ]
        for line in raw.split("\n"):
            stripped = line.strip()
            if stripped.startswith("## ") or stripped.startswith("# "):
                items.append({"text": stripped.lstrip("# ").strip(), "type": "section_header"})
            elif stripped.startswith("- ") or stripped.startswith("* "):
                items.append({"text": stripped[2:], "type": "bullet"})
            elif re.fullmatch(r'-{3,}', stripped) or stripped == "":
                pass  # skip empty lines and horizontal rules — section headers are the visual break
            else:
                items.append({"text": stripped, "type": "body"})

        # Resolve rich text for bullet and body rows
        bullet_prefix = "  \u2022  "
        for item in items:
            if item["type"] in ("bullet", "body"):
                plain, runs = self._parse_markdown_runs(item["text"])
                if item["type"] == "bullet":
                    # Shift run indices past the bullet prefix
                    if runs:
                        for run in runs:
                            run["startIndex"] += len(bullet_prefix)
                    plain = bullet_prefix + plain
                item["plain"] = plain
                item["runs"] = runs
            else:
                item["plain"] = item["text"]
                item["runs"] = None

        # Ensure sheet exists, then clear it
        self._ensure_sheet_exists(spreadsheet_id, sheet_name)
        quoted = self._quote_sheet_name(sheet_name)
        self._service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=f"{quoted}!A1:ZZ100000"
        ).execute()

        sheet_id = self._get_sheet_id(spreadsheet_id, sheet_name)
        if sheet_id is None:
            return

        # Write all rows in one updateCells request with rich text
        cell_rows = []
        for item in items:
            cell = {"userEnteredValue": {"stringValue": item["plain"]}}
            if item["runs"]:
                cell["textFormatRuns"] = item["runs"]
            cell_rows.append({"values": [cell]})

        self._service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": len(cell_rows),
                        "startColumnIndex": 0,
                        "endColumnIndex": 1,
                    },
                    "rows": cell_rows,
                    "fields": "userEnteredValue,textFormatRuns",
                }
            }]},
        ).execute()

        # Build index lists per type for visual formatter
        meta = {t: [] for t in ("title", "meta", "section_header", "bullet", "body")}
        for i, item in enumerate(items):
            meta[item["type"]].append(i)
        meta["total_rows"] = len(items)

        self._format_insights(spreadsheet_id, sheet_id, meta)

    @staticmethod
    def _contiguous_groups(indices: list) -> list:
        """Convert a list of row indices into (start, end) range tuples."""
        if not indices:
            return []
        groups, start, prev = [], indices[0], indices[0]
        for r in indices[1:]:
            if r == prev + 1:
                prev = r
            else:
                groups.append((start, prev + 1))
                start = prev = r
        groups.append((start, prev + 1))
        return groups

    def _format_insights(self, spreadsheet_id: str, sheet_id: int, meta: dict) -> None:
        requests = [
            # Wide column A for readable text
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": 1,
                    },
                    "properties": {"pixelSize": 1020},
                    "fields": "pixelSize",
                }
            },
            # Reset ALL cells in col A to clean defaults first.
            # values().clear() only clears values — on re-runs, old formatting (e.g. white
            # foregroundColor from section headers that shifted positions) would persist and
            # make data rows unreadable. This wipe ensures a clean slate before applying
            # specific per-row formatting below.
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startColumnIndex": 0, "endColumnIndex": 1},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _WHITE,
                            "textFormat": {
                                "foregroundColor": _DARK_GRAY,
                                "bold": False,
                                "italic": False,
                                "fontSize": 10,
                            },
                            "wrapStrategy": "WRAP",
                            "padding": {"top": 0, "bottom": 0, "left": 0, "right": 0},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy,padding)",
                }
            },
        ]

        # Title rows — navy background, large white bold text, tall row
        for r in meta["title"]:
            requests += [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": r,
                            "endRowIndex": r + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": _NAVY,
                                "textFormat": {
                                    "bold": True,
                                    "foregroundColor": _WHITE,
                                    "fontSize": 15,
                                },
                                "verticalAlignment": "MIDDLE",
                                "padding": {"top": 6, "bottom": 6, "left": 14, "right": 14},
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment,padding)",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": r,
                            "endIndex": r + 1,
                        },
                        "properties": {"pixelSize": 46},
                        "fields": "pixelSize",
                    }
                },
            ]

        # Meta rows — light gray bg, small italic gray text
        for r in meta["meta"]:
            requests += [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": r,
                            "endRowIndex": r + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": _VERY_LIGHT_GRAY,
                                "textFormat": {
                                    "italic": True,
                                    "foregroundColor": _MED_GRAY,
                                    "fontSize": 9,
                                },
                                "padding": {"left": 14},
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,padding)",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": r,
                            "endIndex": r + 1,
                        },
                        "properties": {"pixelSize": 20},
                        "fields": "pixelSize",
                    }
                },
            ]

        # Section header rows — blue background, white bold, medium height
        for r in meta["section_header"]:
            requests += [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": r,
                            "endRowIndex": r + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": _BLUE,
                                "textFormat": {
                                    "bold": True,
                                    "foregroundColor": _WHITE,
                                    "fontSize": 11,
                                },
                                "verticalAlignment": "MIDDLE",
                                "padding": {"top": 4, "bottom": 4, "left": 12},
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment,padding)",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": r,
                            "endIndex": r + 1,
                        },
                        "properties": {"pixelSize": 30},
                        "fields": "pixelSize",
                    }
                },
            ]

        # Bullet rows — light blue tint background, batched by contiguous groups
        for start, end in self._contiguous_groups(meta["bullet"]):
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start,
                        "endRowIndex": end,
                        "startColumnIndex": 0,
                        "endColumnIndex": 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _LIGHT_BLUE_TINT,
                            "textFormat": {"foregroundColor": _DARK_GRAY, "fontSize": 10},
                            "padding": {"left": 16},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,padding)",
                }
            })

        # Body rows — white background
        for start, end in self._contiguous_groups(meta["body"]):
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start,
                        "endRowIndex": end,
                        "startColumnIndex": 0,
                        "endColumnIndex": 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _WHITE,
                            "textFormat": {"foregroundColor": _DARK_GRAY, "fontSize": 10},
                            "padding": {"left": 14},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,padding)",
                }
            })

        # First batchUpdate: visual formatting (wrap must be applied before auto-resize)
        self._service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()

        # Second batchUpdate: auto-resize all rows (accounts for wrap set above),
        # then override specific rows with fixed heights.
        n = meta["total_rows"]
        height_requests = [
            {"autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": n,
                }
            }},
        ]
        for r in meta["title"]:
            height_requests.append({"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": r, "endIndex": r + 1},
                "properties": {"pixelSize": 46}, "fields": "pixelSize",
            }})
        for r in meta["meta"]:
            height_requests.append({"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": r, "endIndex": r + 1},
                "properties": {"pixelSize": 20}, "fields": "pixelSize",
            }})
        for r in meta["section_header"]:
            height_requests.append({"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": r, "endIndex": r + 1},
                "properties": {"pixelSize": 30}, "fields": "pixelSize",
            }})
        self._service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": height_requests}
        ).execute()

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------

    def delete_sheet_if_exists(self, spreadsheet_id: str, sheet_name: str) -> None:
        """Delete a sheet tab by name if it exists. Won't delete the last remaining tab."""
        meta = self._service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = meta.get("sheets", [])
        for sheet in sheets:
            props = sheet["properties"]
            if props["title"] == sheet_name:
                if len(sheets) > 1:
                    self._service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={"requests": [{"deleteSheet": {"sheetId": props["sheetId"]}}]},
                    ).execute()
                break
