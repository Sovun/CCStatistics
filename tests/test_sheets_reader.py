import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from src.sheets_reader import SheetsReader


@pytest.fixture
def mock_creds():
    return MagicMock()


@pytest.fixture
def reader(mock_creds):
    with patch("src.sheets_reader.build"):
        return SheetsReader(mock_creds)


def _setup_service(reader, values, tab_name="Sheet1"):
    """Helper: attach mock service returning given values for a single tab."""
    mock_service = MagicMock()
    reader._service = mock_service
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": tab_name}}]
    }
    mock_service.spreadsheets().values().get().execute.return_value = {"values": values}
    return mock_service


def test_read_sheet_returns_dataframe_with_canonical_columns(reader):
    """read_sheet maps recognized headers to canonical column names."""
    values = [
        ["Task Name", "Estimated Hours", "Actual Hours", "Date", "Comments"],
        ["Build login", "8", "4", "2024-01-10", "Claude was great"],
        ["Fix bug #42", "2", "1", "2024-01-11", ""],
    ]
    _setup_service(reader, values)

    df = reader.read_sheet("spreadsheet123", engineer_name="Alice")

    assert "task" in df.columns
    assert "estimated_hours" in df.columns
    assert "actual_hours" in df.columns
    assert len(df) == 2
    assert df.iloc[0]["task"] == "Build login"
    assert df.iloc[0]["estimated_hours"] == 8.0
    assert df.iloc[0]["engineer"] == "Alice"
    assert df["source_sheet"].iloc[0] == "Sheet1"


def test_read_sheet_handles_missing_optional_columns(reader):
    """read_sheet works when optional columns (date, comments) are absent."""
    values = [
        ["Task Name", "Estimate", "Actual"],
        ["Auth module", "5", "3"],
    ]
    _setup_service(reader, values)

    df = reader.read_sheet("spreadsheet123", engineer_name="Bob")

    assert "task" in df.columns
    assert "estimated_hours" in df.columns
    assert "actual_hours" in df.columns
    assert len(df) == 1
    assert df.iloc[0]["task"] == "Auth module"


def test_read_sheet_raises_on_unrecognizable_schema(reader):
    """read_sheet raises ValueError when no required columns can be mapped."""
    values = [
        ["Foo", "Bar", "Baz"],
        ["x", "y", "z"],
    ]
    _setup_service(reader, values)

    with pytest.raises(ValueError, match="Could not find required columns"):
        reader.read_sheet("spreadsheet123", engineer_name="Charlie")


def test_read_sheet_drops_rows_with_empty_task(reader):
    """read_sheet drops rows where task name is empty or whitespace."""
    values = [
        ["Task", "Estimated Hours", "Actual Hours"],
        ["Real task", "4", "2"],
        ["", "1", "1"],
        ["   ", "2", "2"],
    ]
    _setup_service(reader, values)

    df = reader.read_sheet("spreadsheet123", engineer_name="Dave")
    assert len(df) == 1
    assert df.iloc[0]["task"] == "Real task"


def test_read_sheet_coerces_hours_to_float(reader):
    """read_sheet converts estimated_hours and actual_hours to float."""
    values = [
        ["Task", "Estimated Hours", "Actual Hours"],
        ["Task A", "8", "4.5"],
        ["Task B", "bad_value", "2"],
    ]
    _setup_service(reader, values)

    df = reader.read_sheet("spreadsheet123", engineer_name="Eve")
    assert df.iloc[0]["estimated_hours"] == 8.0
    assert df.iloc[0]["actual_hours"] == 4.5
    import math
    assert math.isnan(df.iloc[1]["estimated_hours"])


def test_read_sheet_scans_multiple_tabs_and_uses_first_valid(reader):
    """read_sheet tries each tab and uses the first one with recognizable columns."""
    mock_service = MagicMock()
    reader._service = mock_service

    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [
            {"properties": {"title": "Meta"}},
            {"properties": {"title": "Stats"}},
        ]
    }

    def get_values(spreadsheetId, range, **kwargs):
        mock = MagicMock()
        if range == "Meta":
            mock.execute.return_value = {"values": [["Notes"], ["some note"]]}
        else:
            mock.execute.return_value = {
                "values": [
                    ["Task", "Estimated Hours", "Actual Hours"],
                    ["Feature X", "6", "3"],
                ]
            }
        return mock

    mock_service.spreadsheets().values().get.side_effect = get_values

    df = reader.read_sheet("spreadsheet123", engineer_name="Frank")
    assert df.iloc[0]["task"] == "Feature X"
    assert df["source_sheet"].iloc[0] == "Stats"
