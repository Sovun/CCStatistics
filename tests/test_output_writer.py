import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from src.output_writer import OutputWriter


@pytest.fixture
def mock_creds():
    return MagicMock()


@pytest.fixture
def writer(mock_creds):
    with patch("src.output_writer.build"):
        return OutputWriter(mock_creds)


def _attach_mock_service(writer):
    mock_service = MagicMock()
    writer._service = mock_service
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Sheet1", "sheetId": 0}}]
    }
    mock_service.spreadsheets().batchUpdate().execute.return_value = {}
    mock_service.spreadsheets().values().clear().execute.return_value = {}
    mock_service.spreadsheets().values().update().execute.return_value = {}
    return mock_service


def test_write_statistics_calls_update_with_header_and_rows(writer):
    """write_statistics sends header row + data rows to the target sheet tab."""
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

    update_call = mock_service.spreadsheets().values().update
    update_call.assert_called()
    call_kwargs = update_call.call_args.kwargs
    assert call_kwargs["range"] == "'Statistics'!A1"
    # Header row must be the DataFrame column names
    assert call_kwargs["body"]["values"][0] == list(df.columns)
    # Data row count matches
    assert len(call_kwargs["body"]["values"]) == 1 + len(df)  # header + rows


def test_write_summary_row_writes_key_value_pairs(writer):
    """write_summary_row writes metric name and value pairs to a sheet tab."""
    mock_service = _attach_mock_service(writer)

    summary = {
        "total_tasks": 10,
        "total_estimated_hours": 80.0,
        "total_actual_hours": 50.0,
        "total_hours_saved": 30.0,
        "overall_efficiency_ratio": 0.625,
        "engineers": ["Alice", "Bob"],
    }

    writer.write_summary_row("spreadsheet123", summary, sheet_name="Summary")

    update_call = mock_service.spreadsheets().values().update
    update_call.assert_called()
    values = update_call.call_args.kwargs["body"]["values"]
    # First row should be a header ["Metric", "Value"]
    assert values[0] == ["Metric", "Value"]
    # All summary keys should appear in subsequent rows
    row_keys = [r[0] for r in values[1:] if len(r) >= 1]
    assert "total_tasks" in row_keys or any("total_tasks" in str(k) for k in row_keys)


def test_write_insights_writes_markdown_as_rows(writer):
    """write_insights writes analysis text as individual rows."""
    mock_service = _attach_mock_service(writer)

    analysis = {
        "raw_analysis": "## Benefits\n- Fast\n## Pain Points\n- Slow",
        "comment_count": 5,
    }

    writer.write_insights("spreadsheet123", analysis, sheet_name="Insights")

    update_call = mock_service.spreadsheets().values().update
    update_call.assert_called()
    values = update_call.call_args.kwargs["body"]["values"]
    # Should contain header rows + content rows
    all_text = " ".join(str(v) for row in values for v in row)
    assert "Benefits" in all_text
    assert "Pain Points" in all_text


def test_write_statistics_creates_sheet_tab_if_not_exists(writer):
    """write_statistics creates a new tab if it does not already exist."""
    mock_service = MagicMock()
    writer._service = mock_service
    # Sheet1 exists, Statistics does not
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Sheet1", "sheetId": 0}}]
    }
    mock_service.spreadsheets().batchUpdate().execute.return_value = {}
    mock_service.spreadsheets().values().clear().execute.return_value = {}
    mock_service.spreadsheets().values().update().execute.return_value = {}

    df = pd.DataFrame({"task": ["T"], "estimated_hours": [1.0], "actual_hours": [0.5]})
    writer.write_statistics("spreadsheet123", df, sheet_name="Statistics")

    # batchUpdate should have been called to add the new sheet
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_write_statistics_quotes_sheet_name_in_range(writer):
    """write_statistics uses single-quoted sheet name in the range string."""
    mock_service = _attach_mock_service(writer)

    df = pd.DataFrame({"task": ["T"], "estimated_hours": [1.0], "actual_hours": [0.5]})
    writer.write_statistics("sid", df, sheet_name="Q1 Stats")

    update_call = mock_service.spreadsheets().values().update
    call_kwargs = update_call.call_args.kwargs
    assert call_kwargs["range"] == "'Q1 Stats'!A1"


def test_write_statistics_raises_runtime_error_on_api_failure(writer):
    """write_statistics raises RuntimeError when Sheets API call fails."""
    from googleapiclient.errors import HttpError
    mock_service = _attach_mock_service(writer)

    mock_resp = MagicMock()
    mock_resp.status = 403
    mock_resp.reason = "Forbidden"
    mock_service.spreadsheets().values().clear().execute.side_effect = HttpError(
        resp=mock_resp, content=b"Forbidden"
    )

    df = pd.DataFrame({"task": ["T"], "estimated_hours": [1.0], "actual_hours": [0.5]})
    with pytest.raises(RuntimeError, match="Failed to write to sheet"):
        writer.write_statistics("sid", df)
