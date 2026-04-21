import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


def test_run_pipeline_calls_all_modules_in_order():
    """run_pipeline calls all modules: auth, drive, reader, aggregator, analyzer, writer."""
    mock_df = pd.DataFrame({
        "task": ["Task A"],
        "estimated_hours": [8.0],
        "actual_hours": [4.0],
        "engineer": ["Alice"],
        "date": ["2024-01-10"],
        "comments": ["Great!"],
        "source_sheet": ["Sheet1"],
    })

    with patch("src.main.validate_config"), \
         patch("src.main.get_google_credentials") as mock_auth, \
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
            "summary": {"total_tasks": 1, "engineers": ["Alice"]},
            "comments": [{"text": "Great!", "engineer": "Alice", "task": "Task A", "date": ""}],
        }
        mock_analyze.return_value = {
            "raw_analysis": "## Benefits\n- Good",
            "comment_count": 1,
        }

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


def test_run_pipeline_skips_unreadable_sheets():
    """run_pipeline skips sheets that raise ValueError (unrecognizable schema)."""
    with patch("src.main.validate_config"), \
         patch("src.main.get_google_credentials") as mock_auth, \
         patch("src.main.DriveClient") as MockDrive, \
         patch("src.main.SheetsReader") as MockReader, \
         patch("src.main.aggregate_stats") as mock_agg, \
         patch("src.main.analyze_comments") as mock_analyze, \
         patch("src.main.OutputWriter") as MockWriter:

        mock_auth.return_value = MagicMock()
        drive_instance = MockDrive.return_value
        drive_instance.list_sheets_in_folder.return_value = [
            {"id": "bad_sheet", "name": "Bad Sheet"}
        ]
        drive_instance.find_subfolder.return_value = "subfolder123"
        drive_instance.get_or_create_sheet.return_value = "output_id"

        reader_instance = MockReader.return_value
        reader_instance.read_sheet.side_effect = ValueError("Could not find required columns")

        mock_agg.return_value = {
            "all_tasks": pd.DataFrame(),
            "summary": {},
            "comments": [],
        }
        mock_analyze.return_value = {"raw_analysis": "", "comment_count": 0}

        from src.main import run_pipeline
        # Should not raise — bad sheets are skipped with a warning
        run_pipeline(
            folder_id="folder123",
            aggregated_folder_name="Aggregated Info",
            output_sheet_name="CC Statistics Aggregated",
        )

        # aggregate_stats is still called (with empty frames list)
        mock_agg.assert_called_once_with([])


def test_run_pipeline_prints_output_sheet_url(capsys):
    """run_pipeline prints the URL of the output Google Sheet."""
    mock_df = pd.DataFrame({
        "task": ["T"],
        "estimated_hours": [1.0],
        "actual_hours": [0.5],
        "engineer": ["Alice"],
        "comments": [""],
        "source_sheet": ["S"],
        "date": [""],
    })

    with patch("src.main.validate_config"), \
         patch("src.main.get_google_credentials") as mock_auth, \
         patch("src.main.DriveClient") as MockDrive, \
         patch("src.main.SheetsReader") as MockReader, \
         patch("src.main.aggregate_stats") as mock_agg, \
         patch("src.main.analyze_comments") as mock_analyze, \
         patch("src.main.OutputWriter"):

        mock_auth.return_value = MagicMock()
        drive_instance = MockDrive.return_value
        drive_instance.list_sheets_in_folder.return_value = [{"id": "s1", "name": "S1"}]
        drive_instance.find_subfolder.return_value = "sf1"
        drive_instance.get_or_create_sheet.return_value = "output_abc"

        MockReader.return_value.read_sheet.return_value = mock_df
        mock_agg.return_value = {
            "all_tasks": mock_df,
            "summary": {"total_tasks": 1, "engineers": []},
            "comments": [],
        }
        mock_analyze.return_value = {"raw_analysis": "", "comment_count": 0}

        from src.main import run_pipeline
        run_pipeline(
            folder_id="folder123",
            aggregated_folder_name="Aggregated Info",
            output_sheet_name="CC Statistics Aggregated",
        )

    captured = capsys.readouterr()
    assert "output_abc" in captured.out


def test_run_pipeline_exits_when_subfolder_not_found(capsys):
    """run_pipeline exits with sys.exit(1) when the aggregated subfolder is missing."""
    with patch("src.main.validate_config"), \
         patch("src.main.get_google_credentials") as mock_auth, \
         patch("src.main.DriveClient") as MockDrive, \
         patch("src.main.SheetsReader") as MockReader, \
         patch("src.main.aggregate_stats") as mock_agg, \
         patch("src.main.analyze_comments") as mock_analyze, \
         patch("src.main.OutputWriter"):

        mock_auth.return_value = MagicMock()
        drive_instance = MockDrive.return_value
        drive_instance.list_sheets_in_folder.return_value = []
        drive_instance.find_subfolder.side_effect = ValueError("Subfolder 'Aggregated Info' not found")

        mock_agg.return_value = {"all_tasks": __import__("pandas").DataFrame(), "summary": {}, "comments": []}
        mock_analyze.return_value = {"raw_analysis": "", "comment_count": 0}

        with pytest.raises(SystemExit) as exc_info:
            from src.main import run_pipeline
            run_pipeline(folder_id="folder123", aggregated_folder_name="Aggregated Info", output_sheet_name="Out")

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Aggregated Info" in captured.out


def test_run_pipeline_continues_when_analyze_comments_fails():
    """run_pipeline still writes stats and summary when comment analysis fails."""
    mock_df = __import__("pandas").DataFrame({
        "task": ["T"], "estimated_hours": [1.0], "actual_hours": [0.5],
        "engineer": ["Alice"], "date": [""], "comments": ["test"], "source_sheet": ["S"],
    })

    with patch("src.main.validate_config"), \
         patch("src.main.get_google_credentials") as mock_auth, \
         patch("src.main.DriveClient") as MockDrive, \
         patch("src.main.SheetsReader") as MockReader, \
         patch("src.main.aggregate_stats") as mock_agg, \
         patch("src.main.analyze_comments") as mock_analyze, \
         patch("src.main.OutputWriter") as MockWriter:

        mock_auth.return_value = MagicMock()
        drive_instance = MockDrive.return_value
        drive_instance.list_sheets_in_folder.return_value = [{"id": "s1", "name": "S1"}]
        drive_instance.find_subfolder.return_value = "sf1"
        drive_instance.get_or_create_sheet.return_value = "out1"

        MockReader.return_value.read_sheet.return_value = mock_df
        mock_agg.return_value = {
            "all_tasks": mock_df,
            "summary": {"total_tasks": 1, "engineers": []},
            "comments": [{"text": "test", "engineer": "Alice", "task": "T", "date": ""}],
        }
        mock_analyze.side_effect = RuntimeError("Anthropic API failed")

        writer_instance = MockWriter.return_value

        from src.main import run_pipeline
        run_pipeline(folder_id="folder123", aggregated_folder_name="Aggregated Info", output_sheet_name="Out")

        # Statistics and Summary must still be written despite analysis failure
        writer_instance.write_statistics.assert_called_once()
        writer_instance.write_summary_row.assert_called_once()


def test_run_pipeline_calls_sprint_winner_when_descriptions_present():
    """run_pipeline calls pick_sprint_winner when tasks have task_description column."""
    mock_df = __import__("pandas").DataFrame({
        "task": ["Build auth module"],
        "task_description": ["Rewrote OAuth flow using Claude"],
        "estimated_hours": [8.0],
        "actual_hours": [2.0],
        "hours_saved": [6.0],
        "engineer": ["Alice"],
        "date": ["2026-04-08"],
        "comments": ["Great use of Claude"],
        "source_sheet": ["Sheet1"],
    })

    with patch("src.main.validate_config"), \
         patch("src.main.get_google_credentials") as mock_auth, \
         patch("src.main.DriveClient") as MockDrive, \
         patch("src.main.SheetsReader") as MockReader, \
         patch("src.main.aggregate_stats") as mock_agg, \
         patch("src.main.analyze_comments") as mock_analyze, \
         patch("src.main.pick_sprint_winner") as mock_winner, \
         patch("src.main.OutputWriter") as MockWriter:

        mock_auth.return_value = MagicMock()
        drive_instance = MockDrive.return_value
        drive_instance.list_sheets_in_folder.return_value = [{"id": "s1", "name": "Alice Stats"}]
        drive_instance.find_subfolder.return_value = "subfolder123"
        drive_instance.get_or_create_sheet.return_value = "output_id"

        MockReader.return_value.read_sheet.return_value = mock_df
        mock_agg.return_value = {
            "all_tasks": mock_df,
            "summary": {"total_tasks": 1, "engineers": ["Alice"]},
            "comments": [{"text": "Great use of Claude", "engineer": "Alice", "task": "Build auth module", "date": ""}],
        }
        mock_analyze.return_value = {"raw_analysis": "## Benefits\n- Good", "comment_count": 1}
        mock_winner.return_value = {
            "task": "Build auth module",
            "engineer": "Alice",
            "headline": "OAuth rewrite in two hours",
            "reasoning": "Claude helped rewrite the whole module quickly.",
            "input_tokens": 100,
            "output_tokens": 50,
            "cost_usd": 0.001,
        }

        from src.main import run_pipeline
        run_pipeline(
            folder_id="folder123",
            aggregated_folder_name="Aggregated Info",
            output_sheet_name="CC Statistics Aggregated",
        )

        mock_winner.assert_called_once()
        # The analysis passed to write_insights must contain the sprint_winner
        writer_instance = MockWriter.return_value
        write_insights_call = writer_instance.write_insights.call_args
        analysis_arg = write_insights_call.args[1] if write_insights_call.args[1:] else write_insights_call.kwargs.get("analysis")
        assert analysis_arg is not None
        assert analysis_arg.get("sprint_winner") is not None
        assert analysis_arg["sprint_winner"]["task"] == "Build auth module"
