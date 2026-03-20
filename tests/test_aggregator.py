import math
import pandas as pd
import pytest
from src.aggregator import aggregate_stats


@pytest.fixture
def alice_df():
    return pd.DataFrame({
        "task": ["Task A", "Task B"],
        "estimated_hours": [8.0, 4.0],
        "actual_hours": [4.0, 2.0],
        "engineer": ["Alice", "Alice"],
        "date": ["2024-01-10", "2024-01-11"],
        "comments": ["Claude was great for boilerplate", ""],
        "source_sheet": ["Sheet1", "Sheet1"],
    })


@pytest.fixture
def bob_df():
    return pd.DataFrame({
        "task": ["Task C"],
        "estimated_hours": [6.0],
        "actual_hours": [5.0],
        "engineer": ["Bob"],
        "date": ["2024-01-12"],
        "comments": ["Took longer than expected"],
        "source_sheet": ["Sheet1"],
    })


def test_aggregate_stats_combines_all_rows(alice_df, bob_df):
    result = aggregate_stats([alice_df, bob_df])
    assert len(result["all_tasks"]) == 3


def test_aggregate_stats_calculates_hours_saved(alice_df, bob_df):
    result = aggregate_stats([alice_df, bob_df])
    df = result["all_tasks"]
    assert "hours_saved" in df.columns
    assert df.iloc[0]["hours_saved"] == 4.0


def test_aggregate_stats_calculates_efficiency_ratio(alice_df, bob_df):
    result = aggregate_stats([alice_df, bob_df])
    df = result["all_tasks"]
    assert "efficiency_ratio" in df.columns
    # Task A: 4/8 = 0.5
    assert abs(df.iloc[0]["efficiency_ratio"] - 0.5) < 0.01


def test_aggregate_stats_produces_summary(alice_df, bob_df):
    result = aggregate_stats([alice_df, bob_df])
    summary = result["summary"]
    assert summary["total_tasks"] == 3
    assert summary["total_estimated_hours"] == 18.0
    assert summary["total_actual_hours"] == 11.0
    assert abs(summary["overall_efficiency_ratio"] - (11.0 / 18.0)) < 0.01


def test_aggregate_stats_collects_non_empty_comments(alice_df, bob_df):
    result = aggregate_stats([alice_df, bob_df])
    comments = result["comments"]
    # "Claude was great for boilerplate" and "Took longer than expected" — empty "" excluded
    assert len(comments) == 2
    texts = [c["text"] for c in comments]
    assert "Claude was great for boilerplate" in texts
    assert "Took longer than expected" in texts


def test_aggregate_stats_returns_empty_result_on_empty_input():
    result = aggregate_stats([])
    assert result["all_tasks"].empty
    assert result["summary"] == {}
    assert result["comments"] == []


def test_aggregate_stats_handles_missing_hours_saved_when_hours_are_nan():
    """hours_saved is NaN when either estimated or actual hours is NaN."""
    df = pd.DataFrame({
        "task": ["Task X"],
        "estimated_hours": [float("nan")],
        "actual_hours": [3.0],
        "engineer": ["Zara"],
        "comments": [""],
    })
    result = aggregate_stats([df])
    row = result["all_tasks"].iloc[0]
    assert math.isnan(row["hours_saved"])
