import pytest
import anthropic
from unittest.mock import MagicMock, patch
from src.comment_analyzer import analyze_comments, pick_sprint_winner


def test_analyze_comments_returns_empty_on_no_comments():
    """analyze_comments returns empty result dict when given no comments."""
    result = analyze_comments([], api_key="test-key")
    assert result == {"raw_analysis": "", "comment_count": 0}


def test_analyze_comments_calls_claude_api(monkeypatch):
    """analyze_comments calls the Anthropic API with all comment texts."""
    comments = [
        {"text": "Claude was amazing for boilerplate", "engineer": "Alice", "task": "Auth module", "date": "2024-01-10"},
        {"text": "Struggled with complex business logic", "engineer": "Bob", "task": "Pricing rules", "date": "2024-01-11"},
    ]

    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="## Benefits\n- Great for boilerplate\n## Pain Points\n- Complex logic harder")]
    mock_response.stop_reason = "end_turn"

    with patch("src.comment_analyzer.anthropic.Anthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages.create.return_value = mock_response

        result = analyze_comments(comments, api_key="test-key", model="claude-haiku-4-5-20251001")

    assert result["raw_analysis"] == "## Benefits\n- Great for boilerplate\n## Pain Points\n- Complex logic harder"
    assert result["comment_count"] == 2


def test_analyze_comments_includes_all_texts_in_prompt():
    """analyze_comments includes all comment texts in the message sent to Claude."""
    comments = [
        {"text": "Super fast code generation", "engineer": "Alice", "task": "Task A", "date": "2024-01-10"},
        {"text": "Tests were hard to write", "engineer": "Bob", "task": "Task B", "date": "2024-01-11"},
    ]

    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="## Analysis\n- noted")]
    mock_response.stop_reason = "end_turn"

    with patch("src.comment_analyzer.anthropic.Anthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages.create.return_value = mock_response

        analyze_comments(comments, api_key="test-key")

        call_args = instance.messages.create.call_args
        prompt_content = call_args.kwargs["messages"][0]["content"]

    assert "Super fast code generation" in prompt_content
    assert "Tests were hard to write" in prompt_content


def test_analyze_comments_uses_configured_model():
    """analyze_comments uses the model argument when calling Claude API."""
    comments = [{"text": "Good tool", "engineer": "Alice", "task": "T", "date": ""}]

    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="## Summary\n- good")]
    mock_response.stop_reason = "end_turn"

    with patch("src.comment_analyzer.anthropic.Anthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages.create.return_value = mock_response

        analyze_comments(comments, api_key="test-key", model="claude-opus-4-6")

        call_args = instance.messages.create.call_args
        assert call_args.kwargs["model"] == "claude-opus-4-6"


def test_analyze_comments_raises_on_api_error():
    """analyze_comments raises RuntimeError when Anthropic API call fails."""
    comments = [{"text": "Some feedback", "engineer": "Alice", "task": "T", "date": ""}]

    with patch("src.comment_analyzer.anthropic.Anthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages.create.side_effect = anthropic.APIConnectionError(request=MagicMock())

        with pytest.raises(RuntimeError, match="Claude API call failed"):
            analyze_comments(comments, api_key="test-key")


def test_analyze_comments_raises_on_oversized_input():
    """analyze_comments raises ValueError when total comment text exceeds token limit."""
    # Create a comment with ~500k chars (exceeds 400k limit)
    huge_comment = {"text": "x" * 500_000, "engineer": "Alice", "task": "T", "date": ""}

    with pytest.raises(ValueError, match="Comment input too large"):
        analyze_comments([huge_comment], api_key="test-key")


def test_analyze_comments_warns_on_truncated_response():
    """analyze_comments emits RuntimeWarning when Claude response is truncated."""
    import warnings
    comments = [{"text": "Good feedback", "engineer": "Alice", "task": "T", "date": ""}]

    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="## Summary\n- partial")]
    mock_response.stop_reason = "max_tokens"  # truncated

    with patch("src.comment_analyzer.anthropic.Anthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages.create.return_value = mock_response

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = analyze_comments(comments, api_key="test-key")

        assert any(issubclass(warning.category, RuntimeWarning) for warning in w)
        assert "truncated" in str(w[0].message).lower()

    assert result["raw_analysis"] == "## Summary\n- partial"


# ---------------------------------------------------------------------------
# pick_sprint_winner tests
# ---------------------------------------------------------------------------

def test_pick_sprint_winner_returns_none_on_empty_tasks():
    """pick_sprint_winner returns None immediately when given no tasks."""
    result = pick_sprint_winner([], api_key="test-key")
    assert result is None


def test_pick_sprint_winner_calls_claude_and_parses_response():
    """pick_sprint_winner calls Claude and correctly parses the structured response."""
    tasks = [
        {
            "task": "Refactor auth module",
            "task_description": "Used Claude to rewrite OAuth flow in 1 hour",
            "engineer": "Alice",
            "date": "2026-04-08",
            "hours_saved": "6.0",
            "comments": "Saved huge amount of time",
        }
    ]
    raw_response = (
        "WINNER_TASK: Refactor auth module\n"
        "WINNER_ENGINEER: Alice\n"
        "WINNER_HEADLINE: OAuth rewrite in one hour — six hours saved\n"
        "WINNER_REASONING: Claude produced a clean, idiomatic rewrite of the auth "
        "module in a single session, cutting manual effort by 75%."
    )

    mock_response = MagicMock()
    mock_response.content = [MagicMock(text=raw_response)]
    mock_response.usage.input_tokens = 200
    mock_response.usage.output_tokens = 80

    with patch("src.comment_analyzer.anthropic.Anthropic") as MockClient:
        MockClient.return_value.messages.create.return_value = mock_response

        result = pick_sprint_winner(tasks, api_key="test-key", model="claude-haiku-4-5-20251001")

    assert result is not None
    assert result["task"] == "Refactor auth module"
    assert result["engineer"] == "Alice"
    assert "one hour" in result["headline"]
    assert "auth" in result["reasoning"].lower()
    assert result["input_tokens"] == 200
    assert result["output_tokens"] == 80
    assert result["cost_usd"] >= 0


def test_pick_sprint_winner_returns_none_on_unparseable_response():
    """pick_sprint_winner returns None and warns when response format is wrong."""
    import warnings
    tasks = [{"task": "T", "task_description": "desc", "engineer": "Bob", "date": "", "hours_saved": "", "comments": ""}]

    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="Sorry, I cannot determine a winner.")]
    mock_response.usage.input_tokens = 50
    mock_response.usage.output_tokens = 10

    with patch("src.comment_analyzer.anthropic.Anthropic") as MockClient:
        MockClient.return_value.messages.create.return_value = mock_response

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = pick_sprint_winner(tasks, api_key="test-key")

    assert result is None
    assert any(issubclass(warning.category, RuntimeWarning) for warning in w)


def test_pick_sprint_winner_includes_task_description_in_prompt():
    """pick_sprint_winner includes task description text in the Claude prompt."""
    tasks = [
        {
            "task": "Build CI pipeline",
            "task_description": "Automated full CI/CD with Claude assistance",
            "engineer": "Carol",
            "date": "",
            "hours_saved": "4.0",
            "comments": "",
        }
    ]
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text=(
        "WINNER_TASK: Build CI pipeline\n"
        "WINNER_ENGINEER: Carol\n"
        "WINNER_HEADLINE: Full CI/CD built in record time\n"
        "WINNER_REASONING: Claude generated all pipeline config files instantly."
    ))]
    mock_response.usage.input_tokens = 100
    mock_response.usage.output_tokens = 40

    with patch("src.comment_analyzer.anthropic.Anthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages.create.return_value = mock_response

        pick_sprint_winner(tasks, api_key="test-key")

        prompt = instance.messages.create.call_args.kwargs["messages"][0]["content"]

    assert "Automated full CI/CD with Claude assistance" in prompt
    assert "Build CI pipeline" in prompt
