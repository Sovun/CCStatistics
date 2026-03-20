import pytest
from unittest.mock import MagicMock, patch
from src.comment_analyzer import analyze_comments


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

    with patch("src.comment_analyzer.anthropic.Anthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages.create.return_value = mock_response

        analyze_comments(comments, api_key="test-key", model="claude-opus-4-6")

        call_args = instance.messages.create.call_args
        assert call_args.kwargs["model"] == "claude-opus-4-6"
