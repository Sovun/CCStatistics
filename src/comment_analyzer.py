import time
import anthropic
from src.config import CLAUDE_MODEL
from datetime import date, datetime, timedelta

# Approximate pricing per million tokens (update if Anthropic changes rates)
_PRICING_PER_MTOK = {
    "claude-opus-4-6":    {"input": 15.0,  "output": 75.0},
    "claude-sonnet-4-6":  {"input": 3.0,   "output": 15.0},
    "claude-haiku-4-5":   {"input": 0.80,  "output": 4.0},
}


_ANALYSIS_PROMPT = """\
You are analyzing feedback from software engineers about their experience using Claude Code AI assistant for development tasks.

Below are comments collected from {engineer_count} engineers across {comment_count} tasks.

---
{comments_block}
---

Please analyze these comments and produce a structured report with the following sections:

## Overall Summary
A 3-5 sentence high-level conclusion about the team's Claude Code experience: overall impact, adoption quality, and whether AI assistance is delivering measurable value.

## Key Benefits
Bullet list of the most frequently cited benefits and positive outcomes engineers experienced.

## Key Pain Points & Challenges
Bullet list of recurring difficulties, frustrations, limitations, or cases where AI assistance fell short.

## Best Practices
Bullet list of approaches, workflows, or task types where engineers got the best results from Claude Code.

## Worst Practices
Bullet list of approaches or situations that led to poor results, wasted time, or frustration with Claude Code.

## Usage Patterns
Bullet list of observations about which types of tasks Claude Code helped most vs least, and how engineers are actually using it day-to-day.

## Recommendations
Actionable suggestions for the team to get more value from Claude Code, based directly on the feedback patterns observed.

## Notable Quotes
3-5 direct quotes from engineers that best capture the team's sentiment (include both positive and critical voices).

Keep the analysis concrete, grounded in the actual comments, and actionable.
"""


_SPRINT_WINNER_PROMPT = """\
You are reviewing tasks completed by software engineers who used Claude Code AI assistant.

Below are tasks worked on this week, each with its description and notes:

---
{tasks_block}
---

Identify the SINGLE best "Claude Code win of the week" — the task that best demonstrates \
impressive, creative, or insightful use of Claude Code.

Consider:
- Tasks where AI assistance was especially clever or saved significant time
- Creative problem-solving enabled by Claude Code
- Surprising or elegant outcomes made possible by AI assistance
- Tasks where the description shows genuine AI-human collaboration at its best

Respond in this EXACT format with no extra text before or after:

WINNER_TASK: <exact task name from the list above>
WINNER_ENGINEER: <engineer name>
WINNER_HEADLINE: <one punchy sentence, max 15 words, capturing why this wins>
WINNER_REASONING: <2-3 sentences explaining what made this the standout Claude Code use case>
"""


def pick_sprint_winner(
    tasks: list[dict],
    api_key: str,
    model: str = CLAUDE_MODEL,
) -> "dict | None":
    """Use Claude to pick the best 'Claude Code win of the sprint' from a list of tasks.

    Args:
        tasks: List of dicts with keys: task, task_description, engineer, date,
               hours_saved, comments (all strings)
        api_key: Anthropic API key
        model: Claude model ID to use

    Returns:
        Dict with task, engineer, headline, reasoning, cost_usd, input_tokens, output_tokens
        or None if tasks list is empty.
    """
    if not tasks:
        return None

    def _fmt_task(t: dict) -> str:
        lines = [f"Task: {t.get('task', '')}"]
        if t.get("engineer"):
            lines.append(f"Engineer: {t['engineer']}")
        if t.get("date"):
            lines.append(f"Date: {t['date']}")
        if t.get("task_description"):
            lines.append(f"Description: {t['task_description']}")
        saved = t.get("hours_saved", "")
        if saved and saved != "0" and saved != "0.0":
            lines.append(f"Hours saved: {saved}")
        if t.get("comments"):
            lines.append(f"Notes: {t['comments']}")
        return "\n".join(lines)

    tasks_block = "\n\n".join(_fmt_task(t) for t in tasks)
    prompt = _SPRINT_WINNER_PROMPT.format(tasks_block=tasks_block)

    client = anthropic.Anthropic(api_key=api_key)
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}],
            )
            break
        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < max_retries - 1:
                wait = 10 * (2 ** attempt)
                print(f"  API overloaded (529), retrying in {wait}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"Claude API call failed during sprint winner selection: {e}"
                ) from e
        except anthropic.APIError as e:
            raise RuntimeError(
                f"Claude API call failed during sprint winner selection: {e}"
            ) from e

    raw = response.content[0].text.strip()

    # Parse the structured response
    parsed: dict[str, str] = {}
    for line in raw.split("\n"):
        for key in ("WINNER_TASK", "WINNER_ENGINEER", "WINNER_HEADLINE", "WINNER_REASONING"):
            if line.startswith(f"{key}: "):
                parsed[key] = line[len(f"{key}: "):].strip()
                break

    if not all(k in parsed for k in ("WINNER_TASK", "WINNER_HEADLINE", "WINNER_REASONING")):
        import warnings
        warnings.warn(
            f"Sprint winner response could not be fully parsed. Raw: {raw[:200]}",
            RuntimeWarning,
            stacklevel=2,
        )
        return None

    rates = _PRICING_PER_MTOK.get(model, {"input": 3.0, "output": 15.0})
    cost_usd = (
        response.usage.input_tokens * rates["input"]
        + response.usage.output_tokens * rates["output"]
    ) / 1_000_000

    return {
        "task": parsed.get("WINNER_TASK", ""),
        "engineer": parsed.get("WINNER_ENGINEER", ""),
        "headline": parsed.get("WINNER_HEADLINE", ""),
        "reasoning": parsed.get("WINNER_REASONING", ""),
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
        "cost_usd": cost_usd,
    }


def analyze_comments(
    comments: list[dict],
    api_key: str,
    model: str = CLAUDE_MODEL,
) -> dict:
    """Send engineer comments to Claude for qualitative analysis.

    Args:
        comments: List of dicts with keys: text, engineer, task, date
        api_key: Anthropic API key
        model: Claude model ID to use

    Returns:
        Dict with 'raw_analysis' (markdown string) and 'comment_count' (int)
    """
    if not comments:
        return {"raw_analysis": "", "comment_count": 0}

    # Rough token estimate: 1 token ≈ 4 chars
    MAX_INPUT_CHARS = 400_000  # ~100k tokens, well within Claude's 200k context window
    total_chars = sum(len(c.get("text", "")) for c in comments)
    if total_chars > MAX_INPUT_CHARS:
        raise ValueError(
            f"Comment input too large ({total_chars:,} chars ≈ {total_chars // 4:,} tokens). "
            f"Maximum is ~100k tokens. Consider filtering or batching comments."
        )

    comments_block = "\n\n".join(
        f"[{c.get('engineer', 'Unknown')} | {c.get('task', '')} | {c.get('date', '')}]\n{c.get('text', '')}"
        for c in comments
    )

    engineer_names = {c.get("engineer", "") for c in comments if c.get("engineer")}
    prompt = _ANALYSIS_PROMPT.format(
        engineer_count=len(engineer_names),
        comment_count=len(comments),
        comments_block=comments_block,
    )

    client = anthropic.Anthropic(api_key=api_key)
    t0 = time.time()
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=8192,
                messages=[{"role": "user", "content": prompt}],
            )
            break
        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < max_retries - 1:
                wait = 10 * (2 ** attempt)  # 10s, 20s, 40s
                print(f"  API overloaded (529), retrying in {wait}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"Claude API call failed during comment analysis: {e}"
                ) from e
        except anthropic.APIError as e:
            raise RuntimeError(
                f"Claude API call failed during comment analysis: {e}"
            ) from e
    elapsed = time.time() - t0

    if response.stop_reason != "end_turn":
        import warnings
        warnings.warn(
            f"Claude response was truncated (stop_reason={response.stop_reason!r}). "
            "Analysis may be incomplete. Consider reducing the number of comments.",
            RuntimeWarning,
            stacklevel=2,
        )

    input_tokens = response.usage.input_tokens
    output_tokens = response.usage.output_tokens
    rates = _PRICING_PER_MTOK.get(model, {"input": 3.0, "output": 15.0})
    cost_usd = (input_tokens * rates["input"] + output_tokens * rates["output"]) / 1_000_000

    raw_analysis = response.content[0].text
    return {
        "raw_analysis": raw_analysis,
        "comment_count": len(comments),
        "elapsed_seconds": round(elapsed, 1),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": cost_usd,
    }
