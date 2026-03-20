import anthropic
from src.config import CLAUDE_MODEL


_ANALYSIS_PROMPT = """\
You are analyzing feedback from software engineers about their experience using Claude Code AI assistant for development tasks.

Below are comments collected from {engineer_count} engineers across {comment_count} tasks.

---
{comments_block}
---

Please analyze these comments and produce a structured report with the following sections:

## Summary
A 2-3 sentence overall summary of the team's Claude Code experience.

## Key Benefits
Bullet list of the most frequently cited benefits and positive patterns.

## Pain Points & Challenges
Bullet list of recurring difficulties, frustrations, or limitations observed.

## Usage Patterns
Observations about which types of tasks Claude Code helped most vs least.

## Recommendations
Actionable suggestions for the team to get more value from Claude Code based on this feedback.

## Notable Quotes
3-5 direct quotes that best capture the team's sentiment (positive and negative).

Keep the analysis concrete, grounded in the actual comments, and actionable.
"""


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

    comments_block = "\n\n".join(
        f"[{c.get('engineer', 'Unknown')} | {c.get('task', '')} | {c.get('date', '')}]\n{c['text']}"
        for c in comments
    )

    engineer_names = {c.get("engineer", "") for c in comments if c.get("engineer")}
    prompt = _ANALYSIS_PROMPT.format(
        engineer_count=len(engineer_names),
        comment_count=len(comments),
        comments_block=comments_block,
    )

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    raw_analysis = response.content[0].text
    return {"raw_analysis": raw_analysis, "comment_count": len(comments)}
