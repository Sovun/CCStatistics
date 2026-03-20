import pandas as pd


def aggregate_stats(frames: list[pd.DataFrame]) -> dict:
    """Combine engineer DataFrames and compute derived metrics.

    Returns a dict with keys:
      all_tasks: DataFrame of all rows with added hours_saved and efficiency_ratio columns
      summary:   dict of overall totals (empty dict if no frames)
      comments:  list of {text, task, engineer, date} dicts for non-empty comments
    """
    if not frames:
        return {"all_tasks": pd.DataFrame(), "summary": {}, "comments": []}

    combined = pd.concat(frames, ignore_index=True)

    combined["hours_saved"] = combined["estimated_hours"] - combined["actual_hours"]
    combined["efficiency_ratio"] = combined.apply(
        lambda r: r["actual_hours"] / r["estimated_hours"]
        if pd.notna(r["estimated_hours"]) and r["estimated_hours"] > 0
        else float("nan"),
        axis=1,
    )

    total_estimated = combined["estimated_hours"].sum()
    total_actual = combined["actual_hours"].sum()
    summary = {
        "total_tasks": len(combined),
        "total_estimated_hours": round(float(total_estimated), 2),
        "total_actual_hours": round(float(total_actual), 2),
        "total_hours_saved": round(float(total_estimated - total_actual), 2),
        "overall_efficiency_ratio": round(float(total_actual / total_estimated), 4)
        if total_estimated else 0.0,
        "engineers": sorted(combined["engineer"].dropna().unique().tolist())
        if "engineer" in combined.columns else [],
    }

    comments = []
    if "comments" in combined.columns:
        for _, row in combined.iterrows():
            text = str(row.get("comments", "")).strip()
            if text:
                comments.append({
                    "text": text,
                    "task": str(row.get("task", "")),
                    "engineer": str(row.get("engineer", "")),
                    "date": str(row.get("date", "")),
                })

    return {"all_tasks": combined, "summary": summary, "comments": comments}
