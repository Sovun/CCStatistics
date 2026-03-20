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
    safe_est = combined["estimated_hours"].where(combined["estimated_hours"] > 0)
    combined["efficiency_ratio"] = combined["actual_hours"] / safe_est

    total_estimated = combined["estimated_hours"].sum(min_count=1)
    total_actual = combined["actual_hours"].sum(min_count=1)
    summary = {
        "total_tasks": len(combined),
        "total_estimated_hours": round(float(total_estimated), 2),
        "total_actual_hours": round(float(total_actual), 2),
        "total_hours_saved": round(float(total_estimated - total_actual), 2),
        "overall_efficiency_ratio": round(float(total_actual / total_estimated), 4)
        if pd.notna(total_estimated) and total_estimated > 0
        else 0.0,
        "engineers": sorted(combined["engineer"].dropna().unique().tolist())
        if "engineer" in combined.columns else [],
    }

    comments = []
    if "comments" in combined.columns:
        cols = ["comments", "task", "engineer", "date"]
        available_cols = [c for c in cols if c in combined.columns]
        mask = combined["comments"].astype(str).str.strip().ne("")
        subset = combined.loc[mask, available_cols].copy()
        subset = subset.rename(columns={"comments": "text"})
        subset = subset.fillna("").astype(str)
        comments = subset.to_dict("records")

    return {"all_tasks": combined, "summary": summary, "comments": comments}
