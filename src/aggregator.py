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

    combined.loc[:, "hours_saved"] = combined["estimated_hours"] - combined["actual_hours"]
    safe_est = combined["estimated_hours"].where(combined["estimated_hours"] > 0)
    # Always (re)compute deviation as actual/estimated for consistency across all sheets.
    # Overwrites any deviation value the engineer may have filled in their source sheet.
    combined.loc[:, "deviation"] = combined["actual_hours"] / safe_est

    # Only include rows where both hours are present to avoid asymmetric NaN skipping
    # inflating hours_saved when one side is missing.
    complete = combined.dropna(subset=["estimated_hours", "actual_hours"])
    total_estimated = complete["estimated_hours"].sum(min_count=1)
    total_actual = complete["actual_hours"].sum(min_count=1)
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

    # Extended metrics
    te = summary["total_estimated_hours"]
    ta = summary["total_actual_hours"]
    ths = summary["total_hours_saved"]
    tt = summary["total_tasks"]
    summary["engineers_with_data"] = len(summary["engineers"])
    summary["avg_estimated_per_task"] = round(te / tt, 2) if tt > 0 else 0.0
    summary["avg_actual_per_task"] = round(ta / tt, 2) if tt > 0 else 0.0
    summary["avg_hours_saved_per_task"] = round(ths / tt, 2) if tt > 0 else 0.0
    summary["time_savings_pct"] = round((ths / te) * 100, 1) if te > 0 else 0.0
    summary["speed_multiplier"] = round(te / ta, 2) if ta > 0 else 0.0
    summary["equiv_days_saved"] = round(ths / 8, 1)
    if "deviation" in combined.columns:
        dev = combined["deviation"].dropna()
        summary["tasks_significantly_faster"] = int((dev < 0.75).sum())
        summary["tasks_no_benefit"] = int((dev >= 1.0).sum())
    else:
        summary["tasks_significantly_faster"] = 0
        summary["tasks_no_benefit"] = 0

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
