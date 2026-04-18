import sys
import os
# Get the absolute path of the root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # Adjust '..' based on nesting
# Insert the root directory path to the beginning of sys.path
sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
from config import Config

def caclulate_fundamental_rank(rawDf):
    df = rawDf.copy()
    df["sales_qoq_rank"] = df["QoQ Sales"].rank(pct=True)
    df["sales_yoy_rank"] = df["YOY Quarterly sales growth"].rank(pct=True)

    df["eps_qoq_rank"] = df["QoQ EPS"].rank(pct=True)
    df["eps_yoy_rank"] = df["YoY EPS"].rank(pct=True)

    df["roe_rank"] = df["Return on equity"].rank(pct=True)
    df["roce_rank"] = df["Return on capital employed"].rank(pct=True)

    df["cash_rank"] = df["CFO to EBITDA"].rank(pct=True)  

    df["growth_factor"] = (
        df["sales_qoq_rank"] +
        df["sales_yoy_rank"] +
        df["eps_qoq_rank"] +
        df["eps_yoy_rank"]
    ) / 4

    df["quality_factor"] = (
        df["roe_rank"] +
        df["roce_rank"] +
        df["cash_rank"]
    ) / 3

    df["SuperScore"] = (
        df["growth_factor"] * 0.7 +
        df["quality_factor"] * 0.3
    ) * 100

    df["Verdict"] = np.select(
        [
            df["SuperScore"] >= 85,
            df["SuperScore"] >= 70,
            df["SuperScore"] >= 55
        ],
        [
            "Elite Superperformer",
            "Strong Candidate",
            "Watchlist"
        ],
        default="Weak"
    )
    df = df.sort_values("SuperScore", ascending=False)
    return df

def calculate_fundamental_score(rawDf):
    df = rawDf.copy()
    df["SalesScore"] = np.select(
        [
            df["QoQ Sales"] > 40,
            df["QoQ Sales"] > 25,
            df["QoQ Sales"] > 15
        ],
        [20, 15, 10],
        default=0
    )

    df["EPSScore"] = np.select(
        [
            df["QoQ EPS"] > 50,
            df["QoQ EPS"] > 30,
            df["QoQ EPS"] > 20
        ],
        [20, 15, 10],
        default=0
    )

    df["SalesAccelScore"] = np.where(
        df["QoQ Sales"] > df["YOY Quarterly sales growth"], 10, 0
    )

    df["EPSAccelScore"] = np.where(
        df["QoQ EPS"] > df["YoY EPS"], 10, 0
    )

    df["OPMScore"] = np.where(df["YoY OPM Expansion"] > 0, 10, 0)

    df["ReturnScore"] = np.where(
        (df["Return on equity"] > 17) &
        (df["Return on capital employed"] > 20),
        10,
        0
    )

    df["CFOScore"] = np.select(
        [
            df["CFO to EBITDA"] > 120,
            df["CFO to EBITDA"] > 100,
            df["CFO to EBITDA"] > 80,
            df["CFO to EBITDA"] > 60,
            df["CFO to EBITDA"] > 40,
            df["CFO to EBITDA"] > 20,
            df["CFO to EBITDA"] >= 0
        ],
        [20, 18, 15, 10, 5, 2, 0],
        default=-10
    )

    df["TotalScore"] = (
        df["SalesScore"]
        + df["EPSScore"]
        + df["SalesAccelScore"]
        + df["EPSAccelScore"]
        + df["OPMScore"]
        + df["ReturnScore"]
        + df["CFOScore"]
    )

    df["Verdict"] = np.select(
        [
            df["TotalScore"] >= 80,
            df["TotalScore"] >= 65,
            df["TotalScore"] >= 50
        ],
        [
            "Elite Superperformer",
            "Strong Candidate",
            "Watchlist"
        ],
        default="Weak"
    )

    df = df.sort_values("TotalScore", ascending=False)

    return df


def main():
    filepath = Config.TMP_DIR / "ai-screener.csv"
    df = pd.read_csv(filepath)
    scored_df = calculate_fundamental_score(df)
    scored_df.to_csv(Config.TMP_DIR / "fundamental_scores.csv", index=False)
    rank_df = caclulate_fundamental_rank(df)
    rank_df.to_csv(Config.TMP_DIR / "fundamental_ranks.csv", index=False)

if __name__ == "__main__":
    main()