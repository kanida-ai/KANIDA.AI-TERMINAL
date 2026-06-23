"""
Preserve the v1 analysis to Excel before the v2 (two-stage, open-confirmed) rebuild.

Writes a single timestamped workbook with one sheet per artefact plus computed
summaries. Read-only on the DB.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from persona_engine import db

OUT_DIR = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\persona_v1")


def _df(con, sql):
    return pd.read_sql_query(sql, con)


def export(stamp: str) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"persona_v1_analysis_{stamp}.xlsx"
    con = db.connect(read_only=True)
    try:
        sheets = {}

        # by-year summaries (the headline numbers)
        sheets["fo_summary_by_year"] = _df(con, """
            SELECT substr(prediction_date,1,4) year, direction,
                   ROUND(AVG(hit)*10.0,3) avg_hits_of_10,
                   ROUND(AVG(hit)*100.0,2) hit_pct, COUNT(*) n
            FROM fo_prediction_outcomes GROUP BY year, direction ORDER BY year, direction""")
        sheets["lt_summary_by_year"] = _df(con, """
            SELECT substr(prediction_date,1,4) year,
                   ROUND(AVG(in_top10_4wk)*100.0,2) hit4_pct,
                   ROUND(AVG(in_top10_8wk)*100.0,2) hit8_pct,
                   ROUND(AVG(ret_4wk),2) avg_ret_4wk, ROUND(AVG(ret_8wk),2) avg_ret_8wk,
                   COUNT(*) n
            FROM lt_prediction_outcomes GROUP BY year ORDER BY year""")

        # raw artefacts
        sheets["fo_daily_predictions"] = _df(con, "SELECT * FROM fo_daily_predictions ORDER BY prediction_date,direction,rank")
        sheets["fo_prediction_outcomes"] = _df(con, "SELECT * FROM fo_prediction_outcomes ORDER BY prediction_date,direction,predicted_rank")
        sheets["lt_daily_predictions"] = _df(con, "SELECT * FROM lt_daily_predictions ORDER BY prediction_date,rank")
        sheets["lt_prediction_outcomes"] = _df(con, "SELECT * FROM lt_prediction_outcomes ORDER BY prediction_date,predicted_rank")
        sheets["fo_miss_analysis"] = _df(con, "SELECT * FROM fo_miss_analysis ORDER BY outcome_date,direction,actual_rank")
        sheets["lt_miss_analysis"] = _df(con, "SELECT * FROM lt_miss_analysis ORDER BY prediction_date,window,actual_rank")
        sheets["learning_proposals"] = _df(con, "SELECT * FROM learning_proposals ORDER BY persona,hit_rate DESC")
        sheets["fo_daily_review"] = _df(con, "SELECT prediction_date,outcome_date,long_hit,short_hit,combined_hit,review_text FROM fo_daily_review ORDER BY prediction_date")
        sheets["persona_model_weights"] = _df(con, "SELECT * FROM persona_model_weights ORDER BY persona,feature_name")

        with pd.ExcelWriter(path, engine="openpyxl") as xl:
            for name, d in sheets.items():
                # Excel hard cap safety
                d.head(1_000_000).to_excel(xl, sheet_name=name[:31], index=False)
        print(f"wrote {path}  ({len(sheets)} sheets)")
        for name, d in sheets.items():
            print(f"  {name:26s} {len(d):>7d} rows")
        return path
    finally:
        con.close()


if __name__ == "__main__":
    stamp = sys.argv[1] if len(sys.argv) > 1 else "snapshot"
    export(stamp)
