import pandas as pd
from pathlib import Path
p = Path(__file__).resolve().parent.parent / "outputs" / "Falcon_Intraday_Backtest_Results.xlsx"
xl = pd.ExcelFile(p)
print("SHEETS:", xl.sheet_names)
for s in ["3_Time_Of_Day", "4_Year_Breakdown", "5_Rank_Contribution"]:
    print(f"\n===== {s} =====")
    print(pd.read_excel(p, sheet_name=s).to_string(index=False))
print("\n===== 2_Daily_Trade_Log (first 4 rows) =====")
print(pd.read_excel(p, sheet_name="2_Daily_Trade_Log").head(4).to_string(index=False))
print("\n===== 6_Parity_Checks =====")
print(pd.read_excel(p, sheet_name="6_Parity_Checks").to_string(index=False))
