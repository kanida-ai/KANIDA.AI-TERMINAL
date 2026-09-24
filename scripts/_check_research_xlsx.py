import pandas as pd
from pathlib import Path
p = Path(__file__).resolve().parent.parent / "outputs" / "Falcon_95pct_Research.xlsx"
pd.set_option("display.width", 200); pd.set_option("display.max_columns", 30)
xl = pd.ExcelFile(p)
print("SHEETS:", xl.sheet_names)
print("\n===== 2_Single_Filter_Scan (top 20) =====")
print(pd.read_excel(p, "2_Single_Filter_Scan").to_string(index=False))
print("\n===== 3_Two_Filter =====")
print(pd.read_excel(p, "3_Two_Filter").to_string(index=False))
print("\n===== 4_Three_Filter =====")
print(pd.read_excel(p, "4_Three_Filter").to_string(index=False))
print("\n===== 7_Combined_Matrix =====")
print(pd.read_excel(p, "7_Combined_Matrix").to_string(index=False))
print("\n===== 8_Year_By_Year (best combo) =====")
print(pd.read_excel(p, "8_Year_By_Year").to_string(index=False))
print("\n===== 10_Miss_Day_Analysis (+ N-floor table below) =====")
print(pd.read_excel(p, "10_Miss_Day_Analysis").to_string(index=False))
