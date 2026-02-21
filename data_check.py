import pandas as pd

df = pd.read_csv("data/kospi_top200_1y_daily_20260221_1y_20260221_233428.csv")
print("005930 존재 여부:", "005930" in df["ticker"].astype(str).str.zfill(6).unique())
print("티커 개수:", df["ticker"].nunique())