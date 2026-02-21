def naver_stock_url(ticker: str) -> str:
    t = str(ticker).zfill(6)
    return f"https://finance.naver.com/item/main.naver?code={t}"