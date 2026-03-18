import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')

    print("抓取 Yahoo Finance...")
    tickers = ["^GSPC", "RSP", "^VIX"]
    yf_data = yf.download(tickers, start=start_date, end=end_date)['Close']
    yf_data.columns = ['SP500EW', 'SP500', 'VIX']

    print("抓取 FRED...")
    # 改用 get_data_fred 確保相容性
    fred_data = web.get_data_fred(["BAMLH0A0HYM2", "DFII10"], start_date, end_date)
    fred_data.columns = ['HY_Spread', 'TIPS_10Y']

    print("合併中...")
    # 使用 ffill() 填補假日
    full_data = yf_data.join(fred_data, how='outer').ffill()
    full_data.index.name = 'Date'
    
    # 存檔
    full_data.to_csv("historical_data.csv")
    print("成功生成 historical_data.csv")

if __name__ == "__main__":
    fetch_all_data()
