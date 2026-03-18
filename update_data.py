import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    # 設定起點為 2003 年 5 月
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')

    print("抓取 Yahoo Finance 資料 (SP500, SP500EW, VIX)...")
    tickers = ["^GSPC", "RSP", "^VIX"]
    # 抓取收盤價
    yf_data = yf.download(tickers, start=start_date, end=end_date)['Close']
    # 確保欄位順序對應 RSP, ^GSPC, ^VIX
    yf_data.columns = ['SP500EW', 'SP500', 'VIX']

    print("抓取 FRED 資料 (高收益債利差, TIPS_10Y)...")
    fred_data = web.DataReader(["BAMLH0A0HYM2", "DFII10"], "fred", start_date, end_date)
    fred_data.columns = ['HY_Spread', 'TIPS_10Y']

    print("合併資料並處理假日空缺...")
    # 使用 outer join 保留所有日期，並用前一日的數據填補假日空值
    full_data = yf_data.join(fred_data, how='outer').fillna(method='ffill')

    # 命名索引並輸出
    full_data.index.name = 'Date'
    full_data.to_csv("historical_data.csv")
    print("完成！已儲存為 historical_data.csv")

if __name__ == "__main__":
    fetch_all_data()
