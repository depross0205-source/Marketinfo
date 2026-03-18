import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    start = "2003-05-01"
    end = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 啟動標準化同步：{start} ~ {end}")

    # 1. 抓取 Yahoo Finance 資料
    def get_yf(ticker, name):
        df = yf.download(ticker, start=start, end=end, progress=False)
        # 處理 yfinance 可能產生的多層索引，確保只取 Close 數值
        ser = df['Close'][ticker] if isinstance(df.columns, pd.MultiIndex) else df['Close']
        return ser.squeeze().rename(name)

    sp500 = get_yf("^GSPC", "SP500")
    sp500ew = get_yf("RSP", "SP500EW")
    vix = get_yf("^VIX", "VIX")

    # 2. 抓取 FRED 資料
    try:
        fred = web.get_data_fred(["BAMLH0A0HYM2", "DFII10"], start, end)
        fred.columns = ["HY_Spread", "TIPS_10Y"]
    except:
        fred = pd.DataFrame(columns=["HY_Spread", "TIPS_10Y"])

    # 3. 讀取標準化 cape.csv
    try:
        cape_df = pd.read_csv("cape.csv", parse_dates=['Date'])
        # 轉換為「月份週期」以便對齊
        cape_df['YM'] = cape_df['Date'].dt.to_period('M')
        cape_lookup = cape_df.groupby('YM')['Value'].last().rename('CAPE')
    except:
        cape_lookup = pd.Series(name="CAPE")

    # 4. 核心合併邏輯
    # 以標普 500 的交易日期為基底
    main_df = pd.DataFrame(index=sp500.index)
    main_df['YM'] = main_df.index.to_period('M')
    
    # 併入日資料
    main_df = main_df.join([sp500, sp500ew, vix, fred])
    
    # 併入月資料 (CAPE)
    main_df = main_df.merge(cape_lookup, left_on='YM', right_index=True, how='left')

    # 5. 資料填補與輸出 (讓每個月的每一天都擁有當月 CAPE 數值)
    final = main_df.ffill().bfill().drop(columns=['YM'])
    
    final.index.name = 'Date'
    final.to_csv("historical_data.csv")
    print(f"✅ 同步完成！目前資料表最後五筆：\n{final.tail()}")

if __name__ == "__main__":
    fetch_all_data()
