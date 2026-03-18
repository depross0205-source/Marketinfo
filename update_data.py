import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime
import time

def fetch_all_data():
    # 設定起點為 2003-05-01 (RSP 與 TIPS 的最早交集點)
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 開始抓取長週期資料：{start_date} ~ {end_date}")

    # --- 1. 抓取 Yahoo Finance 資料 ---
    def get_yf(ticker, name):
        try:
            # 抓取原始資料，不進行自動調整以維持數據原始性
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if df.empty: return pd.Series(name=name)
            # 處理新版 yfinance 可能產生的 MultiIndex 標題
            if isinstance(df.columns, pd.MultiIndex):
                return df['Close'][ticker].rename(name)
            return df['Close'].rename(name)
        except:
            return pd.Series(name=name)

    print("正在下載：SP500, SP500EW, VIX...")
    sp500 = get_yf("^GSPC", "SP500")
    sp500ew = get_yf("RSP", "SP500EW")
    vix = get_yf("^VIX", "VIX")

    # --- 2. 抓取 FRED 資料 ---
    print("正在下載：HY_Spread, TIPS_10Y, CAPE...")
    try:
        # FRED 代碼：BAMLH0A0HYM2 (利差), DFII10 (10Y TIPS), CAPE (席勒本益比)
        fred = web.get_data_fred(["BAMLH0A0HYM2", "DFII10", "CAPE"], start_date, end_date)
        fred.columns = ['HY_Spread', 'TIPS_10Y', 'CAPE']
    except:
        fred = pd.DataFrame()

    # --- 3. 抓取 Stooq 資料 (S5TW) ---
    print("正在下載：S5TW (20MA Breadth)...")
    try:
        # ^S5TW 為標普500成分股站上20日線比例
        s5tw_raw = web.DataReader("^S5TW", "stooq", start_date, end_date)
        s5tw = s5tw_raw['Close'].sort_index().rename("S5TW")
    except:
        s5tw = pd.Series(name="S5TW")

    # --- 4. 合併所有資料 (Outer Join 確保長度最大化) ---
    print("正在進行多源資料對齊...")
    # 以 SP500 為主軸合併所有數據
    main_df = pd.DataFrame(index=sp500.index)
    main_df = main_df.join([sp500, sp500ew, vix, fred, s5tw], how='outer')

    # --- 5. 資料清洗 ---
    # 1. 填補空值 (ffill): 處理假日與每月更新的 CAPE
    # 2. 刪除沒有 SP500 報價的日期
    main_df = main_df.ffill().dropna(subset=['SP500'])

    # --- 6. 產出 CSV ---
    if not main_df.empty:
        main_df.index.name = 'Date'
        # 確保欄位順序正確
        cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE', 'S5TW']
        main_df = main_df[cols]
        main_df.to_csv("historical_data.csv")
        print(f"✅ 成功！產出週期：{main_df.index[0].date()} 至 {main_df.index[-1].date()}")
        print(f"總筆數：{len(main_df)}")
    else:
        print("❌ 錯誤：合併後無資料")

if __name__ == "__main__":
    fetch_all_data()
