import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    # 起點設為 2003-05-01 (RSP 成立日，確保資料長度最大化)
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 開始全量抓取：{start_date} ~ {end_date}")

    # --- 1. 定義抓取工具 (確保索引為 datetime) ---
    def safe_get_yf(ticker, name):
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if df.empty: return pd.Series(name=name)
            if isinstance(df.columns, pd.MultiIndex):
                return df['Close'][ticker].rename(name)
            return df['Close'].rename(name)
        except: return pd.Series(name=name)

    def safe_get_fred(series_id, name):
        try:
            data = web.get_data_fred(series_id, start_date, end_date)
            return data[series_id].rename(name)
        except: return pd.Series(name=name)

    # --- 2. 執行抓取 ---
    print("正在抓取 Yahoo Finance 資料...")
    sp500 = safe_get_yf("^GSPC", "SP500")
    sp500ew = safe_get_yf("RSP", "SP500EW")
    vix = safe_get_yf("^VIX", "VIX")

    print("正在抓取 FRED 資料 (利差, TIPS, CAPE)...")
    hy_spread = safe_get_fred("BAMLH0A0HYM2", "HY_Spread")
    tips_10y = safe_get_fred("DFII10", "TIPS_10Y")
    cape = safe_get_fred("CAPE", "CAPE")

    print("正在抓取 Stooq 資料 (S5TW)...")
    try:
        s5tw_df = web.DataReader("^S5TW", "stooq", start_date, end_date)
        s5tw = s5tw_df['Close'].sort_index().rename("S5TW")
    except:
        s5tw = pd.Series(name="S5TW")

    # --- 3. 強制合併與「前值填補 (ffill)」 ---
    # 這是解決空格的最關鍵步驟
    print("正在進行資料合併與空值填補...")
    
    # 以 SP500 的日期作為主時間軸
    main_df = pd.DataFrame(index=sp500.index)
    
    # 逐一合併
    for s in [sp500, sp500ew, vix, hy_spread, tips_10y, cape, s5tw]:
        main_df = main_df.join(s, how='left')

    # 關鍵：使用 ffill() 把每月更新的 CAPE 和假日沒開市的利差「往下填滿」
    # 這樣每一天都會有最新的可用數據，而不會是空格
    main_df = main_df.ffill()

    # 確保欄位順序
    cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE', 'S5TW']
    main_df = main_df[cols]

    # --- 4. 產出 CSV ---
    if not main_df.dropna(subset=['SP500']).empty:
        main_df.index.name = 'Date'
        main_df.to_csv("historical_data.csv")
        print(f"✅ 更新成功！目前資料筆數：{len(main_df)}")
        print(f"欄位確認：{main_df.columns.tolist()}")
    else:
        print("❌ 失敗：合併後無有效資料")

if __name__ == "__main__":
    fetch_all_data()
