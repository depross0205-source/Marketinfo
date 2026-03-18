import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    # 設定起點為 2003-05-01 (RSP 成立日，確保資料長度最大化)
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 開始全量抓取：{start_date} ~ {end_date}")

    # --- 1. 定義抓取工具 (強制移除時區資訊，解決合併空格問題) ---
    def get_yf_safe(ticker, name):
        try:
            print(f"正在抓取 {name} ({ticker})...")
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if df.empty: return pd.Series(name=name, dtype='float64')
            
            # 取得收盤價
            if isinstance(df.columns, pd.MultiIndex):
                ser = df['Close'][ticker]
            else:
                ser = df['Close']
            
            # 強制移除時區，這是對齊 FRED 數據的關鍵
            if ser.index.tz is not None:
                ser.index = ser.index.tz_localize(None)
            return ser.rename(name)
        except Exception as e:
            print(f"⚠️ {name} 失敗: {e}")
            return pd.Series(name=name, dtype='float64')

    # --- 2. 執行抓取 ---
    sp500 = get_yf_safe("^GSPC", "SP500")
    sp500ew = get_yf_safe("RSP", "SP500EW")
    vix = get_yf_safe("^VIX", "VIX")

    print("正在從 FRED 抓取總經數據 (HY_Spread, TIPS, CAPE)...")
    try:
        # FRED 數據預設無時區
        fred = web.get_data_fred(["BAMLH0A0HYM2", "DFII10", "CAPE"], start_date, end_date)
        fred.columns = ['HY_Spread', 'TIPS_10Y', 'CAPE']
    except Exception as e:
        print(f"⚠️ FRED 抓取失敗: {e}")
        fred = pd.DataFrame(columns=['HY_Spread', 'TIPS_10Y', 'CAPE'])

    print("正在從 Stooq 抓取 S5TW (市場寬度)...")
    try:
        s5tw_df = web.DataReader("^S5TW", "stooq", start_date, end_date)
        s5tw = s5tw_df['Close'].sort_index().rename("S5TW")
        # 移除時區
        if s5tw.index.tz is not None:
            s5tw.index = s5tw.index.tz_localize(None)
        else:
            s5tw.index = pd.to_datetime(s5tw.index)
    except Exception as e:
        print(f"⚠️ S5TW 抓取失敗: {e}")
        s5tw = pd.Series(name="S5TW", dtype='float64')

    # --- 3. 強制對齊與合併 ---
    print("正在進行資料合併與空值填補...")
    # 以 SP500 的交易日作為主時間軸
    main_df = pd.DataFrame(index=sp500.index)
    
    # 逐一合併所有 Series
    for s in [sp500, sp500ew, vix, fred, s5tw]:
        main_df = main_df.join(s, how='left')

    # --- 4. 數據清洗 (解決每月更新導致的空格問題) ---
    # 使用 ffill() 把每月更新的 CAPE 和假日沒開市的利差「往下填滿」
    main_df = main_df.ffill()

    # 確保所有要求的 8 個欄位順序正確
    target_cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE', 'S5TW']
    
    # 檢查是否有漏掉的欄位並補空值，避免 CSV 結構出錯
    for col in target_cols:
        if col not in main_df.columns:
            main_df[col] = pd.NA

    # --- 5. 輸出 CSV ---
    main_df.index.name = 'Date'
    main_df[target_cols].to_csv("historical_data.csv")
    print(f"✅ 更新完成！目前資料總筆數：{len(main_df)}")
    print(f"最後一筆資料日期：{main_df.index[-1]}")

if __name__ == "__main__":
    fetch_all_data()
