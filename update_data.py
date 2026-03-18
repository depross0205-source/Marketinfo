import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    # 起點設為 2003-01-01，這是 RSP 與 TIPS 數據開始完整的邊界
    start_date = "2003-01-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"開始抓取長週期資料：{start_date} ~ {end_date}")

    # --- 1. 定義安全抓取函式 ---
    def get_yf_safe(ticker, label):
        try:
            print(f"正在抓取 {label} ({ticker})...")
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if df.empty: return pd.Series(name=label, dtype='float64')
            
            # 處理 yfinance 可能產生的 MultiIndex 標題 (常見報錯點)
            if isinstance(df.columns, pd.MultiIndex):
                ser = df['Close'][ticker]
            else:
                ser = df['Close']
            return ser.rename(label)
        except Exception as e:
            print(f"⚠️ {label} 抓取失敗: {e}")
            return pd.Series(name=label, dtype='float64')

    # --- 2. 抓取所有資料源 ---
    # Yahoo Finance 數據
    sp500 = get_yf_safe("^GSPC", "SP500")
    sp500ew = get_yf_safe("RSP", "SP500EW")
    vix = get_yf_safe("^VIX", "VIX")

    # FRED 數據 (利差, TIPS, CAPE)
    print("正在從 FRED 抓取總經數據...")
    try:
        fred = web.get_data_fred(["BAMLH0A0HYM2", "DFII10", "CAPE"], start_date, end_date)
        fred.columns = ['HY_Spread', 'TIPS_10Y', 'CAPE']
    except Exception as e:
        print(f"⚠️ FRED 失敗: {e}")
        fred = pd.DataFrame(columns=['HY_Spread', 'TIPS_10Y', 'CAPE'])

    # Stooq 數據 (S5TW)
    print("正在從 Stooq 抓取 S5TW...")
    try:
        s5tw_df = web.DataReader("^S5TW", "stooq", start_date, end_date)
        s5tw = s5tw_df['Close'].sort_index().rename("S5TW")
    except Exception as e:
        print(f"⚠️ S5TW 失敗: {e}")
        s5tw = pd.Series(name="S5TW", dtype='float64')

    # --- 3. 合併資料 (以 Date 為準對齊) ---
    print("正在合併與清洗資料...")
    # 先建立以 SP500 日期為基準的 DataFrame
    main_df = pd.DataFrame(index=sp500.index)
    
    # 依序合併
    main_df = main_df.join([sp500, sp500ew, vix, fred, s5tw], how='left')
    
    # 修正欄位順序以符合要求
    target_cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE', 'S5TW']
    # 補齊缺失欄位(避免程式崩潰)
    for col in target_cols:
        if col not in main_df.columns:
            main_df[col] = pd.NA

    # --- 4. 數據清洗：ffill (前值填補) 是回測最重要的步驟 ---
    # 它可以把「每月更新的 CAPE」與「假日不開盤的利差」填滿每一天
    main_df = main_df.ffill()
    
    # 刪除沒有 SP500 報價的無效日期 (通常是週六日)
    main_df = main_df.dropna(subset=['SP500'])

    # --- 5. 存檔 ---
    if not main_df.empty:
        main_df.index.name = 'Date'
        main_df[target_cols].to_csv("historical_data.csv")
        print(f"✅ 成功更新！總天數：{len(main_df)}")
        print(f"資料範例：\n{main_df[target_cols].tail(3)}")
    else:
        print("❌ 錯誤：合併後的資料表是空的！")

if __name__ == "__main__":
    fetch_all_data()
