import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    # 設定最長回測週期
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 開始抓取完整資料庫：{start_date} ~ {end_date}")

    # --- 1. 定義防錯抓取與索引歸一化工具 ---
    def clean_index(df_or_ser):
        """將所有索引轉為無時區的 datetime 格式"""
        if df_or_ser is None or (isinstance(df_or_ser, (pd.Series, pd.DataFrame)) and df_or_ser.empty):
            return df_or_ser
        df_or_ser.index = pd.to_datetime(df_or_ser.index).tz_localize(None)
        return df_or_ser

    def get_yf_stable(ticker, name):
        try:
            print(f"正在抓取 {name}...")
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if df.empty: return pd.Series(name=name, dtype='float64')
            
            # 處理 MultiIndex 格式
            ser = df['Close'][ticker] if isinstance(df.columns, pd.MultiIndex) else df['Close']
            return clean_index(ser).rename(name)
        except:
            return pd.Series(name=name, dtype='float64')

    # --- 2. 下載資料 ---
    # 下載股市資料
    sp500 = get_yf_stable("^GSPC", "SP500")
    sp500ew = get_yf_stable("RSP", "SP500EW")
    vix = get_yf_stable("^VIX", "VIX")

    # 下載 FRED 總經資料
    print("正在抓取 FRED 總經數據 (利差, TIPS, CAPE)...")
    try:
        # FRED 數據代碼：BAMLH0A0HYM2 (利差), DFII10 (10Y TIPS), CAPE (席勒本益比)
        fred = web.get_data_fred(["BAMLH0A0HYM2", "DFII10", "CAPE"], start_date, end_date)
        fred = clean_index(fred)
        fred.columns = ['HY_Spread', 'TIPS_10Y', 'CAPE']
    except Exception as e:
        print(f"⚠️ FRED 失敗: {e}")
        fred = pd.DataFrame(columns=['HY_Spread', 'TIPS_10Y', 'CAPE'])

    # 下載 Stooq 資料
    print("正在抓取 Stooq 市場寬度 (S5TW)...")
    try:
        # ^S5TW 為標普500成分股站上20日線比例
        s5tw_raw = web.DataReader("^S5TW", "stooq", start_date, end_date)
        s5tw = clean_index(s5tw_raw['Close']).sort_index().rename("S5TW")
    except:
        s5tw = pd.Series(name="S5TW", dtype='float64')

    # --- 3. 終極合併與填補 ---
    print("對齊 8 個欄位中...")
    # 建立主表
    main_df = pd.DataFrame(index=sp500.index)
    
    # 逐一併入 (使用 left join 確保以 SP500 交易日為準)
    for s in [sp500, sp500ew, vix, fred, s5tw]:
        if isinstance(s, (pd.Series, pd.DataFrame)):
            main_df = main_df.join(s, how='left')

    # 執行前值填補 (ffill) 解決 CAPE 月更新與 API 缺漏問題
    main_df = main_df.ffill()

    # 確保欄位排序與完整性
    target_cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE', 'S5TW']
    for col in target_cols:
        if col not in main_df.columns:
            main_df[col] = pd.NA

    # --- 4. 產出 CSV ---
    main_df.index.name = 'Date'
    main_df[target_cols].to_csv("historical_data.csv")
    print(f"✅ 任務成功！產出檔案：historical_data.csv (共 {len(main_df)} 筆)")

if __name__ == "__main__":
    fetch_all_data()
