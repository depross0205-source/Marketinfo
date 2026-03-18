import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    # 起點 2003 年，涵蓋最長週期
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 啟動終極資料補全程序：{start_date} ~ {end_date}")

    # --- 1. 定義標準化處理函式 (解決數據消失的核心) ---
    def to_clean_df(obj, name):
        if obj is None or (isinstance(obj, (pd.Series, pd.DataFrame)) and obj.empty):
            return pd.DataFrame(columns=[name])
        # 轉為 DataFrame
        df = obj.to_frame(name) if isinstance(obj, pd.Series) else obj
        # 強制轉為「純日期字串」格式 (YYYY-MM-DD)，消除時區干擾
        df.index = pd.to_datetime(df.index).strftime('%Y-%m-%d')
        # 移除重複索引
        df = df[~df.index.duplicated(keep='first')]
        return df

    # --- 2. 抓取線上資料 (Yahoo Finance & FRED) ---
    print("正在抓取 Yahoo Finance (SP500, RSP, VIX)...")
    try:
        yf_raw = yf.download(["^GSPC", "RSP", "^VIX"], start=start_date, end=end_date, progress=False)['Close']
        sp500 = to_clean_df(yf_raw['^GSPC'], "SP500")
        sp500ew = to_clean_df(yf_raw['RSP'], "SP500EW")
        vix = to_clean_df(yf_raw['^VIX'], "VIX")
    except Exception as e:
        print(f"Yahoo 抓取失敗: {e}")
        sp500 = sp500ew = vix = pd.DataFrame()

    print("正在抓取 FRED (利差, TIPS)...")
    try:
        # BAMLH0A0HYM2 = HY_Spread, DFII10 = TIPS_10Y
        fred_raw = web.get_data_fred(["BAMLH0A0HYM2", "DFII10"], start_date, end_date)
        fred_raw.columns = ["HY_Spread", "TIPS_10Y"]
        fred = to_clean_df(fred_raw, "")
    except Exception as e:
        print(f"FRED 抓取失敗: {e}")
        fred = pd.DataFrame(columns=["HY_Spread", "TIPS_10Y"])

    # --- 3. 處理本地 CAPE CSV 檔案 ---
    print("正在讀取本地 CAPE 檔案...")
    try:
        # 檔名精確對齊：Cape.xlsx - 工作表1.csv
        cape_file = "Cape.xlsx - 工作表1.csv"
        cape_local = pd.read_csv(cape_file)
        # 將 CSV 的日期轉為 年-月 字串，方便日資料對應
        cape_local['Date'] = pd.to_datetime(cape_local['Date']).dt.strftime('%Y-%m')
        # 如果同月有多筆，取最後一筆數值
        cape_map = cape_local.groupby('Date')['Value'].last()
    except Exception as e:
        print(f"❌ CAPE 檔案讀取失敗: {e}")
        cape_map = pd.Series()

    # --- 4. 終極合併程序 ---
    print("執行強制對齊合併...")
    # 以標普 500 的日期作為主軸
    main_df = sp500.copy()
    
    # 合併日資料 (RSP, VIX, HY_Spread, TIPS)
    for other in [sp500ew, vix, fred]:
        main_df = main_df.merge(other, left_index=True, right_index=True, how='left')

    # 處理 CAPE：根據每一列的日期對應到 YearMonth
    main_df['YearMonth'] = pd.to_datetime(main_df.index).strftime('%Y-%m')
    main_df['CAPE'] = main_df['YearMonth'].map(cape_map)

    # --- 5. 填補數據 (解決假日與月資料空格) ---
    # ffill() 讓同一個月的每一天都擁有當月的 CAPE 數值
    main_df = main_df.ffill().bfill()

    # 整理最終 6 個欄位 (已移除 S5TW)
    target_cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE']
    final_output = main_df[target_cols]

    # --- 6. 產出 CSV 檔案 ---
    final_output.index.name = 'Date'
    final_output.to_csv("historical_data.csv")
    
    print(f"✅ 更新成功！目前產出 {len(final_output)} 筆資料。")
    print(f"最後三筆數據預覽：\n{final_output.tail(3)}")

if __name__ == "__main__":
    fetch_all_data()
