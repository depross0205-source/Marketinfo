import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 啟動終極資料補全程序：{start_date} ~ {end_date}")

    # --- 1. 定義標準化處理函式 ---
    def to_clean_df(obj, name):
        if obj is None or (isinstance(obj, (pd.Series, pd.DataFrame)) and obj.empty):
            return pd.DataFrame(columns=[name])
        # 轉為 DataFrame
        df = obj.to_frame(name) if isinstance(obj, pd.Series) else obj
        # 強制轉為「純日期字串」格式，這是解決對不準最有效的方法
        df.index = pd.to_datetime(df.index).strftime('%Y-%m-%d')
        df = df[~df.index.duplicated(keep='first')]
        return df

    # --- 2. 抓取線上資料 ---
    print("正在抓取 Yahoo Finance (SP500, RSP, VIX)...")
    try:
        yf_raw = yf.download(["^GSPC", "RSP", "^VIX"], start=start_date, end=end_date, progress=False)['Close']
        sp500 = to_clean_df(yf_raw['^GSPC'], "SP500")
        sp500ew = to_clean_df(yf_raw['RSP'], "SP500EW")
        vix = to_clean_df(yf_raw['^VIX'], "VIX")
    except:
        sp500 = sp500ew = vix = pd.DataFrame()

    print("正在抓取 FRED (利差, TIPS)...")
    try:
        # BAMLH0A0HYM2 = HY_Spread, DFII10 = TIPS_10Y
        fred_raw = web.get_data_fred(["BAMLH0A0HYM2", "DFII10"], start_date, end_date)
        fred_raw.columns = ["HY_Spread", "TIPS_10Y"]
        fred = to_clean_df(fred_raw, "")
    except:
        fred = pd.DataFrame(columns=["HY_Spread", "TIPS_10Y"])

    # --- 3. 處理本地 CAPE 資料 ---
    print("正在處理本地 CAPE CSV 檔案...")
    try:
        # 請確保此檔名與你上傳到 GitHub 的檔名完全一致
        cape_file = "以下檔案的副本： Cape.xlsx - 工作表1.csv"
        cape_local = pd.read_csv(cape_file)
        cape_local['Date'] = pd.to_datetime(cape_local['Date']).dt.strftime('%Y-%m')
        # 每月取最後一筆值
        cape_map = cape_local.groupby('Date')['Value'].last()
    except Exception as e:
        print(f"❌ CAPE 檔案讀取失敗: {e}")
        cape_map = pd.Series()

    # --- 4. 終極合併 ---
    print("執行強制合併程序...")
    # 以 SP500 的日期為基底
    main_df = sp500.copy()
    
    # 合併其餘日資料
    for other in [sp500ew, vix, fred]:
        main_df = main_df.merge(other, left_index=True, right_index=True, how='left')

    # 處理 CAPE (根據年-月匹配)
    main_df['YearMonth'] = pd.to_datetime(main_df.index).strftime('%Y-%m')
    main_df['CAPE'] = main_df['YearMonth'].map(cape_map)

    # --- 5. 數據填補 (ffill) ---
    # 確保假日或 CAPE 沒更新的日子會自動帶入前值
    main_df = main_df.ffill().bfill()

    # 整理最終欄位順序
    cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE']
    final_output = main_df[cols]

    # --- 6. 產出 CSV ---
    final_output.index.name = 'Date'
    final_output.to_csv("historical_data.csv")
    
    print(f"✅ 更新完成！產出 {len(final_output)} 筆資料。")
    print(f"最後五筆預覽：\n{final_output.tail()}")

if __name__ == "__main__":
    fetch_all_data()
