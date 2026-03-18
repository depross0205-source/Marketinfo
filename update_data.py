import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 啟動終極對齊程序：{start_date} ~ {end_date}")

    # --- 1. 統一格式化函式 ---
    def to_standard_df(df_or_ser, name):
        if df_or_ser is None or (isinstance(df_or_ser, (pd.Series, pd.DataFrame)) and df_or_ser.empty):
            print(f"⚠️ {name} 抓取結果為空")
            return pd.DataFrame(columns=[name])
        
        # 轉為 DataFrame
        temp_df = df_or_ser.to_frame(name) if isinstance(df_or_ser, pd.Series) else df_or_ser
        
        # 【關鍵步驟】強制將索引轉為字串格式 'YYYY-MM-DD'，徹底消除時區與時間戳干擾
        temp_df.index = pd.to_datetime(temp_df.index).strftime('%Y-%m-%d')
        # 移除重複日期 (如果有)
        temp_df = temp_df[~temp_df.index.duplicated(keep='first')]
        return temp_df

    # --- 2. 抓取數據 ---
    print("正在抓取 Yahoo Finance...")
    try:
        yf_raw = yf.download(["^GSPC", "RSP", "^VIX"], start=start_date, end=end_date, progress=False)['Close']
        # 處理 yfinance 可能的 MultiIndex
        sp500 = to_standard_df(yf_raw['^GSPC'] if '^GSPC' in yf_raw else None, "SP500")
        sp500ew = to_standard_df(yf_raw['RSP'] if 'RSP' in yf_raw else None, "SP500EW")
        vix = to_standard_df(yf_raw['^VIX'] if '^VIX' in yf_raw else None, "VIX")
    except:
        sp500, sp500ew, vix = [pd.DataFrame(columns=[c]) for c in ["SP500", "SP500EW", "VIX"]]

    print("正在抓取 FRED (利差, TIPS, CAPE)...")
    try:
        # FRED 數據：BAMLH0A0HYM2(利差), DFII10(TIPS), CAPE(席勒本益比)
        fred_raw = web.get_data_fred(["BAMLH0A0HYM2", "DFII10", "CAPE"], start_date, end_date)
        fred_raw.columns = ['HY_Spread', 'TIPS_10Y', 'CAPE']
        fred = to_standard_df(fred_raw, "") # 這裡傳空字串因為已有 columns
    except:
        fred = pd.DataFrame(columns=['HY_Spread', 'TIPS_10Y', 'CAPE'])

    print("正在抓取 Stooq (S5TW)...")
    try:
        s5tw_raw = web.DataReader("^S5TW", "stooq", start_date, end_date)
        s5tw = to_standard_df(s5tw_raw['Close'], "S5TW")
    except:
        s5tw = pd.DataFrame(columns=["S5TW"])

    # --- 3. 合併與填補 ---
    print("執行強制對齊合併...")
    # 以 SP500 的日期作為主軸
    final_df = sp500.copy()
    
    # 逐一合併其他表
    for other in [sp500ew, vix, fred, s5tw]:
        final_df = final_df.merge(other, left_index=True, right_index=True, how='left')

    # 排序日期 (字串排序與日期排序一致)
    final_df = final_df.sort_index()

    # 執行前值填補 (ffill) 解決 CAPE 月更新問題
    final_df = final_df.ffill()

    # --- 4. 產出與驗證 ---
    target_cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE', 'S5TW']
    # 補齊可能缺失的欄位
    for col in target_cols:
        if col not in final_df.columns:
            final_df[col] = ""

    final_df = final_df[target_cols]
    final_df.index.name = 'Date'
    
    # 存檔
    final_df.to_csv("historical_data.csv")
    print(f"✅ 更新完成！存儲於 historical_data.csv，共 {len(final_df)} 筆數據。")
    print(f"數據預覽：\n{final_df.tail(3)}")

if __name__ == "__main__":
    fetch_all_data()
