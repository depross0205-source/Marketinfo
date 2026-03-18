import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    # 起點 2003 年，涵蓋最長週期
    start = "2003-05-01"
    end = datetime.today().strftime('%Y-%m-%d')
    print(f"執行更新中: {start} ~ {end}")

    # --- 1. 抓取資料 ---
    # 分開抓取 Yahoo Finance 避免標題結構出錯
    print("正在抓取 Yahoo Finance (SP500, RSP, VIX)...")
    sp500_raw = yf.download("^GSPC", start=start, end=end, progress=False)['Close']
    rsp_raw = yf.download("RSP", start=start, end=end, progress=False)['Close']
    vix_raw = yf.download("^VIX", start=start, end=end, progress=False)['Close']

    print("正在抓取 FRED (利差, TIPS, CAPE)...")
    # 包含 HY_Spread, TIPS_10Y, CAPE
    fred = web.get_data_fred(["BAMLH0A0HYM2", "DFII10", "CAPE"], start, end)

    print("正在抓取 Stooq (S5TW)...")
    # ^S5TW 為市場寬度指標
    try:
        s5tw_raw = web.DataReader("^S5TW", "stooq", start, end)['Close']
    except:
        print("S5TW 暫時無法抓取，將由前值填補")
        s5tw_raw = pd.Series()

    # --- 2. 強制統一日期格式 (解決數據消失的核心) ---
    def force_clean(data):
        if data is None or (isinstance(data, (pd.Series, pd.DataFrame)) and data.empty):
            return data
        # 全部轉為「無時區」的日期，並只保留日期部分
        data.index = pd.to_datetime(data.index).tz_localize(None).normalize()
        return data

    sp500 = force_clean(sp500_raw)
    rsp = force_clean(rsp_raw)
    vix = force_clean(vix_raw)
    fred = force_clean(fred)
    s5tw = force_clean(s5tw_raw).sort_index()

    # --- 3. 合併資料 ---
    # 建立主時間軸
    df = pd.DataFrame(index=sp500.index)
    
    # 填入數據 (使用指派方式最穩定，不會因為 Join 失敗而消失)
    df['SP500'] = sp500
    df['SP500EW'] = rsp
    df['VIX'] = vix
    df['HY_Spread'] = fred['BAMLH0A0HYM2']
    df['TIPS_10Y'] = fred['DFII10']
    df['CAPE'] = fred['CAPE']
    df['S5TW'] = s5tw

    # --- 4. 關鍵補救：前值填補 ---
    # CAPE 每月才一個點，利差假日沒資料，必須填補才能在回測中完整顯示
    df = df.ffill()

    # 確保 8 個欄位順序
    cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE', 'S5TW']
    df = df[cols]

    # --- 5. 存檔 ---
    df.index.name = "Date"
    df.to_csv("historical_data.csv")
    print(f"✅ 更新完成，共產出 {len(df)} 筆資料")

if __name__ == "__main__":
    fetch_all_data()
