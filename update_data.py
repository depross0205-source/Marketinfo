import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    # S5TW 在 Stooq 的資料約從 2005 年開始較完整
    start_date = "2005-01-01"
    end_date = datetime.today().strftime('%Y-%m-%d')

    print(f"🚀 開始全量更新，時間範圍：{start_date} 至 {end_date}")

    # 1. 抓取 Yahoo Finance 資料 (日資料)
    print("Step 1: 抓取股市日資料 (SP500, RSP, VIX)...")
    tickers = {"^GSPC": "SP500", "RSP": "SP500EW", "^VIX": "VIX"}
    yf_list = []
    for t, name in tickers.items():
        data = yf.download(t, start=start_date, end=end_date)['Close']
        yf_list.append(pd.Series(data, name=name))
    
    # 2. 抓取 FRED 數據 (含利差、TIPS、CAPE)
    print("Step 2: 抓取 FRED 總經數據 (含 CAPE 估值)...")
    try:
        # FRED 代碼說明：BAMLH0A0HYM2(利差), DFII10(TIPS), CAPE(席勒本益比)
        fred_data = web.get_data_fred(["BAMLH0A0HYM2", "DFII10", "CAPE"], start_date, end_date)
        fred_data.columns = ['HY_Spread', 'TIPS_10Y', 'CAPE']
    except Exception as e:
        print(f"❌ FRED 抓取失敗: {e}")
        fred_data = pd.DataFrame()

    # 3. 抓取 S5TW (從 Stooq 資料庫)
    print("Step 3: 抓取市場寬度 S5TW (20MA Breadth)...")
    try:
        # Stooq 代碼為 ^S5TW
        s5tw = web.DataReader("^S5TW", "stooq", start_date, end_date)['Close']
        s5tw.name = "S5TW"
    except Exception as e:
        print(f"❌ S5TW 抓取失敗: {e}")
        s5tw = pd.Series(name="S5TW")

    # 4. 資料合併與對齊
    print("Step 4: 正在進行資料對齊與清洗...")
    df = pd.concat(yf_list, axis=1)
    df = df.join(fred_data, how='left')
    df = df.join(s5tw, how='left')

    # 使用 ffill 填補假日與每月更新的 CAPE，確保每天都有數值
    df = df.ffill().dropna()

    # 5. 輸出 CSV
    if not df.empty:
        df.index.name = 'Date'
        df.to_csv("historical_data.csv")
        print(f"✅ 成功！已產出 {len(df)} 筆資料。")
        print(f"最後更新日期：{df.index[-1]}")
    else:
        print("⚠️ 警告：產出的資料表為空。")

if __name__ == "__main__":
    fetch_all_data()
