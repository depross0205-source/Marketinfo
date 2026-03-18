import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    start_date = "2005-01-01"
    end_date = datetime.today().strftime('%Y-%m-%d')

    print(f"🚀 開始全量更新，時間範圍：{start_date} 至 {end_date}")

    # --- Step 1: 抓取 Yahoo Finance 資料 (加入防呆機制) ---
    def get_yf_safe(ticker, name):
        print(f"正在抓取 {name} ({ticker})...")
        try:
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)
            # 檢查是否為空或是 None
            if data is None or data.empty:
                print(f"⚠️ 警告：{ticker} 沒抓到資料")
                return pd.Series(name=name, dtype='float64')
            
            # 處理 yfinance 可能產生的多層索引 (MultiIndex)
            if isinstance(data.columns, pd.MultiIndex):
                return data['Close'][ticker].rename(name)
            return data['Close'].rename(name)
        except Exception as e:
            print(f"❌ {ticker} 抓取時發生錯誤: {e}")
            return pd.Series(name=name, dtype='float64')

    sp500 = get_yf_safe("^GSPC", "SP500")
    rsp = get_yf_safe("RSP", "SP500EW")
    vix = get_yf_safe("^VIX", "VIX")
    
    # --- Step 2: 抓取 FRED 數據 ---
    print("Step 2: 抓取 FRED 總經數據 (利差, TIPS, CAPE)...")
    try:
        fred_data = web.get_data_fred(["BAMLH0A0HYM2", "DFII10", "CAPE"], start_date, end_date)
        fred_data.columns = ['HY_Spread', 'TIPS_10Y', 'CAPE']
    except Exception as e:
        print(f"❌ FRED 抓取失敗: {e}")
        fred_data = pd.DataFrame()

    # --- Step 3: 抓取 S5TW (Stooq) ---
    print("Step 3: 抓取市場寬度 S5TW...")
    try:
        s5tw_df = web.DataReader("^S5TW", "stooq", start_date, end_date)
        if not s5tw_df.empty:
            s5tw = s5tw_df['Close'].sort_index().rename("S5TW")
        else:
            s5tw = pd.Series(name="S5TW", dtype='float64')
    except Exception as e:
        print(f"❌ S5TW 抓取失敗: {e}")
        s5tw = pd.Series(name="S5TW", dtype='float64')

    # --- Step 4: 資料合併 ---
    print("Step 4: 正在進行資料對齊與清洗...")
    # 先合併 Yahoo 資料
    df = pd.concat([sp500, rsp, vix], axis=1)
    # 再合併其他資料
    df = df.join(fred_data, how='left')
    df = df.join(s5tw, how='left')

    # 使用 ffill 填補假日
    df = df.ffill().dropna()

    if not df.empty:
        df.index.name = 'Date'
        df.to_csv("historical_data.csv")
        print(f"✅ 成功！已產出 {len(df)} 筆資料。")
        print(f"最新一筆數據日期：{df.index[-1]}")
    else:
        print("⚠️ 錯誤：最終合併資料表為空，請手動檢查資料源。")

if __name__ == "__main__":
    fetch_all_data()
