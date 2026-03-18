import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    start_date = "2005-01-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"執行時間：{datetime.now()}，範圍：{start_date} ~ {end_date}")

    # --- 1. 定義防掉包抓取函式 ---
    def get_yf_data(ticker, label):
        try:
            print(f"正在抓取 {label} ({ticker})...")
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if df.empty:
                print(f"⚠️ {ticker} 回傳空值")
                return pd.Series(name=label)
            # 處理 yfinance 新版可能產生的多層索引 (MultiIndex)
            if isinstance(df.columns, pd.MultiIndex):
                return df['Close'][ticker].rename(label)
            return df['Close'].rename(label)
        except Exception as e:
            print(f"❌ {ticker} 失敗: {e}")
            return pd.Series(name=label)

    # --- 2. 抓取各項資料 ---
    sp500 = get_yf_data("^GSPC", "SP500")
    rsp = get_yf_data("RSP", "SP500EW")
    vix = get_yf_data("^VIX", "VIX")
    
    print("正在抓取 FRED 資料 (利差, TIPS, CAPE)...")
    try:
        fred = web.get_data_fred(["BAMLH0A0HYM2", "DFII10", "CAPE"], start_date, end_date)
        fred.columns = ['HY_Spread', 'TIPS_10Y', 'CAPE']
    except:
        print("⚠️ FRED 抓取失敗")
        fred = pd.DataFrame()

    print("正在抓取 Stooq 資料 (S5TW)...")
    try:
        s5tw_raw = web.DataReader("^S5TW", "stooq", start_date, end_date)
        s5tw = s5tw_raw['Close'].sort_index().rename("S5TW")
    except:
        print("⚠️ S5TW 抓取失敗")
        s5tw = pd.Series(name="S5TW")

    # --- 3. 合併與清洗 ---
    print("正在合併資料...")
    final_df = pd.DataFrame(index=sp500.index)
    final_df = final_df.join([rsp, vix, fred, s5tw], how='left')
    final_df['SP500'] = sp500 # 確保主標竿存在

    # 填補假日並刪除無效列
    final_df = final_df.ffill().dropna(subset=['SP500'])

    # --- 4. 存檔檢查 ---
    if not final_df.empty:
        final_df.index.name = 'Date'
        final_df.to_csv("historical_data.csv")
        print(f"✅ 成功！產出 {len(final_df)} 筆資料，最後日期為 {final_df.index[-1]}")
    else:
        print("❌ 失敗：合併後的資料表是空的！")

if __name__ == "__main__":
    fetch_all_data()
