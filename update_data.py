import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime
import time

def fetch_all_data():
    # 設定回測起點：RSP (等權重) 最早資料約從 2003-05 開始
    start_date = "2004-01-01"
    end_date = datetime.today().strftime('%Y-%m-%d')

    print(f"正在抓取資料，範圍：{start_date} 至 {end_date}")

    # 1. 抓取 Yahoo Finance 資料 (採單個抓取以避免 Multi-index 造成合併錯誤)
    def get_yf(ticker):
        data = yf.download(ticker, start=start_date, end=end_date)
        return data['Close'] if not data.empty else pd.Series()

    sp500 = get_yf("^GSPC")
    rsp = get_yf("RSP")
    vix = get_yf("^VIX")

    # 2. 抓取 FRED 資料 (高收益債利差與 10年期 TIPS)
    try:
        fred_data = web.get_data_fred(["BAMLH0A0HYM2", "DFII10"], start_date, end_date)
        fred_data.columns = ['HY_Spread', 'TIPS_10Y']
    except Exception as e:
        print(f"FRED 抓取失敗: {e}")
        fred_data = pd.DataFrame()

    # 3. 資料對齊與合併
    df = pd.DataFrame(index=sp500.index)
    df['SP500'] = sp500
    df['SP500EW'] = rsp
    df['VIX'] = vix
    
    if not fred_data.empty:
        df = df.join(fred_data, how='left')
    
    # 4. 資料清洗
    # 使用 ffill() 填補假日 (例如週六日的利差會引用週五的數值)
    df = df.ffill().dropna()

    if df.empty:
        print("錯誤：最終資料表為空，請檢查網路或代碼。")
        return

    # 5. 儲存檔案
    df.index.name = 'Date'
    df.to_csv("historical_data.csv")
    print(f"成功！已產出 {len(df)} 筆資料，最後更新日期：{df.index[-1]}")

if __name__ == "__main__":
    fetch_all_data()
