import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    # 稍微縮短一點年份確保 RSP 資料是完整的 (從 2004 開始最穩)
    start_date = "2004-01-01"
    end_date = datetime.today().strftime('%Y-%m-%d')

    print("Step 1: 抓取 Yahoo Finance 資料...")
    # 分開抓取以確保資料都有抓到
    sp500 = yf.download("^GSPC", start=start_date, end=end_date)['Close']
    rsp = yf.download("RSP", start=start_date, end=end_date)['Close']
    vix = yf.download("^VIX", start=start_date, end=end_date)['Close']

    print("Step 2: 抓取 FRED 資料...")
    # 改用 get_data_fred
    fred_data = web.get_data_fred(["BAMLH0A0HYM2", "DFII10"], start_date, end_date)
    
    print("Step 3: 合併資料...")
    # 建立一個 DataFrame 並統一命名欄位
    df = pd.DataFrame(index=sp500.index)
    df['SP500'] = sp500
    df['SP500EW'] = rsp
    df['VIX'] = vix
    
    # 合併 FRED
    df = df.join(fred_data, how='left')
    df.columns = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y']

    # 填補假日空值 (ffill) 並刪除完全沒資料的列
    df = df.ffill().dropna()

    if df.empty:
        print("警告：合併後的資料是空的！請檢查來源。")
    else:
        df.index.name = 'Date'
        df.to_csv("historical_data.csv")
        print(f"成功！已儲存 {len(df)} 筆資料到 historical_data.csv")

if __name__ == "__main__":
    fetch_all_data()
