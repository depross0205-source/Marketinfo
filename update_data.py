import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 啟動轉檔程序：{start_date} ~ {end_date}")

    # 1. 安全抓取日資料 (修正維度錯誤)
    def get_yf_fix(ticker, name):
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)
            # 關鍵修正：確保只取出 Close 欄位並轉為 1D Series
            if isinstance(df.columns, pd.MultiIndex):
                ser = df['Close'][ticker]
            else:
                ser = df['Close']
            return ser.squeeze().rename(name)
        except: return pd.Series(name=name)

    sp500 = get_yf_fix("^GSPC", "SP500")
    sp500ew = get_yf_fix("RSP", "SP500EW")
    vix = get_yf_fix("^VIX", "VIX")

    # 2. 抓取 FRED 資料
    try:
        hy = web.get_data_fred("BAMLH0A0HYM2", start_date, end_date)["BAMLH0A0HYM2"].rename("HY_Spread")
        tips = web.get_data_fred("DFII10", start_date, end_date)["DFII10"].rename("TIPS_10Y")
    except:
        hy = pd.Series(name="HY_Spread"); tips = pd.Series(name="TIPS_10Y")

    # 3. 讀取並轉換本地 Cape.csv
    print("正在轉換本地 Cape.csv...")
    try:
        cape_df = pd.read_csv("Cape.csv")
        cape_df['Date'] = pd.to_datetime(cape_df['Date'])
        # 將日期格式化為「年-月」
        cape_df['YM'] = cape_df['Date'].dt.to_period('M')
        # 同月份的每一天都設為同一個數值
        cape_lookup = cape_df.groupby('YM')['Value'].last().rename('cape')
    except Exception as e:
        print(f"❌ CAPE 讀取失敗: {e}")
        cape_lookup = pd.Series()

    # 4. 終極合併
    main_df = pd.DataFrame(index=sp500.index)
    main_df['YM'] = main_df.index.to_period('M')
    
    # 併入所有指標
    main_df = main_df.join([sp500, sp500ew, vix, hy, tips])
    # 根據月份對齊 CAPE
    main_df = main_df.merge(cape_lookup, left_on='YM', right_index=True, how='left')

    # 填補空值 (讓週末或假日也有資料)
    main_df = main_df.ffill()

    # 5. 輸出結果
    cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE']
    final_csv = main_df[cols]
    final_csv.index.name = 'Date'
    # 移除時區資訊確保 Excel 能讀
    final_csv.index = pd.to_datetime(final_csv.index).tz_localize(None)
    final_csv.to_csv("historical_data.csv")
    print(f"✅ 轉檔完成！總共產出 {len(final_csv)} 筆數據。")

if __name__ == "__main__":
    fetch_all_data()
