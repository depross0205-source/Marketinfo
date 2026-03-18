import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime
import os

def fetch_all_data():
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 啟動強化清洗程序：{start_date} ~ {end_date}")

    # 1. 抓取線上資料
    def get_yf_fix(ticker, name):
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)
            ser = df['Close'][ticker] if isinstance(df.columns, pd.MultiIndex) else df['Close']
            return ser.squeeze().rename(name)
        except: return pd.Series(name=name)

    sp500, sp500ew, vix = get_yf_fix("^GSPC", "SP500"), get_yf_fix("RSP", "SP500EW"), get_yf_fix("^VIX", "VIX")
    try:
        hy = web.get_data_fred("BAMLH0A0HYM2", start_date, end_date)["BAMLH0A0HYM2"].rename("HY_Spread")
        tips = web.get_data_fred("DFII10", start_date, end_date)["DFII10"].rename("TIPS_10Y")
    except:
        hy = pd.Series(name="HY_Spread"); tips = pd.Series(name="TIPS_10Y")

    # 2. 鋼鐵級 CAPE 處理邏輯
    cape_lookup = pd.Series()
    try:
        target_file = next((f for f in os.listdir('.') if f.lower().startswith('cape') and f.endswith('.csv')), None)
        if target_file:
            print(f"🎯 讀取檔案: {target_file}")
            # 讀取時自動處理可能的編碼問題
            cape_df = pd.read_csv(target_file, skipinitialspace=True)
            
            # 【解決問題 2】自動修復欄位名稱（移除空格、轉為首字母大寫）
            cape_df.columns = [str(c).strip().capitalize() for c in cape_df.columns]
            
            if 'Date' in cape_df.columns:
                # 【解決問題 1】暴力清理日期字串中的所有隱藏字元
                cape_df['Date'] = cape_df['Date'].astype(str).str.replace(r'[^\w\s,]', '', regex=True).str.strip()
                
                # 【解決日期解析錯誤】使用最保險的模糊解析，並強制 errors='coerce'
                cape_df['Date'] = pd.to_datetime(cape_df['Date'], errors='coerce')
                
                # 移除解析失敗的行
                cape_df = cape_df.dropna(subset=['Date'])
                cape_df['YM'] = cape_df['Date'].dt.to_period('M')
                cape_lookup = cape_df.groupby('YM')['Value'].last().rename('CAPE')
            else:
                print(f"❌ 找不到 'Date' 欄位，現有欄位為: {cape_df.columns.tolist()}")
    except Exception as e:
        print(f"❌ CAPE 處理失敗: {e}")

    # 3. 合併與輸出
    main_df = pd.DataFrame(index=sp500.index)
    main_df['YM'] = main_df.index.to_period('M')
    main_df = main_df.join([sp500, sp500ew, vix, hy, tips])
    main_df = main_df.merge(cape_lookup, left_on='YM', right_index=True, how='left').ffill()

    cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE']
    final_output = main_df[cols]
    final_output.index.name = 'Date'
    final_output.index = pd.to_datetime(final_output.index).tz_localize(None)
    final_output.to_csv("historical_data.csv")
    print(f"✅ 更新完成！最後日期: {final_output.index[-1]}")

if __name__ == "__main__":
    fetch_all_data()
