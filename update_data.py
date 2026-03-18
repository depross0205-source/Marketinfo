import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime
import os

def fetch_all_data():
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 啟動強化解析程序：{start_date} ~ {end_date}")

    # 1. 抓取 Yahoo Finance 日資料
    def get_yf_fix(ticker, name):
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)
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

    # 3. 讀取並精準解析 CAPE 檔案 (解決 Mar 17 錯誤)
    print("正在精準對齊 CAPE 數據格式...")
    cape_lookup = pd.Series()
    try:
        target_file = None
        for f in os.listdir('.'):
            if f.lower().startswith('cape') and f.endswith('.csv'):
                target_file = f
                break
        
        if target_file:
            print(f"🎯 找到檔案: {target_file}")
            cape_df = pd.read_csv(target_file)
            
            # 【核心修復】強制指定日期格式為：月份(簡寫) 日期, 年份
            # 這能解決 Mar 17 被誤認為 17 月的問題
            cape_df['Date'] = pd.to_datetime(
                cape_df['Date'].str.strip(), 
                format='%b %d, %Y', 
                errors='coerce'
            )
            
            # 萬一還有沒抓到的格式，進行第二次模糊解析補漏
            missing = cape_df['Date'].isna()
            if missing.any():
                cape_df.loc[missing, 'Date'] = pd.to_datetime(
                    cape_df.loc[missing, 'Date'], 
                    errors='coerce'
                )

            # 移除無效列並對齊月份
            cape_df = cape_df.dropna(subset=['Date'])
            cape_df['YM'] = cape_df['Date'].dt.to_period('M')
            cape_lookup = cape_df.groupby('YM')['Value'].last().rename('CAPE')
        else:
            print("❌ 找不到開頭為 'cape' 的 CSV 檔案")
    except Exception as e:
        print(f"❌ CAPE 處理失敗: {e}")

    # 4. 合併與填補
    main_df = pd.DataFrame(index=sp500.index)
    main_df['YM'] = main_df.index.to_period('M')
    main_df = main_df.join([sp500, sp500ew, vix, hy, tips])
    main_df = main_df.merge(cape_lookup, left_on='YM', right_index=True, how='left')
    main_df = main_df.ffill()

    # 5. 輸出 CSV
    cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE']
    final_csv = main_df[cols]
    final_csv.index.name = 'Date'
    final_csv.index = pd.to_datetime(final_csv.index).tz_localize(None)
    final_csv.to_csv("historical_data.csv")
    print(f"✅ 任務圓滿完成！")

if __name__ == "__main__":
    fetch_all_data()
