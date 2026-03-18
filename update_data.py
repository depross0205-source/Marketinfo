import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime
import os

def fetch_all_data():
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 啟動格式對齊程序：{start_date} ~ {end_date}")

    # 1. 抓取 Yahoo Finance 日資料
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

    # 2. 處理本地 CAPE 檔案 (解決日期格式與合併錯誤)
    cape_lookup = pd.Series()
    try:
        target_file = next((f for f in os.listdir('.') if f.lower().startswith('cape') and f.endswith('.csv')), None)
        if target_file:
            print(f"🎯 讀取檔案: {target_file}")
            cape_df = pd.read_csv(target_file)
            
            # 強制解析日期 (處理 Mar 17, 2026 格式)
            # 我們不指定格式，讓 pandas 自動偵測，但加入 errors='coerce' 避免崩潰
            cape_df['Date_Fixed'] = pd.to_datetime(cape_df.iloc[:, 0], errors='coerce')
            
            # 找到數值欄位 (通常是第二欄)
            val_col = cape_df.iloc[:, 1]
            
            # 將日期轉換為 Period['M'] (月份格式)，這是合併成功的關鍵
            cape_df['YM'] = cape_df['Date_Fixed'].dt.to_period('M')
            
            # 建立以「月份」為索引的查詢表
            cape_lookup = cape_df.dropna(subset=['YM']).groupby('YM').last().iloc[:, 1]
            cape_lookup.name = 'CAPE'
            print("✅ CAPE 月份對齊完成")
        else:
            print("❌ 找不到 CAPE 檔案")
    except Exception as e:
        print(f"❌ CAPE 處理失敗: {e}")

    # 3. 合併資料 (核心修正：確保兩邊都是 Period['M'])
    main_df = pd.DataFrame(index=sp500.index)
    
    # 建立左邊的月份索引 (Period)
    main_df['YM'] = main_df.index.to_period('M')
    
    # 合併日資料
    main_df = main_df.join([sp500, sp500ew, vix, hy, tips])
    
    # 合併 CAPE (左邊是 YM 欄位，右邊是索引，且兩者都是 Period[M] 類型)
    main_df = main_df.merge(cape_lookup, left_on='YM', right_index=True, how='left')

    # 填補數據
    main_df = main_df.ffill().bfill()

    # 4. 輸出 CSV
    cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE']
    final_csv = main_df[cols]
    final_csv.index.name = 'Date'
    final_csv.index = pd.to_datetime(final_csv.index).tz_localize(None)
    final_csv.to_csv("historical_data.csv")
    print(f"✅ 更新完成！")

if __name__ == "__main__":
    fetch_all_data()
