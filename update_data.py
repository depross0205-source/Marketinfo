import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime
import os

def fetch_all_data():
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 啟動全自動對齊程序：{start_date} ~ {end_date}")

    # 1. 抓取線上日資料
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

    # 2. 智慧型 CAPE 處理 (不依賴欄位名稱)
    cape_lookup = pd.Series()
    try:
        target_file = next((f for f in os.listdir('.') if f.lower().startswith('cape') and f.endswith('.csv')), None)
        if target_file:
            print(f"🎯 讀取檔案: {target_file}")
            # 讀取 CSV，不假設有標題
            raw_df = pd.read_csv(target_file, header=None)
            
            # 智慧判斷：哪一欄是日期？哪一欄是數值？
            col_date, col_val = None, None
            
            for col in raw_df.columns:
                # 嘗試解析該欄位的第 2 列數據 (避開標題)
                sample = str(raw_df[col].iloc[1] if len(raw_df) > 1 else raw_df[col].iloc[0])
                # 如果包含月份單字或斜線，判定為日期
                if any(m in sample.lower() for m in ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec','/','-']):
                    col_date = col
                # 如果可以轉為數字，判定為數值
                try:
                    float(sample.replace(',',''))
                    if col_val is None: col_val = col
                except: pass

            if col_date is not None and col_val is not None:
                print(f"✅ 自動識別：第 {col_date+1} 欄為日期，第 {col_val+1} 欄為數值")
                
                # 清洗數據
                cape_df = pd.DataFrame({
                    'Date': pd.to_datetime(raw_df[col_date], errors='coerce'),
                    'Value': pd.to_numeric(raw_df[col_val], errors='coerce')
                }).dropna()
                
                cape_df['YM'] = cape_df['Date'].dt.to_period('M')
                cape_lookup = cape_df.groupby('YM')['Value'].last().rename('CAPE')
            else:
                print("❌ 無法識別 CSV 欄位，請確認內容格式")
    except Exception as e:
        print(f"❌ CAPE 處理失敗: {e}")

    # 3. 合併與填補
    main_df = pd.DataFrame(index=sp500.index)
    main_df['YM'] = main_df.index.to_period('M')
    main_df = main_df.join([sp500, sp500ew, vix, hy, tips])
    main_df = main_df.merge(cape_lookup, left_on='YM', right_index=True, how='left')
    main_df = main_df.ffill().bfill()

    # 4. 輸出 CSV
    cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE']
    final_csv = main_df[cols]
    final_csv.index.name = 'Date'
    final_csv.index = pd.to_datetime(final_csv.index).tz_localize(None)
    final_csv.to_csv("historical_data.csv")
    print(f"✅ 更新成功！目前資料總數：{len(final_csv)}")

if __name__ == "__main__":
    fetch_all_data()
