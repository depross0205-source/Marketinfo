import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime

def fetch_all_data():
    # 起點設為 2003-05-01 (RSP 成立日)
    start_date = "2003-05-01"
    end_date = datetime.today().strftime('%Y-%m-%d')
    print(f"🚀 開始抓取資料：{start_date} ~ {end_date}")

    # --- 1. 定義抓取工具 (確保索引為 datetime) ---
    def safe_get_yf(ticker, name):
        try:
            # 加上 auto_adjust=False 確保獲取原始 Close 價格
            df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)
            if df.empty: return pd.Series(name=name)
            # 處理 MultiIndex 標題情況
            if isinstance(df.columns, pd.MultiIndex):
                return df['Close'][ticker].rename(name)
            return df['Close'].rename(name)
        except Exception as e:
            print(f"⚠️ {name} 抓取失敗: {e}")
            return pd.Series(name=name)

    def safe_get_fred(series_id, name):
        try:
            data = web.get_data_fred(series_id, start_date, end_date)
            return data[series_id].rename(name)
        except Exception as e:
            print(f"⚠️ {name} 抓取失敗: {e}")
            return pd.Series(name=name)

    # --- 2. 執行線上資料抓取 (移除 S5TW) ---
    print("正在抓取 Yahoo Finance 資料 (SP500, SP500EW, VIX)...")
    sp500 = safe_get_yf("^GSPC", "SP500")
    sp500ew = safe_get_yf("RSP", "SP500EW")
    vix = safe_get_yf("^VIX", "VIX")

    print("正在抓取 FRED 資料 (HY_Spread, TIPS)...")
    hy_spread = safe_get_fred("BAMLH0A0HYM2", "HY_Spread")
    tips_10y = safe_get_fred("DFII10", "TIPS_10Y")

    # --- 3. 讀取並處理本地 CAPE 資料 ---
    print("正在處理本地 CAPE 資料...")
    try:
        cape_file = "以下檔案的副本： Cape.xlsx - 工作表1.csv"
        cape_df = pd.read_csv(cape_file)
        # 轉換日期格式
        cape_df['Date'] = pd.to_datetime(cape_df['Date'])
        # 提取年份和月份，用於後續合併
        cape_df['YearMonth'] = cape_df['Date'].dt.to_period('M')
        # 如果同一個月有多筆資料，取最後一筆 (或根據需求取平均)
        cape_monthly = cape_df.groupby('YearMonth')['Value'].last().rename('CAPE')
    except Exception as e:
        print(f"❌ 讀取本地 CAPE 失敗: {e}")
        cape_monthly = pd.Series()

    # --- 4. 資料合併 ---
    print("正在進行資料合併與空值填補...")
    
    # 以 SP500 的日期作為主時間軸
    main_df = pd.DataFrame(index=sp500.index)
    
    # 建立一個 YearMonth 欄位用來與 CAPE 合併
    main_df['YearMonth'] = main_df.index.to_period('M')
    
    # 逐一合併其他欄位
    main_df = main_df.join(sp500, how='left')
    main_df = main_df.join(sp500ew, how='left')
    main_df = main_df.join(vix, how='left')
    main_df = main_df.join(hy_spread, how='left')
    main_df = main_df.join(tips_10y, how='left')
    
    # 根據月份將 CAPE 數據對應過去 (確保同一個月每一天都有一樣的數值)
    main_df = main_df.merge(cape_monthly, left_on='YearMonth', right_index=True, how='left')

    # 使用 ffill() 填補其他日資料的空值 (如假日)
    # CAPE 因為是用月份 merge，理論上整個月都會有值，不需額外 ffill
    main_df = main_df.ffill()

    # 確保欄位順序 (移除 S5TW)
    cols = ['SP500', 'SP500EW', 'VIX', 'HY_Spread', 'TIPS_10Y', 'CAPE']
    main_df = main_df[cols]

    # --- 5. 產出 CSV ---
    if not main_df.dropna(subset=['SP500']).empty:
        main_df.index.name = 'Date'
        # 強制將索引轉為無時區的日期格式
        main_df.index = pd.to_datetime(main_df.index).tz_localize(None)
        main_df.to_csv("historical_data.csv")
        print(f"✅ 更新成功！目前資料筆數：{len(main_df)}")
        print(f"欄位確認：{main_df.columns.tolist()}")
    else:
        print("❌ 失敗：合併後無有效資料")

if __name__ == "__main__":
    fetch_all_data()
