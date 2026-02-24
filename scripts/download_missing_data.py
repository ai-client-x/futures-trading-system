#!/usr/bin/env python3
"""
下载缺失的历史股票数据
2015-2018年数据不足，需要补充
"""

import sqlite3
import tushare as ts
import pandas as pd
import time

TOKEN = 'b9080562a895f7d37428b57a3fa21b31b17b8e922276466bfbde1021'

def init_tushare():
    ts.set_token(TOKEN)
    return ts.pro_api()

def get_all_codes():
    """获取A股全部股票列表"""
    pro = init_tushare()
    df = pro.stock_basic(exchange='', list_status='L')
    return df['ts_code'].tolist()

def download_stock_data(pro, ts_code, start_date='20150101', end_date='20181231'):
    """下载单只股票历史数据"""
    try:
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df
    except Exception as e:
        return None

def save_to_db(df, ts_code):
    """保存到数据库"""
    if df is None or len(df) == 0:
        return 0
    
    conn = sqlite3.connect('data/stocks.db')
    cursor = conn.cursor()
    
    saved = 0
    for _, row in df.iterrows():
        trade_date = row['trade_date']
        amount = row.get('amount')
        
        # 检查是否已存在
        exists = cursor.execute("""
            SELECT 1 FROM daily WHERE ts_code = ? AND trade_date = ?
        """, (ts_code, trade_date)).fetchone()
        
        if not exists:
            cursor.execute("""
                INSERT INTO daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ts_code, trade_date,
                row.get('open'), row.get('high'), row.get('low'), row.get('close'),
                row.get('pre_close'), row.get('change'), row.get('pct_chg'),
                row.get('vol'), amount
            ))
            saved += 1
    
    conn.commit()
    conn.close()
    return saved

def main():
    print("="*60)
    print("下载缺失的股票历史数据 (2015-2018)")
    print("="*60)
    
    # 获取A股列表
    codes = get_all_codes()
    print(f"A股总数: {len(codes)}")
    
    # 检查当前数据
    conn = sqlite3.connect('data/stocks.db')
    
    # 找出2019年有数据的股票（作为基准）
    codes_2019 = set([row[0] for row in conn.execute(
        "SELECT DISTINCT ts_code FROM daily WHERE trade_date LIKE '2019%'"
    ).fetchall()])
    
    print(f"2019年有数据的股票: {len(codes_2019)}")
    conn.close()
    
    # 需要下载的股票
    codes_to_download = list(codes_2019)
    print(f"需要下载: {len(codes_to_download)}")
    
    if not codes_to_download:
        print("没有需要下载的数据")
        return
    
    pro = init_tushare()
    
    total_saved = 0
    for i, code in enumerate(codes_to_download):
        if i % 50 == 0:
            print(f"进度: {i}/{len(codes_to_download)}")
        
        df = download_stock_data(pro, code, '20150101', '20181231')
        if df is not None and len(df) > 0:
            saved = save_to_db(df, code)
            total_saved += saved
        
        time.sleep(0.15)  # 避免请求过快
    
    print(f"\n完成! 共保存 {total_saved} 条数据")
    
    # 验证结果
    conn = sqlite3.connect('data/stocks.db')
    for year in ['2015', '2016', '2017', '2018']:
        cnt = conn.execute(f"SELECT COUNT(DISTINCT ts_code) FROM daily WHERE trade_date LIKE '{year}%'").fetchone()[0]
        print(f"{year}年: {cnt} 只股票")
    conn.close()

if __name__ == "__main__":
    main()
