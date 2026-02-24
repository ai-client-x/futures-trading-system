#!/usr/bin/env python3
"""
修复数据库中的 amount 字段
使用 Tushare 重新下载所有年份的数据
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
    """获取所有需要修复的股票代码"""
    conn = sqlite3.connect('data/stocks.db')
    
    # 获取所有 amount 为 NULL 的股票
    df = pd.read_sql("""
        SELECT DISTINCT ts_code 
        FROM daily 
        WHERE amount IS NULL
    """, conn)
    
    conn.close()
    return df['ts_code'].tolist()

def download_and_update(pro, ts_code, start_date='20100101', end_date='20251231'):
    """下载数据并更新数据库"""
    try:
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if df is None or len(df) == 0:
            return 0
        
        # 更新数据库
        conn = sqlite3.connect('data/stocks.db')
        
        updated = 0
        for _, row in df.iterrows():
            trade_date = row['trade_date']
            amount = row.get('amount')
            
            if amount is not None and amount > 0:
                conn.execute("""
                    UPDATE daily 
                    SET amount = ? 
                    WHERE ts_code = ? AND trade_date = ?
                """, (amount, ts_code, trade_date))
                updated += 1
        
        conn.commit()
        conn.close()
        
        return updated
        
    except Exception as e:
        print(f"  Error {ts_code}: {e}")
        return 0

def main():
    print("="*60)
    print("修复 amount 字段 (所有年份)")
    print("="*60)
    
    # 获取需要修复的股票
    codes = get_all_codes()
    print(f"需要修复的股票: {len(codes)} 只")
    
    if not codes:
        print("没有需要修复的数据")
        return
    
    # 检查剩余NULL数量
    conn = sqlite3.connect('data/stocks.db')
    cnt_before = conn.execute("SELECT COUNT(*) FROM daily WHERE amount IS NULL").fetchone()[0]
    print(f"修复前 amount 为 NULL: {cnt_before}")
    conn.close()
    
    # 初始化 Tushare
    pro = init_tushare()
    
    # 逐个下载更新
    total_updated = 0
    for i, code in enumerate(codes):
        if i % 10 == 0:
            print(f"进度: {i}/{len(codes)}")
        
        cnt = download_and_update(pro, code)
        if cnt > 0:
            total_updated += cnt
        
        # 避免请求过快
        time.sleep(0.2)
    
    print(f"\n完成! 更新了 {total_updated} 条数据")
    
    # 验证结果
    conn = sqlite3.connect('data/stocks.db')
    cnt_after = conn.execute("""
        SELECT COUNT(*) FROM daily WHERE amount IS NULL
    """).fetchone()[0]
    
    print(f"修复后 amount 为 NULL: {cnt_after}")
    print(f"共修复: {cnt_before - cnt_after} 条")
    
    # 按年统计
    print("\n按年统计 amount 为 NULL 的记录:")
    for year in ['2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024', '2025']:
        cnt = conn.execute(f"""
            SELECT COUNT(*) FROM daily WHERE trade_date LIKE '{year}%' AND amount IS NULL
        """).fetchone()[0]
        if cnt > 0:
            print(f"  {year}年: {cnt}")
    
    conn.close()

if __name__ == "__main__":
    main()
