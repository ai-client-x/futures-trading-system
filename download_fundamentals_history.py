#!/usr/bin/env python3
"""
下载历史基本面数据 - 完整版
每批只取少量股票确保获取全部历史数据
"""

import sqlite3
import tushare as ts
import pandas as pd
import time
from datetime import datetime

TOKEN = 'b9080562a895f7d37428b57a3fa21b31b17b8e922276466bfbde1021'

def init_tushare():
    ts.set_token(TOKEN)
    return ts.pro_api()

def create_table():
    conn = sqlite3.connect('data/stocks.db')
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT,
            end_date TEXT,
            roe REAL,
            netprofit_margin REAL,
            grossprofit_margin REAL,
            expense_ratio REAL,
            operate_profit_margin REAL,
            current_ratio REAL,
            quick_ratio REAL,
            debt_to_assets REAL,
            update_time TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_to_db(records):
    if not records:
        return 0
    
    conn = sqlite3.connect('data/stocks.db')
    saved = 0
    
    for r in records:
        ts_code = r.get('ts_code')
        end_date = r.get('end_date')
        
        if not ts_code or not end_date:
            continue
        
        exists = conn.execute("""
            SELECT 1 FROM fundamentals_history 
            WHERE ts_code = ? AND end_date = ?
        """, (ts_code, str(end_date))).fetchone()
        
        if not exists:
            conn.execute("""
                INSERT INTO fundamentals_history (
                    ts_code, end_date, roe, netprofit_margin, grossprofit_margin,
                    expense_ratio, operate_profit_margin, current_ratio,
                    quick_ratio, debt_to_assets, update_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ts_code, str(end_date),
                r.get('roe'), r.get('netprofit_margin'), r.get('grossprofit_margin'),
                r.get('expense_ratio'), r.get('operate_profit_margin'),
                r.get('current_ratio'), r.get('quick_ratio'),
                r.get('debt_to_assets'),
                datetime.now().isoformat()
            ))
            saved += 1
    
    conn.commit()
    conn.close()
    return saved

def main():
    print("="*60)
    print("下载历史基本面数据")
    print("="*60)
    
    pro = init_tushare()
    stocks = pro.stock_basic(exchange='', list_status='L')
    all_codes = stocks['ts_code'].tolist()
    print(f"A股总数: {len(all_codes)}")
    
    create_table()
    
    conn = sqlite3.connect('data/stocks.db')
    existing = conn.execute("SELECT COUNT(DISTINCT ts_code) FROM fundamentals_history").fetchone()[0]
    print(f"已有数据: {existing}只股票")
    
    # 跳过已有股票
    if existing > 0:
        existing_codes = [r[0] for r in conn.execute("SELECT DISTINCT ts_code FROM fundamentals_history").fetchall()]
        all_codes = [c for c in all_codes if c not in existing_codes]
        print(f"需要下载: {len(all_codes)}只")
    conn.close()
    
    if not all_codes:
        print("已全部下载完成")
        return
    
    # 每批2只股票 (每只约40个季度，2*40=80 < 100)
    batch_size = 2
    total_saved = 0
    
    for batch_start in range(0, min(2000, len(all_codes)), batch_size):  # 限制前2000只
        batch_codes = all_codes[batch_start:batch_start + batch_size]
        
        try:
            df = pro.fina_indicator(
                ts_code=','.join(batch_codes),
                start_date='20150101',
                end_date='20251231',
                fields='ts_code,end_date,roe,netprofit_margin,grossprofit_margin,expense_ratio,operate_profit_margin,current_ratio,quick_ratio,debt_to_assets'
            )
            
            if df is not None and len(df) > 0:
                # 去重
                df = df.drop_duplicates(['ts_code', 'end_date'])
                records = df.to_dict('records')
                saved = save_to_db(records)
                total_saved += saved
            
            if (batch_start // batch_size) % 50 == 0:
                print(f"进度: {batch_start + len(batch_codes)}/{min(2000, len(all_codes))}, 新增: {total_saved}")
            
        except Exception as e:
            print(f"批次错误: {e}")
        
        time.sleep(0.3)
    
    print(f"\n完成! 本次新增: {total_saved} 条")
    
    # 验证
    conn = sqlite3.connect('data/stocks.db')
    total = conn.execute("SELECT COUNT(*) FROM fundamentals_history").fetchone()[0]
    stocks_cnt = conn.execute("SELECT COUNT(DISTINCT ts_code) FROM fundamentals_history").fetchone()[0]
    print(f"总记录: {total}, 股票数: {stocks_cnt}")
    
    print("\n按年统计:")
    for year in range(2015, 2026):
        cnt = conn.execute(f"SELECT COUNT(*) FROM fundamentals_history WHERE end_date LIKE '{year}%'").fetchone()[0]
        if cnt > 0:
            print(f"  {year}年: {cnt}")
    
    conn.close()

if __name__ == "__main__":
    main()
