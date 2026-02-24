#!/usr/bin/env python3
"""
CSV数据转换为SQLite数据库 - 优化版
使用批量插入提高速度
"""

import os
import sqlite3
import pandas as pd
import glob


def csv_to_sqlite_fast(csv_dir: str, db_path: str = "data/stocks.db"):
    """快速转换CSV到SQLite"""
    
    print("="*60)
    print("📦 CSV → SQLite 转换 (优化版)")
    print("="*60)
    
    # 删除旧数据库
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"已删除旧数据库: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 创建表
    cursor.execute("""
        CREATE TABLE daily (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            pre_close REAL,
            change REAL,
            pct_chg REAL,
            vol REAL,
            amount REAL,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE stocks (
            ts_code TEXT PRIMARY KEY,
            name TEXT,
            market TEXT
        )
    """)
    
    # 创建索引
    cursor.execute("CREATE INDEX idx_code_date ON daily(ts_code, trade_date)")
    
    conn.commit()
    
    csv_files = glob.glob(f"{csv_dir}/*.csv")
    print(f"📂 找到 {len(csv_files)} 个CSV文件")
    
    total_rows = 0
    
    for i, filepath in enumerate(csv_files):
        filename = os.path.basename(filepath)
        parts = filename.replace('.csv', '').split('_')
        ts_code = parts[0]
        name = '_'.join(parts[1:])
        market = 'SH' if '.SH' in ts_code else 'SZ'
        
        try:
            # 读取CSV
            df = pd.read_csv(filepath)
            
            # 转换日期
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
            
            # 添加ts_code
            df['ts_code'] = ts_code
            
            # 批量插入股票信息
            cursor.execute("INSERT OR REPLACE INTO stocks VALUES (?, ?, ?)", 
                         (ts_code, name, market))
            
            # 批量插入行情数据
            cols = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 
                   'pre_close', 'change', 'pct_chg', 'vol', 'amount']
            df_to_insert = df[cols].dropna(subset=['trade_date'])
            
            # 使用INSERT OR REPLACE处理重复
            for _, row in df_to_insert.iterrows():
                try:
                    cursor.execute(f"""
                        INSERT OR REPLACE INTO daily 
                        VALUES ({','.join(['?']*len(cols))})
                    """, tuple(row))
                except:
                    pass
            
            total_rows += len(df)
            
            if (i + 1) % 500 == 0:
                conn.commit()
                print(f"  进度: {i+1}/{len(csv_files)}, 已导入 {total_rows:,} 行")
                
        except Exception as e:
            print(f"  ✗ {filename}: {e}")
    
    conn.commit()
    
    # 统计
    cursor.execute("SELECT COUNT(*) FROM daily")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM stocks")
    stock_count = cursor.fetchone()[0]
    
    conn.close()
    
    # 结果
    csv_size = sum(os.path.getsize(f) for f in csv_files) / 1024 / 1024
    db_size = os.path.getsize(db_path) / 1024 / 1024
    
    print("\n" + "="*60)
    print("✅ 转换完成!")
    print("="*60)
    print(f"股票数量: {stock_count}")
    print(f"行情数据: {total:,} 条")
    print(f"CSV大小: {csv_size:.1f} MB")
    print(f"SQLite大小: {db_size:.1f} MB")
    print(f"压缩率: {(1-db_size/csv_size)*100:.1f}%")
    
    return db_path


if __name__ == "__main__":
    csv_to_sqlite_fast("data/tushare_all", "data/stocks.db")
