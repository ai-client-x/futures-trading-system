#!/usr/bin/env python3
"""
补全基本面财务数据
获取更多股票的ROE等财务指标
"""

import os
import sys
import time
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Tushare Token
TUSHARE_TOKEN = os.environ.get('TUSHARE_TOKEN', 'b9080562a895f7d37428b57a3fa21b31b17b8e922276466bfbde1021')

def download_financial_data():
    """下载财务指标数据"""
    import tushare as ts
    import pandas as pd
    import sqlite3
    
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    
    conn = sqlite3.connect('data/stocks.db')
    
    # 获取已有ROE的股票
    cursor = conn.cursor()
    cursor.execute('SELECT ts_code FROM fundamentals WHERE roe IS NOT NULL')
    existing = set(row[0] for row in cursor.fetchall())
    logger.info(f"已有财务数据: {len(existing)} 只")
    
    # 获取所有股票
    cursor.execute('SELECT ts_code FROM fundamentals')
    all_stocks = [row[0] for row in cursor.fetchall()]
    
    # 需要获取财务数据的股票
    to_fetch = [s for s in all_stocks if s not in existing]
    logger.info(f"需要获取财务数据: {len(to_fetch)} 只")
    
    # 批量获取财务指标
    all_data = []
    batch_size = 500
    
    for i in range(0, len(to_fetch), batch_size):
        batch = to_fetch[i:i+batch_size]
        codes = ','.join(batch)
        
        try:
            df = pro.fina_indicator(
                ts_code=codes,
                fields='ts_code,end_date,roe,netprofit_margin,grossprofit_margin,expense_ratio,operate_profit_margin,current_ratio,quick_ratio,debt_to_assets,debt_to_equity'
            )
            
            if df is not None and len(df) > 0:
                # 取最新报告期
                df = df.sort_values('end_date', ascending=False).drop_duplicates('ts_code', keep='first')
                all_data.append(df)
                logger.info(f"获取: {min(i+batch_size, len(to_fetch))}/{len(to_fetch)}, 有效数据: {len(df)}")
                
        except Exception as e:
            logger.error(f"批量{i}失败: {e}")
        
        time.sleep(1)  # 避免限流
    
    if all_data:
        financial_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"共获取财务数据: {len(financial_df)} 条")
        
        # 更新数据库
        for _, row in financial_df.iterrows():
            ts_code = row['ts_code']
            cursor.execute(f"""
                UPDATE fundamentals SET
                    roe = ?,
                    netprofit_margin = ?,
                    grossprofit_margin = ?,
                    expense_ratio = ?,
                    operate_profit_margin = ?,
                    current_ratio = ?,
                    quick_ratio = ?,
                    debt_to_assets = ?,
                    debt_to_equity = ?,
                    end_date = ?
                WHERE ts_code = ?
            """, (
                row.get('roe'), row.get('netprofit_margin'), row.get('grossprofit_margin'),
                row.get('expense_ratio'), row.get('operate_profit_margin'),
                row.get('current_ratio'), row.get('quick_ratio'),
                row.get('debt_to_assets'), row.get('debt_to_equity'),
                row.get('end_date'), ts_code
            ))
        
        conn.commit()
        
        # 验证
        cursor.execute('SELECT COUNT(*) FROM fundamentals WHERE roe IS NOT NULL')
        new_count = cursor.fetchone()[0]
        logger.info(f"更新后有ROE数据: {new_count} 只")
    
    conn.close()


if __name__ == "__main__":
    download_financial_data()
