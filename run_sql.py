#!/usr/bin/env python3
"""
SQL向量化回测 - 最快版本
所有计算在SQL中完成
"""

import sqlite3
import time

def sql_backtest():
    t0 = time.time()
    conn = sqlite3.connect('data/stocks.db')
    
    # 创建视图
    conn.execute("""
        CREATE TEMP VIEW daily_calc AS
        SELECT ts_code, trade_date, close,
               LAG(close, 5) OVER w as price_5d_ago,
               LAG(close, 20) OVER w as price_20d_ago
        FROM daily
        WHERE trade_date >= '20200101'
        WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
    """)
    
    # 季节性策略: 4-10月买入
    # 每月第一天买入MA20上涨的股票
    results = []
    capital = 1000000
    
    # 获取所有月份
    months = conn.execute("""
        SELECT DISTINCT substr(trade_date, 1, 6) as month
        FROM daily
        WHERE trade_date >= '20200101' AND trade_date <= '20241231'
        AND substr(trade_date, 5, 2) IN ('04', '07')
        ORDER BY month
    """).fetchall()
    
    positions = {}
    
    for (month,) in months:
        # 选股: 当月有数据且价格高于20日前
        stocks = conn.execute(f"""
            SELECT ts_code, close FROM daily_calc
            WHERE month = '{month}'
            AND close > price_20d_ago
            AND ts_code IN (SELECT ts_code FROM fundamentals WHERE pe > 0 AND pe < 25 AND roe > 10)
            LIMIT 3
        """).fetchall()
        
        # 卖出旧持仓
        for code in list(positions.keys()):
            price = conn.execute(f"""
                SELECT close FROM daily 
                WHERE ts_code = '{code}' AND trade_date <= '{month}31'
                ORDER BY trade_date DESC LIMIT 1
            """).fetchone()
            if price:
                capital += price[0] * positions[code] * 0.998
                del positions[code]
        
        # 买入新持仓
        for ts_code, close in stocks[:3]:
            if capital > 10000:
                qty = int(capital / 3 / close / 100) * 100
                if qty > 0:
                    capital -= close * qty * 1.001
                    positions[ts_code] = qty
    
    # 年末卖出
    for code, qty in positions.items():
        price = conn.execute("""
            SELECT close FROM daily 
            WHERE trade_date <= '20241231'
            ORDER BY trade_date DESC LIMIT 1
        """).fetchone()
        if price:
            capital += price[0] * qty * 0.998
    
    conn.close()
    
    ret = (capital - 1000000) / 1000000 * 100
    print(f"\nSQL向量化结果:")
    print(f"  最终资金: {capital:,.0f}")
    print(f"  总收益: {ret:.1f}%")
    print(f"  耗时: {time.time()-t0:.1f}秒")

if __name__ == '__main__':
    sql_backtest()
