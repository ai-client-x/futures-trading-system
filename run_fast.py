#!/usr/bin/env python3
"""
向量化回测引擎 - 极简极速版
不预先计算指标，在需要时直接计算
"""

import sqlite3
import pandas as pd
import numpy as np
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = "data/stocks.db"


def fast_backtest():
    """极速回测"""
    t0 = time.time()
    
    # 加载数据
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT ts_code, trade_date, close 
        FROM daily 
        WHERE trade_date >= '20200101'
        ORDER BY ts_code, trade_date
    """, conn)
    
    funds = pd.read_sql("""
        SELECT ts_code FROM fundamentals 
        WHERE pe > 0 AND pe < 25 AND roe > 10
    """, conn)['ts_code'].tolist()
    conn.close()
    
    # 过滤基本面
    df = df[df['ts_code'].isin(funds)]
    
    trade_dates = sorted(df['trade_date'].unique())
    print(f"加载: {time.time()-t0:.1f}秒, {len(trade_dates)}天")
    
    # 按股票分组预计算
    stock_data = {code: group['close'].values for code, group in df.groupby('ts_code')}
    stock_dates = {code: sorted(group['trade_date'].unique()) for code, group in df.groupby('ts_code')}
    
    # 策略参数
    TP, SL = 0.20, 0.08
    START_MONTH, END_MONTH = 4, 10
    
    capital = 1000000
    positions = {}
    
    for i, date in enumerate(trade_dates):
        month = int(date[4:6])
        in_season = START_MONTH <= month <= END_MONTH
        
        # 卖出
        to_del = []
        for code, pos in positions.items():
            prices = stock_data.get(code)
            if prices is None:
                continue
            
            dates = stock_dates.get(code, [])
            # 找到该日期对应的索引
            try:
                idx = dates.index(date)
                if idx < 20:
                    continue
                price = prices[idx]
                cost = pos['cost']
                ret = (price - cost) / cost
                
                if ret > TP or ret < -SL or (month > END_MONTH and not pos.get('marked')):
                    if month > END_MONTH and not pos.get('marked'):
                        pos['marked'] = True
                    else:
                        capital += price * pos['qty'] * 0.998
                        to_del.append(code)
            except:
                continue
        
        for code in to_del:
            del positions[code]
        
        # 买入
        if in_season and len(positions) < 3 and capital > 50000:
            if month == START_MONTH or month == 7:
                # 选股: MA5 > MA20
                candidates = []
                for code in funds[:50]:  # 只检查前50只
                    if code in positions:
                        continue
                    prices = stock_data.get(code)
                    dates = stock_dates.get(code, [])
                    if prices is None or len(dates) < 20:
                        continue
                    try:
                        idx = dates.index(date)
                        if idx < 20:
                            continue
                        ma5 = np.mean(prices[idx-5:idx])
                        ma20 = np.mean(prices[idx-20:idx]) if idx >= 20 else 0
                        if ma5 > ma20:
                            candidates.append((code, prices[idx]))
                    except:
                        continue
                
                # 买入
                for code, price in candidates[:3-len(positions)]:
                    if capital > 10000:
                        qty = int(capital / 3 / price / 100) * 100
                        if qty > 0:
                            capital -= price * qty * 1.001
                            positions[code] = {'qty': qty, 'cost': price, 'marked': False}
    
        if (i+1) % 300 == 0:
            print(f"进度: {i+1}/{len(trade_dates)}")
    
    total = capital
    ret = (total - 1000000) / 1000000 * 100
    
    print(f"\n结果:")
    print(f"  最终资金: {total:,.0f}")
    print(f"  总收益: {ret:.1f}%")
    print(f"  持仓: {len(positions)}")
    print(f"  总耗时: {time.time()-t0:.1f}秒")


if __name__ == '__main__':
    fast_backtest()
