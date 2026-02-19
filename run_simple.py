#!/usr/bin/env python3
"""
简化版回测 - 去掉市场状态检测提高速度
"""

import sqlite3
import pandas as pd
import numpy as np
from collections import Counter

DB_PATH = "data/stocks.db"

def get_pool(date):
    conn = sqlite3.connect(DB_PATH)
    year = date[:4]
    has_year = pd.read_sql(f"""
        SELECT DISTINCT ts_code FROM daily 
        WHERE trade_date >= '{year}0101' AND trade_date <= '{year}1231'
    """, conn)['ts_code'].tolist()
    
    df = pd.read_sql("""
        SELECT ts_code FROM fundamentals
        WHERE pe > 0 AND pe < 25 AND roe > 10
    """, conn)
    pool = df[df['ts_code'].isin(has_year)]['ts_code'].head(20).tolist()
    conn.close()
    return pool

def get_price(code, date):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"""
        SELECT close FROM daily 
        WHERE ts_code = '{code}' AND trade_date <= '{date}'
        ORDER BY trade_date DESC LIMIT 60
    """, conn)
    conn.close()
    return df['close'].iloc[-1] if len(df) >= 60 else None

def check_ma(prices):
    if len(prices) < 60:
        return 'hold'
    ma5 = prices['close'].rolling(5).mean()
    ma20 = prices['close'].rolling(20).mean()
    if ma5.iloc[-1] > ma20.iloc[-1] and ma5.iloc[-2] <= ma20.iloc[-2]:
        return 'buy'
    if ma5.iloc[-1] < ma20.iloc[-1] and ma5.iloc[-2] >= ma20.iloc[-2]:
        return 'sell'
    return 'hold'

# 参数
TP1, TP2, TP3 = 0.10, 0.15, 0.20
SL1, SL2, SL3 = 0.03, 0.05, 0.08

def run_backtest():
    capital = 1000000
    positions = {}
    trades = []
    
    conn = sqlite3.connect(DB_PATH)
    trade_dates = pd.read_sql("""
        SELECT DISTINCT trade_date FROM daily 
        WHERE trade_date >= '20200101' AND trade_date <= '20241231'
        ORDER BY trade_date
    """, conn)['trade_date'].tolist()
    conn.close()
    
    last_month = None
    
    for i, date in enumerate(trade_dates):
        month = date[:6]
        if month != last_month:
            pool = get_pool(date)
            last_month = month
        
        # 卖出
        to_sell = []
        for code, pos in list(positions.items()):
            prices = pd.read_sql(f"""
                SELECT close FROM daily 
                WHERE ts_code = '{code}' AND trade_date <= '{date}'
                ORDER BY trade_date DESC LIMIT 60
            """, conn)
            if len(prices) < 20:
                continue
            
            price = prices['close'].iloc[-1]
            ret = (price - pos['cost']) / pos['cost']
            
            sell_qty = 0
            reason = ''
            
            if ret >= TP3:
                sell_qty = pos['qty']
                reason = 'TP20%'
            elif ret >= TP2 and not pos.get('tp15', False):
                sell_qty = int(pos['qty'] * 0.6)
                reason = 'TP15%'
                pos['tp15'] = True
            elif ret >= TP1 and not pos.get('tp10', False):
                sell_qty = int(pos['qty'] * 0.3)
                reason = 'TP10%'
                pos['tp10'] = True
            elif ret <= -SL3:
                sell_qty = pos['qty']
                reason = 'SL8%'
            elif ret <= -SL2 and not pos.get('sl5', False):
                sell_qty = int(pos['qty'] * 0.6)
                reason = 'SL5%'
                pos['sl5'] = True
            elif ret <= -SL1 and not pos.get('sl3', False):
                sell_qty = int(pos['qty'] * 0.3)
                reason = 'SL3%'
                pos['sl3'] = True
            
            if sell_qty > 0:
                to_sell.append((code, price, sell_qty, reason))
        
        for code, price, qty, reason in to_sell:
            capital += price * qty * 0.998
            trades.append({'date': date, 'code': code, 'action': 'sell', 'reason': reason})
            remaining = positions[code]['qty'] - qty
            if remaining > 0:
                positions[code]['qty'] = remaining
            else:
                del positions[code]
        
        # 买入
        if len(positions) < 5 and capital > 50000:
            for code in pool:
                if len(positions) >= 5:
                    break
                if code in positions:
                    continue
                price = get_price(code, date)
                if price:
                    qty = int(capital / 5 / price / 100) * 100
                    if qty > 0 and capital > price * qty * 1.001:
                        capital -= price * qty * 1.001
                        positions[code] = {
                            'qty': qty, 
                            'cost': price,
                            'tp10': False,
                            'tp15': False,
                            'sl3': False,
                            'sl5': False,
                        }
                        trades.append({'date': date, 'code': code, 'action': 'buy'})
    
    conn.close()
    
    total_return = (capital - 1000000) / 1000000 * 100
    sell_reasons = Counter(t['reason'] for t in trades if t['action'] == 'sell')
    
    print("="*50)
    print("简化版回测结果 (2020-2024)")
    print("="*50)
    print(f"最终资金: {capital:,.0f}")
    print(f"总收益: {total_return:.2f}%")
    print(f"买入: {sum(1 for t in trades if t['action']=='buy')}")
    print(f"卖出: {sum(1 for t in trades if t['action']=='sell')}")
    print(f"卖出原因: {dict(sell_reasons)}")

if __name__ == '__main__':
    run_backtest()
