#!/usr/bin/env python3
"""
保守方案 + 顺势金字塔加仓回测
- 止损5%，止盈10%
- 顺势加仓：上涨趋势中加仓（让利润奔跑）
- 分批止盈：涨10%卖30%，涨15%卖60%，涨20%清仓
"""

import sqlite3
import pandas as pd
import numpy as np
from collections import Counter

DB_PATH = "data/stocks.db"

def get_stock_pool(date):
    """获取选股池"""
    conn = sqlite3.connect(DB_PATH)
    
    year = date[:4]
    has_year = pd.read_sql(f"""
        SELECT DISTINCT ts_code FROM daily 
        WHERE trade_date >= '{year}0101' AND trade_date <= '{year}1231'
    """, conn)['ts_code'].tolist()
    
    df = pd.read_sql("""
        SELECT ts_code, name FROM fundamentals
        WHERE pe > 0 AND pe < 25 AND roe > 10
    """, conn)
    
    pool = df[df['ts_code'].isin(has_year)].head(30)
    conn.close()
    return pool.to_dict('records')

def get_prices(code, end_date):
    """获取到某日之前的所有价格"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"""
        SELECT trade_date, close FROM daily 
        WHERE ts_code = '{code}' AND trade_date <= '{end_date}'
        ORDER BY trade_date
    """, conn)
    conn.close()
    return df

def check_ma_signal(prices):
    """MA信号"""
    if len(prices) < 60:
        return 'hold', 0
    
    close = prices['close']
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    
    # 计算涨幅
    ret = (close.iloc[-1] / close.iloc[-20] - 1) if len(prices) >= 20 else 0
    
    # 金叉买入
    if ma5.iloc[-1] > ma20.iloc[-1] and ma5.iloc[-2] <= ma20.iloc[-2]:
        return 'buy', ret
    # 死叉卖出
    if ma5.iloc[-1] < ma20.iloc[-1] and ma5.iloc[-2] >= ma20.iloc[-2]:
        return 'sell', ret
    return 'hold', ret

def check_bullish_add(prices):
    """检查是否可以顺势加仓"""
    if len(prices) < 60:
        return False
    
    close = prices['close']
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    
    # 顺势加仓条件：
    # 1. 多头排列 (MA5 > MA20)
    # 2. 连续上涨 (今天涨，昨天也涨)
    # 3. 涨幅 > 3% (强势股)
    
    if ma5.iloc[-1] <= ma20.iloc[-1]:
        return False
    
    if len(prices) < 3:
        return False
    
    # 连续上涨
    if close.iloc[-1] <= close.iloc[-2]:
        return False
    
    # 涨幅超过3%
    ret_20d = close.iloc[-1] / close.iloc[-20] - 1 if len(prices) >= 20 else 0
    if ret_20d < 0.03:
        return False
    
    return True

def run_backtest():
    """运行回测"""
    capital = 1000000
    initial_capital = 1000000
    positions = []
    trades = []
    
    # 参数
    SL = 0.05
    TP1 = 0.10
    TP2 = 0.15
    TP3 = 0.20
    
    conn = sqlite3.connect(DB_PATH)
    trade_dates = pd.read_sql("""
        SELECT DISTINCT trade_date FROM daily 
        WHERE trade_date >= '20200101' AND trade_date <= '20241231'
        ORDER BY trade_date
    """, conn)['trade_date'].tolist()
    conn.close()
    
    last_month = None
    pool = []
    
    for i, date in enumerate(trade_dates):
        month = date[:6]
        
        if month != last_month:
            pool = get_stock_pool(date)
            last_month = month
        
        if not pool:
            continue
        
        # ===== 卖出 =====
        to_sell = []
        for pos in list(positions):
            prices = get_prices(pos['code'], date)
            if len(prices) < 20:
                continue
            
            signal, ret = check_ma_signal(prices)
            price = prices.iloc[-1]['close']
            
            # 计算当前收益率
            curr_ret = (price - pos['cost']) / pos['cost']
            
            sell_qty = 0
            reason = ''
            
            # 分批止盈
            if curr_ret >= TP3:
                sell_qty = pos['qty']
                reason = '止盈20%清仓'
            elif curr_ret >= TP2 and pos.get('layers', 1) >= 1:
                sell_qty = int(pos['qty'] * 0.6)
                reason = '止盈15%卖出60%'
            elif curr_ret >= TP1 and pos.get('layers', 1) == 1:
                sell_qty = int(pos['qty'] * 0.3)
                reason = '止盈10%卖出30%'
            # 止损
            elif curr_ret <= -SL:
                sell_qty = pos['qty']
                reason = '止损5%'
            # 死叉卖出
            elif signal == 'sell':
                sell_qty = pos['qty']
                reason = '死叉卖出'
            
            if sell_qty > 0:
                to_sell.append((pos, price, sell_qty, reason))
        
        for pos, price, sell_qty, reason in to_sell:
            capital += price * sell_qty * 0.998
            trades.append({'date': date, 'code': pos['code'], 'action': 'sell', 'reason': reason})
            remaining = pos['qty'] - sell_qty
            if remaining > 0:
                pos['qty'] = remaining
            else:
                positions = [p for p in positions if p['code'] != pos['code']]
        
        # ===== 买入/加仓 =====
        held = set(p['code'] for p in positions)
        
        # 顺势加仓：检查现有持仓是否需要加仓
        for pos in positions:
            if pos.get('layers', 1) >= 2:  # 最多加仓1次
                continue
            
            prices = get_prices(pos['code'], date)
            if len(prices) < 60:
                continue
            
            # 顺势加仓条件
            if check_bullish_add(prices):
                curr_price = prices.iloc[-1]['close']
                add_qty = pos['qty']  # 加仓数量等于原持仓
                if capital > curr_price * add_qty * 1.001:
                    capital -= curr_price * add_qty * 1.001
                    total_cost = pos['cost'] * pos['qty'] + curr_price * add_qty
                    pos['qty'] += add_qty
                    pos['cost'] = total_cost / pos['qty']
                    pos['layers'] = pos.get('layers', 1) + 1
                    trades.append({'date': date, 'code': pos['code'], 'action': 'add', 'reason': '顺势加仓'})
        
        # 新建仓
        max_pos = 5
        if len(positions) < max_pos and capital > 0:
            for cand in pool:
                if len(positions) >= max_pos:
                    break
                if cand['ts_code'] in held:
                    continue
                
                prices = get_prices(cand['ts_code'], date)
                if len(prices) < 60:
                    continue
                
                signal, ret = check_ma_signal(prices)
                if signal == 'buy':
                    price = prices.iloc[-1]['close']
                    qty = int(capital / max_pos / price / 100) * 100
                    if qty > 0 and capital > price * qty * 1.001:
                        capital -= price * qty * 1.001
                        positions.append({
                            'code': cand['ts_code'],
                            'qty': qty,
                            'cost': price,
                            'layers': 1,
                            'name': cand.get('name', '')
                        })
                        trades.append({'date': date, 'code': cand['ts_code'], 'action': 'buy'})
    
    # 最终统计
    total_return = (capital - initial_capital) / initial_capital * 100
    annual_return = (1 + total_return/100) ** 0.25 - 1
    
    buy_count = sum(1 for t in trades if t['action'] == 'buy')
    add_count = sum(1 for t in trades if t['action'] == 'add')
    sell_count = sum(1 for t in trades if t['action'] == 'sell')
    sell_reasons = Counter(t['reason'] for t in trades if t['action'] == 'sell')
    
    print("="*60)
    print("保守方案 + 顺势金字塔加仓回测 (2020-2024)")
    print("="*60)
    print(f"初始资金: {initial_capital:,.0f}")
    print(f"最终资金: {capital:,.0f}")
    print(f"总收益: {total_return:.2f}%")
    print(f"年化收益: {annual_return*100:.2f}%")
    print(f"\n交易统计:")
    print(f"  买入: {buy_count}次")
    print(f"  顺势加仓: {add_count}次")
    print(f"  卖出: {sell_count}次")
    print(f"  总交易: {len(trades)}次")
    print(f"\n卖出原因:")
    for reason, count in sell_reasons.most_common():
        print(f"  {reason}: {count}")

if __name__ == '__main__':
    run_backtest()
