#!/usr/bin/env python3
"""
自适应市场状态的交易系统
- 根据大盘均线判断牛/熊/震荡市
- 采用不同加仓策略
"""

import sqlite3
import pandas as pd
import numpy as np
from collections import Counter
from enum import Enum

DB_PATH = "data/stocks.db"

class MarketRegime(Enum):
    BULL = "bull"      # 牛市
    BEAR = "bear"      # 熊市  
    SIDEWAYS = "sideways"  # 震荡市

def get_market_regime(date):
    """根据大盘均线判断市场状态"""
    conn = sqlite3.connect(DB_PATH)
    
    # 获取沪深300指数数据
    df = pd.read_sql(f"""
        SELECT trade_date, close FROM daily 
        WHERE ts_code = '000300.SH' AND trade_date <= '{date}'
        ORDER BY trade_date DESC LIMIT 100
    """, conn)
    conn.close()
    
    if len(df) < 60:
        return MarketRegime.SIDEWAYS
    
    df = df.sort_values('trade_date')
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma20 = df['close'].rolling(20).mean().iloc[-1]
    ma60 = df['close'].rolling(60).mean().iloc[-1]
    
    # 牛市：均线多头排列
    if ma5 > ma20 > ma60:
        return MarketRegime.BULL
    # 熊市：均线空头排列
    elif ma5 < ma20 < ma60:
        return MarketRegime.BEAR
    else:
        return MarketRegime.SIDEWAYS

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
        return 'hold'
    
    close = prices['close']
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    
    if ma5.iloc[-1] > ma20.iloc[-1] and ma5.iloc[-2] <= ma20.iloc[-2]:
        return 'buy'
    if ma5.iloc[-1] < ma20.iloc[-1] and ma5.iloc[-2] >= ma20.iloc[-2]:
        return 'sell'
    return 'hold'

def run_backtest():
    """运行回测"""
    capital = 1000000
    initial_capital = 1000000
    positions = []
    trades = []
    
    # 参数
    SL = 0.05
    TP1, TP2, TP3 = 0.10, 0.15, 0.20
    
    # 策略参数
    strategy_params = {
        MarketRegime.BULL: {'add_on_rise': 0.05, 'max_layers': 2, 'add_ratio': [0.5, 1.0]},
        MarketRegime.BEAR: {'add_on_drop': 0.05, 'max_layers': 2, 'add_ratio': [1.0, 0.5]},
        MarketRegime.SIDEWAYS: {'add_on_drop': 0.08, 'max_layers': 1, 'add_ratio': [0.5]},
    }
    
    conn = sqlite3.connect(DB_PATH)
    trade_dates = pd.read_sql("""
        SELECT DISTINCT trade_date FROM daily 
        WHERE trade_date >= '20200101' AND trade_date <= '20241231'
        ORDER BY trade_date
    """, conn)['trade_date'].tolist()
    conn.close()
    
    last_month = None
    pool = []
    last_regime = MarketRegime.SIDEWAYS
    
    # 统计
    regime_count = Counter()
    
    for i, date in enumerate(trade_dates):
        month = date[:6]
        
        # 每月更新一次股票池
        if month != last_month:
            pool = get_stock_pool(date)
            last_month = month
        
        # 每周判断一次市场状态
        if i % 5 == 0:  # 每5天判断一次
            last_regime = get_market_regime(date)
        
        regime_count[last_regime.value] += 1
        
        if not pool:
            continue
        
        # ===== 卖出 =====
        to_sell = []
        for pos in list(positions):
            prices = get_prices(pos['code'], date)
            if len(prices) < 20:
                continue
            
            signal = check_ma_signal(prices)
            price = prices.iloc[-1]['close']
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
        
        # 获取当前市场状态的策略参数
        params = strategy_params[last_regime]
        
        # 检查加仓（根据市场状态）
        for pos in positions:
            if pos.get('layers', 1) >= params['max_layers']:
                continue
            
            prices = get_prices(pos['code'], date)
            if len(prices) < 60:
                continue
            
            curr_price = prices.iloc[-1]['close']
            ret = (curr_price - pos['cost']) / pos['cost']
            
            should_add = False
            if last_regime == MarketRegime.BULL:
                # 牛市：上涨时加仓
                should_add = ret >= params['add_on_rise']
            elif last_regime == MarketRegime.BEAR:
                # 熊市：下跌时加仓
                should_add = ret <= -params['add_on_drop']
            else:
                # 震荡市：少交易
                should_add = ret <= -params['add_on_drop']
            
            if should_add:
                layer_idx = pos.get('layers', 1) - 1
                add_ratio = params['add_ratio'][min(layer_idx, len(params['add_ratio'])-1)]
                add_qty = int(pos['qty'] * add_ratio)
                
                if add_qty > 0 and capital > curr_price * add_qty * 1.001:
                    capital -= curr_price * add_qty * 1.001
                    total_cost = pos['cost'] * pos['qty'] + curr_price * add_qty
                    pos['qty'] += add_qty
                    pos['cost'] = total_cost / pos['qty']
                    pos['layers'] = pos.get('layers', 1) + 1
                    trades.append({
                        'date': date, 
                        'code': pos['code'], 
                        'action': 'add', 
                        'reason': f'{last_regime.value}加仓'
                    })
        
        # 新建仓
        max_pos = 5
        if len(positions) < max_pos and capital > 50000:
            for cand in pool:
                if len(positions) >= max_pos:
                    break
                if cand['ts_code'] in held:
                    continue
                
                prices = get_prices(cand['ts_code'], date)
                if len(prices) < 60:
                    continue
                
                signal = check_ma_signal(prices)
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
    print("自适应市场状态交易系统 (2020-2024)")
    print("="*60)
    print(f"初始资金: {initial_capital:,.0f}")
    print(f"最终资金: {capital:,.0f}")
    print(f"总收益: {total_return:.2f}%")
    print(f"年化收益: {annual_return*100:.2f}%")
    print(f"\n交易统计:")
    print(f"  买入: {buy_count}次")
    print(f"  加仓: {add_count}次")
    print(f"  卖出: {sell_count}次")
    print(f"  总交易: {len(trades)}次")
    print(f"\n市场状态分布:")
    for regime, count in regime_count.most_common():
        print(f"  {regime}: {count}天 ({count/sum(regime_count.values())*100:.1f}%)")
    print(f"\n卖出原因:")
    for reason, count in sell_reasons.most_common():
        print(f"  {reason}: {count}")

if __name__ == '__main__':
    run_backtest()
