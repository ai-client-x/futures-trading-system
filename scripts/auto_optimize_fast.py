#!/usr/bin/env python3
"""
快速参数优化脚本
使用简化版回测加快速度
"""

import sys
sys.path.insert(0, '.')

import sqlite3
import pandas as pd
import numpy as np
import time
import random
from datetime import datetime
from collections import defaultdict

import importlib.util
spec = importlib.util.spec_from_file_location("signal_strength", "src/signal_strength.py")
signal_strength = importlib.util.module_from_spec(spec)
spec.loader.exec_module(signal_strength)
calc_signal_strength = signal_strength.calc_signal_strength


def load_data_fast():
    """快速加载数据 - 只用50只股票"""
    conn = sqlite3.connect('data/stocks.db')
    dates_df = pd.read_sql("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= '20160101' AND trade_date <= '20191231' ORDER BY trade_date", conn)
    dates = [str(d) for d in dates_df['trade_date'].tolist()]
    
    # 只取成交额前50只
    daily_df = pd.read_sql("""
        SELECT trade_date, ts_code, open, high, low, close, vol FROM daily 
        WHERE trade_date >= '20160101' AND trade_date <= '20191231'
        AND ts_code IN (SELECT DISTINCT ts_code FROM daily ORDER BY amount DESC LIMIT 50)
    """, conn)
    conn.close()
    
    date_to_idx = {d: i for i, d in enumerate(dates)}
    stocks = {}
    for code in daily_df['ts_code'].unique():
        code_df = daily_df[daily_df['ts_code'] == code].copy()
        code_df['date_idx'] = code_df['trade_date'].map(date_to_idx)
        code_df = code_df.dropna(subset=['date_idx'])
        code_df['date_idx'] = code_df['date_idx'].astype(int)
        
        n = len(dates)
        stocks[code] = {k: np.zeros(n) for k in ['open','high','low','close','vol']}
        
        for _, row in code_df.iterrows():
            idx = int(row['date_idx'])
            if 0 <= idx < n:
                for k in stocks[code]:
                    stocks[code][k][idx] = row[k]
        
        for k in stocks[code]:
            last = 0
            for i in range(n):
                if stocks[code][k][i] == 0:
                    stocks[code][k][i] = last
                else:
                    last = stocks[code][k][i]
    return stocks, dates


def detect_regime(stocks, i, th=0.08):
    if i < 20:
        return 'Sideways'
    closes = []
    for code, data in list(stocks.items())[:50]:
        if i >= len(data['close']) or data['close'][i] == 0:
            continue
        start = i - 20
        if start >= 0 and data['close'][start] > 0:
            ret = (data['close'][i] - data['close'][start]) / data['close'][start]
            closes.append(ret)
    if len(closes) < 10:
        return 'Sideways'
    avg = np.mean(closes)
    if avg > th:
        return 'Bull'
    elif avg < -th:
        return 'Bear'
    return 'Sideways'


def fast_backtest(stocks, dates, params):
    """快速回测"""
    threshold = params.get('threshold', 10)
    buy_gap = params.get('buy_gap', 20)
    sl = params.get('sl', 0.10)
    tp = params.get('tp', 0.20)
    max_pos = params.get('max_pos', 10)
    bull_strat = params.get('bull_strat', 'RSI逆势')
    side_strat = params.get('side_strat', '布林带')
    
    cash = 1000000
    positions = {}
    trades = []
    
    for i in range(len(dates)):
        regime = detect_regime(stocks, i)
        
        # 熊市空仓
        if regime == 'Bear':
            for code in list(positions.keys()):
                if code in stocks and i < len(stocks[code]['close']):
                    price = stocks[code]['close'][i]
                    if price > 0:
                        ret = (price - positions[code]['cost']) / positions[code]['cost']
                        cash += price * positions[code]['qty'] * 0.999
                        trades.append(ret)
            positions = {}
            continue
        
        strategy = bull_strat if regime == 'Bull' else side_strat
        
        # 止盈止损
        for code in list(positions.keys()):
            pos = positions[code]
            if i - pos['buy_idx'] < 1:
                continue
            if code in stocks and i < len(stocks[code]['close']):
                price = stocks[code]['close'][i]
                if price > 0:
                    ret = (price - pos['cost']) / pos['cost']
                    if ret >= tp or ret <= -sl:
                        cash += price * pos['qty'] * 0.999
                        trades.append(ret)
                        del positions[code]
        
        # 买入 - 简化版
        if i % buy_gap < 3 and len(positions) < max_pos and cash > 10000:
            for code in list(stocks.keys())[:20]:
                if code in positions or i < 15:
                    continue
                if i >= len(stocks[code]['close']) or stocks[code]['close'][i] == 0:
                    continue
                
                hist = pd.DataFrame({
                    'Open': stocks[code]['open'][max(0,i-25):i+1],
                    'High': stocks[code]['high'][max(0,i-25):i+1],
                    'Low': stocks[code]['low'][max(0,i-25):i+1],
                    'Close': stocks[code]['close'][max(0,i-25):i+1],
                    'Volume': stocks[code]['vol'][max(0,i-25):i+1]
                })
                
                if len(hist) < 15:
                    continue
                
                try:
                    score = calc_signal_strength(hist, strategy, 'buy')
                except:
                    score = 0
                
                if score >= threshold and len(positions) < max_pos:
                    price = stocks[code]['close'][i]
                    qty = int(100000 / price / 100) * 100
                    if qty > 0 and price * qty * 1.001 <= cash:
                        cash -= price * qty * 1.001
                        positions[code] = {'cost': price, 'qty': qty, 'buy_idx': i}
    
    if not trades:
        return {'return': -1, 'trades': 0, 'win_rate': 0, 'params': params}
    
    final_cash = cash + sum(p['cost'] * p['qty'] for p in positions.values())
    total_return = (final_cash - 1000000) / 1000000
    
    # 计算最大回撤
    equity = [1000000]
    for ret in trades:
        equity.append(equity[-1] * (1 + ret))
    
    peak = equity[0]
    max_dd = 0
    for e in equity:
        if e > peak:
            peak = e
        dd = (peak - e) / peak
        if dd > max_dd:
            max_dd = dd
    
    win_rate = sum(1 for r in trades if r > 0) / len(trades)
    
    return {
        'return': total_return,
        'trades': len(trades),
        'win_rate': win_rate,
        'max_dd': max_dd,
        'params': params
    }


def main():
    print("加载数据(快速版)...", flush=True)
    stocks, dates = load_data_fast()
    print(f"股票: {len(stocks)}, 交易日: {len(dates)}", flush=True)
    
    start_time = time.time()
    end_time = start_time + 3600  # 1小时
    
    best = {'return': 0}
    iteration = 0
    
    print(f"开始优化 ({datetime.now().strftime('%H:%M:%S')})", flush=True)
    
    while time.time() < end_time:
        iteration += 1
        
        params = {
            'threshold': random.choice([8, 10, 12, 15]),
            'buy_gap': random.choice([15, 20, 25]),
            'sl': random.choice([0.08, 0.10, 0.12]),
            'tp': random.choice([0.18, 0.20, 0.25]),
            'max_pos': random.choice([8, 10]),
            'bull_strat': random.choice(['RSI逆势', 'MACD策略', '成交量突破']),
            'side_strat': random.choice(['布林带', '威廉指标', '双底形态'])
        }
        
        result = fast_backtest(stocks, dates, params)
        
        # 评分
        if result['return'] > best['return'] and result['trades'] > 30:
            best = result
            print(f"[{iteration}] 新最佳! 收益:{result['return']*100:.1f}%, 胜率:{result['win_rate']*100:.0f}%, 回撤:{result['max_dd']*100:.1f}%, 交易:{result['trades']}", flush=True)
            print(f"  参数: {params}", flush=True)
        
        if iteration % 100 == 0:
            elapsed = (time.time() - start_time) / 60
            print(f"进度: {elapsed:.0f}分钟, {iteration}次, 最佳:{best['return']*100:.1f}%", flush=True)
    
    print()
    print("="*60, flush=True)
    print("优化完成!", flush=True)
    print(f"测试: {iteration}次", flush=True)
    print(f"最佳: 收益{best['return']*100:.1f}%, 胜率{best['win_rate']*100:.0f}%, 回撤{best['max_dd']*100:.1f}%", flush=True)
    print(f"参数: {best['params']}", flush=True)
    print("="*60, flush=True)


if __name__ == "__main__":
    main()
