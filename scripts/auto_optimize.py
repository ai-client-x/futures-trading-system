#!/usr/bin/env python3
"""
参数自动优化脚本
连续运行1小时，不断尝试不同参数组合
"""

import sys
sys.path.insert(0, '.')

import sqlite3
import pandas as pd
import numpy as np
import time
import random
from datetime import datetime

import importlib.util
spec = importlib.util.spec_from_file_location("signal_strength", "src/signal_strength.py")
signal_strength = importlib.util.module_from_spec(spec)
spec.loader.exec_module(signal_strength)
calc_signal_strength = signal_strength.calc_signal_strength

STRATEGIES = {
    'Bull': ['RSI逆势', 'MACD策略', '成交量突破', '突破前高', '布林带+RSI'],
    'Bear': [],  # 熊市空仓
    'Sideways': ['布林带', '威廉指标', '双底形态', 'RSI+均线', '趋势过滤']
}


def load_data():
    conn = sqlite3.connect('data/stocks.db')
    dates_df = pd.read_sql("""
        SELECT DISTINCT trade_date FROM daily 
        WHERE trade_date >= '20160101' AND trade_date <= '20191231'
        ORDER BY trade_date
    """, conn)
    dates = [str(d) for d in dates_df['trade_date'].tolist()]
    
    daily_df = pd.read_sql("""
        SELECT trade_date, ts_code, open, high, low, close, vol FROM daily 
        WHERE trade_date >= '20160101' AND trade_date <= '20191231'
        AND ts_code IN (SELECT DISTINCT ts_code FROM daily ORDER BY amount DESC LIMIT 150)
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


def detect_regime(stocks, i, threshold=0.08):
    if i < 30:
        return 'Sideways'
    closes = []
    for code, data in list(stocks.items())[:100]:
        if i >= len(data['close']) or data['close'][i] == 0:
            continue
        start = i - 30
        if start >= 0 and data['close'][start] > 0:
            ret = (data['close'][i] - data['close'][start]) / data['close'][start]
            closes.append(ret)
    if len(closes) < 20:
        return 'Sideways'
    avg = np.mean(closes)
    if avg > threshold:
        return 'Bull'
    elif avg < -threshold:
        return 'Bear'
    return 'Sideways'


def backtest(stocks, dates, params):
    signal_threshold = params.get('signal_threshold', 10)
    buy_freq = params.get('buy_freq', 20)
    sl = params.get('sl', 0.10)
    tp = params.get('tp', 0.20)
    max_pos = params.get('max_pos', 10)
    regime_th = params.get('regime_th', 0.08)
    
    bull_strat = params.get('bull_strat', 'RSI逆势')
    side_strat = params.get('side_strat', '布林带')
    
    cash = 1000000
    positions = {}
    trade_returns = []
    
    for i in range(len(dates)):
        regime = detect_regime(stocks, i, regime_th)
        
        if regime == 'Bear':
            for code in list(positions.keys()):
                if code in stocks and i < len(stocks[code]['close']):
                    price = stocks[code]['close'][i]
                    if price > 0:
                        ret = (price - positions[code]['cost']) / positions[code]['cost']
                        cash += price * positions[code]['qty'] * 0.999
                        trade_returns.append(ret)
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
                        trade_returns.append(ret)
                        del positions[code]
        
        # 买入
        if i % buy_freq < 5 and len(positions) < max_pos and cash > 10000:
            signals = []
            for code in list(stocks.keys())[:40]:
                if code in positions or i < 20:
                    continue
                if i >= len(stocks[code]['close']) or stocks[code]['close'][i] == 0:
                    continue
                
                hist = pd.DataFrame({
                    'Open': stocks[code]['open'][max(0,i-30):i+1],
                    'High': stocks[code]['high'][max(0,i-30):i+1],
                    'Low': stocks[code]['low'][max(0,i-30):i+1],
                    'Close': stocks[code]['close'][max(0,i-30):i+1],
                    'Volume': stocks[code]['vol'][max(0,i-30):i+1]
                })
                
                if len(hist) < 20:
                    continue
                
                try:
                    score = calc_signal_strength(hist, strategy, 'buy')
                except:
                    score = 0
                
                if score >= signal_threshold:
                    signals.append({'code': code, 'price': stocks[code]['close'][i], 'score': score})
            
            signals.sort(key=lambda x: x['score'], reverse=True)
            
            for sig in signals[:2]:
                if cash >= 100000:
                    qty = int(100000 / sig['price'] / 100) * 100
                    if qty > 0 and sig['price'] * qty * 1.001 <= cash:
                        cash -= sig['price'] * qty * 1.001
                        positions[sig['code']] = {'cost': sig['price'], 'qty': qty, 'buy_idx': i}
    
    # 计算结果
    if not trade_returns:
        return {'return': -1, 'trades': 0, 'win_rate': 0, 'max_dd': 1}
    
    final_cash = cash + sum(p['cost'] * p['qty'] for p in positions.values())
    total_return = (final_cash - 1000000) / 1000000
    
    # 计算最大回撤
    equity = [1000000]
    for ret in trade_returns:
        equity.append(equity[-1] * (1 + ret))
    
    peak = equity[0]
    max_dd = 0
    for e in equity:
        if e > peak:
            peak = e
        dd = (peak - e) / peak
        if dd > max_dd:
            max_dd = dd
    
    win_rate = sum(1 for r in trade_returns if r > 0) / len(trade_returns)
    
    return {
        'return': total_return,
        'trades': len(trade_returns),
        'win_rate': win_rate,
        'max_dd': max_dd,
        'params': params
    }


def main():
    print("加载数据...")
    stocks, dates = load_data()
    print(f"股票: {len(stocks)}, 交易日: {len(dates)}")
    
    start_time = time.time()
    end_time = start_time + 3600  # 1小时
    
    best_result = {'return': 0, 'trades': 0, 'win_rate': 0, 'max_dd': 1, 'params': {}}
    results_log = []
    
    iteration = 0
    
    print(f"开始优化 ({datetime.now().strftime('%H:%M:%S')})")
    
    while time.time() < end_time:
        iteration += 1
        
        # 随机生成参数
        params = {
            'signal_threshold': random.choice([5, 8, 10, 12, 15]),
            'buy_freq': random.choice([10, 15, 20, 25, 30]),
            'sl': random.choice([0.08, 0.10, 0.12, 0.15]),
            'tp': random.choice([0.15, 0.20, 0.25, 0.30]),
            'max_pos': random.choice([5, 8, 10, 12]),
            'regime_th': random.choice([0.06, 0.08, 0.10, 0.12]),
            'bull_strat': random.choice(STRATEGIES['Bull']),
            'side_strat': random.choice(STRATEGIES['Sideways'])
        }
        
        result = backtest(stocks, dates, params)
        
        # 评分：收益高、回撤低、交易次数适中
        score = result['return'] * 2 - result['max_dd'] * 0.5
        
        if result['return'] > best_result['return'] and result['trades'] > 20:
            best_result = result
            print(f"[{iteration}] 新最佳! 收益:{result['return']*100:.1f}%, 胜率:{result['win_rate']*100:.0f}%, 回撤:{result['max_dd']*100:.1f}%, 交易:{result['trades']}")
            print(f"  参数: {params}")
        
        # 每50次输出进度
        if iteration % 50 == 0:
            elapsed = (time.time() - start_time) / 60
            remaining = 60 - elapsed
            print(f"进度: {elapsed:.0f}分钟, 已测试{iteration}次, 当前最佳:{best_result['return']*100:.1f}%")
    
    print()
    print("="*60)
    print("优化完成!")
    print(f"测试次数: {iteration}")
    print(f"最佳结果:")
    print(f"  收益: {best_result['return']*100:.2f}%")
    print(f"  胜率: {best_result['win_rate']*100:.1f}%")
    print(f"  回撤: {best_result['max_dd']*100:.2f}%")
    print(f"  交易: {best_result['trades']}次")
    print(f"  参数: {best_result['params']}")
    print("="*60)


if __name__ == "__main__":
    main()
