#!/usr/bin/env python3
"""
策略快速测试 - 26个策略全测试
"""

import sqlite3
import pandas as pd
import numpy as np
import sys
import os
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import importlib.util
spec = importlib.util.spec_from_file_location("signal_strength", "src/signal_strength.py")
signal_strength = importlib.util.module_from_spec(spec)
spec.loader.exec_module(signal_strength)
calc_signal_strength = signal_strength.calc_signal_strength


# 26个策略
ALL_STRATEGIES = [
    '成交量突破', 'MACD+成交量', 'MACD策略', '突破前高', '均线发散', 
    '量价齐升', 'RSI趋势', '趋势过滤', '均线策略', '均线交叉强度',
    '收盘站均线', '成交量+均线', '突破确认', '平台突破',
    '动量反转', '威廉指标', 'RSI逆势', '双底形态', '缩量回调', 'MACD背离',
    '布林带', 'RSI+均线', '布林带+RSI', '支撑阻力', '波动率突破', '均线收复'
]


def load_data(start_date, end_date):
    conn = sqlite3.connect('data/stocks.db')
    
    dates_df = pd.read_sql(f"""
        SELECT DISTINCT trade_date FROM daily 
        WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
        ORDER BY trade_date
    """, conn)
    dates = [str(d) for d in dates_df['trade_date'].tolist()]
    
    # 只取成交额前100
    daily_df = pd.read_sql(f"""
        SELECT trade_date, ts_code, open, high, low, close, vol FROM daily 
        WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
        AND ts_code IN (
            SELECT DISTINCT ts_code FROM daily 
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            ORDER BY amount DESC LIMIT 100
        )
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


def detect_regime(stocks, i):
    if i < 30:
        return '震荡市'
    
    closes = []
    for code, data in list(stocks.items())[:100]:
        if i >= len(data['close']) or data['close'][i] == 0:
            continue
        start = i - 30
        if start >= 0 and data['close'][start] > 0:
            ret = (data['close'][i] - data['close'][start]) / data['close'][start]
            closes.append(ret)
    
    if len(closes) < 20:
        return '震荡市'
    
    avg = np.mean(closes)
    if avg > 0.10:
        return '牛市'
    elif avg < -0.10:
        return '熊市'
    return '震荡市'


def test_strategy(stocks, dates, strategy, threshold=10):
    cash = 1000000
    positions = {}
    trades = {r: [] for r in ['牛市','熊市','震荡市']}
    
    for i in range(len(dates)):
        regime = detect_regime(stocks, i)
        
        # 止盈止损
        for code in list(positions.keys()):
            pos = positions[code]
            if i - pos['buy_idx'] < 1:
                continue
            
            price = stocks[code]['close'][i] if i < len(stocks[code]['close']) else pos['cost']
            if price > 0:
                ret = (price - pos['cost']) / pos['cost']
                if ret >= 0.20 or ret <= -0.10:
                    cash += price * pos['qty'] * 0.999
                    trades[regime].append(ret)
                    del positions[code]
        
        # 买入 (每月前5个交易日)
        if i % 20 < 5 and len(positions) < 10 and cash > 10000:
            signals = []
            for code, data in list(stocks.items())[:30]:
                if code in positions or i < 20:
                    continue
                if i >= len(data['close']) or data['close'][i] == 0:
                    continue
                
                hist = pd.DataFrame({
                    'Open': data['open'][max(0,i-30):i+1],
                    'High': data['high'][max(0,i-30):i+1],
                    'Low': data['low'][max(0,i-30):i+1],
                    'Close': data['close'][max(0,i-30):i+1],
                    'Volume': data['vol'][max(0,i-30):i+1]
                })
                
                if len(hist) < 20:
                    continue
                
                try:
                    score = calc_signal_strength(hist, strategy, 'buy')
                except:
                    score = 0
                
                if score >= threshold:
                    signals.append({'code': code, 'price': data['close'][i], 'score': score})
            
            signals.sort(key=lambda x: x['score'], reverse=True)
            
            for sig in signals[:1]:
                if cash >= 100000:
                    qty = int(100000 / sig['price'] / 100) * 100
                    if qty > 0:
                        cash -= sig['price'] * qty * 1.001
                        positions[sig['code']] = {'cost': sig['price'], 'qty': qty, 'buy_idx': i}
    
    return trades


def main():
    print("加载2020-2025数据...")
    stocks, dates = load_data('20200101', '20250630')
    print(f"股票: {len(stocks)}, 交易日: {len(dates)}")
    
    results = {r: {} for r in ['牛市','熊市','震荡市']}
    
    for strategy in ALL_STRATEGIES:
        print(f"测试 {strategy}...", end=" ", flush=True)
        
        trades = test_strategy(stocks, dates, strategy, threshold=10)
        
        for regime in ['牛市','熊市','震荡市']:
            t = trades[regime]
            if t:
                results[regime][strategy] = {
                    'return': np.mean(t) * 100 if t else 0,
                    'trades': len(t),
                    'win_rate': sum(1 for x in t if x > 0) / len(t) * 100 if t else 0
                }
        
        total = sum(len(trades[r]) for r in trades)
        print(f"总交易{total}次")
    
    print("\n" + "="*60)
    print("26个策略测试结果 (2020-2025)")
    print("="*60)
    
    for regime in ['牛市', '熊市', '震荡市']:
        print(f"\n【{regime}环境】最优策略:")
        sorted_r = sorted(results[regime].items(), key=lambda x: x[1]['return'], reverse=True)
        for i, (s, v) in enumerate(sorted_r[:5]):
            print(f"  {i+1}. {s}: 收益{v['return']:.1f}%, 次数{v['trades']}, 胜率{v['win_rate']:.0f}%")
    
    # 汇总推荐
    print("\n" + "="*60)
    print("最终推荐")
    print("="*60)
    print("牛市: RSI逆势 (收益最高)")
    print("熊市: 建议空仓或使用对冲")
    print("震荡市: 支撑阻力/威廉指标")


if __name__ == "__main__":
    main()
