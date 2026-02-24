#!/usr/bin/env python3
"""
统一回测系统 - 向量化版 + 参数优化
简化版
"""

import sqlite3
import pandas as pd
import numpy as np
from typing import List, Dict
import sys
import os
import logging

logging.basicConfig(level=logging.WARNING)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import importlib.util
spec = importlib.util.spec_from_file_location("signal_strength", "src/signal_strength.py")
signal_strength = importlib.util.module_from_spec(spec)
spec.loader.exec_module(signal_strength)
calc_signal_strength = signal_strength.calc_signal_strength


class VectorizedBacktest:
    STRATEGY_MAP = {
        '牛市': ['成交量突破', 'MACD+成交量', 'MACD策略', '突破前高', '均线发散', 
                '量价齐升', 'RSI趋势', '趋势过滤', '均线策略', '均线交叉强度',
                '收盘站均线', '成交量+均线', '突破确认', '平台突破'],
        '熊市': ['动量反转', '威廉指标', 'RSI逆势', '双底形态', '缩量回调', 'MACD背离'],
        '震荡市': ['布林带', 'RSI+均线', '布林带+RSI', '支撑阻力', '波动率突破', '均线收复']
    }
    
    def __init__(self, initial_capital=1000000, tp=0.20, sl=0.10, signal_threshold=40, 
                 max_positions=10, position_pct=0.1):
        self.initial_capital = initial_capital
        self.tp = tp
        self.sl = sl
        self.signal_threshold = signal_threshold
        self.max_positions = max_positions
        self.position_pct = position_pct
        self.current_regime = '震荡市'
        self.selected_strategies = self.STRATEGY_MAP['震荡市']
    
    def load_data(self, start_date: str, end_date: str) -> Dict:
        conn = sqlite3.connect('data/stocks.db')
        dates_df = pd.read_sql(f"""
            SELECT DISTINCT trade_date FROM daily 
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            ORDER BY trade_date
        """, conn)
        dates = [str(d) for d in dates_df['trade_date'].tolist()]
        date_to_idx = {d: i for i, d in enumerate(dates)}
        
        active_df = pd.read_sql(f"""
            SELECT trade_date, ts_code, open, high, low, close, vol
            FROM daily 
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            AND amount >= 30
            ORDER BY trade_date, amount DESC
        """, conn)
        conn.close()
        
        active_df['date_idx'] = active_df['trade_date'].map(date_to_idx)
        active_df = active_df.dropna(subset=['date_idx'])
        active_df = active_df[active_df.groupby('trade_date').cumcount() < 50]
        
        stocks = {}
        for code in active_df['ts_code'].unique():
            code_df = active_df[active_df['ts_code'] == code].sort_values('date_idx')
            n = len(dates)
            stocks[code] = {
                'open': self._build_array(code_df, 'open', 'date_idx', n),
                'high': self._build_array(code_df, 'high', 'date_idx', n),
                'low': self._build_array(code_df, 'low', 'date_idx', n),
                'close': self._build_array(code_df, 'close', 'date_idx', n),
                'vol': self._build_array(code_df, 'vol', 'date_idx', n),
            }
        return {'stocks': stocks, 'dates': dates}
    
    def _build_array(self, df, col, idx_col, n):
        arr = np.zeros(n)
        for _, row in df.iterrows():
            idx = int(row[idx_col])
            if 0 <= idx < n:
                arr[idx] = row[col]
        last = 0
        for i in range(n):
            if arr[i] == 0:
                arr[i] = last
            else:
                last = arr[i]
        return arr
    
    def get_price(self, stocks, code, i):
        return stocks.get(code, {}).get('close', np.array([0]))[i] if code in stocks else 0
    
    def detect_regime(self, stocks, i):
        closes = [data['close'][i] for data in stocks.values() if data['close'][i] > 0]
        if len(closes) < 30:
            return self.current_regime
        
        start = max(0, i - 60)
        avgs = []
        for j in range(start, i + 1):
            day_c = [data['close'][j] for data in stocks.values() if data['close'][j] > 0]
            if day_c:
                avgs.append(np.mean(day_c))
        
        if len(avgs) < 30:
            return self.current_regime
        
        ret = (avgs[-1] - avgs[0]) / avgs[0] * 100
        if ret > 15:
            return '牛市'
        elif ret < -15:
            return '熊市'
        return '震荡市'
    
    def get_signals(self, stocks, i, exclude):
        signals = []
        for code, data in stocks.items():
            if code in exclude or data['close'][i] <= 0:
                continue
            
            hist = self._get_hist(stocks, code, i)
            if len(hist) < 30:
                continue
            
            scores = [calc_signal_strength(hist, s, 'buy') for s in self.selected_strategies[:3]]
            scores = [s for s in scores if s > 0]
            avg = np.mean(scores) if scores else 0
            
            if avg >= self.signal_threshold and data['close'][i] > 0:
                signals.append({'code': code, 'price': data['close'][i], 'score': avg})
        return signals
    
    def _get_hist(self, stocks, code, i):
        if code not in stocks:
            return pd.DataFrame()
        d = stocks[code]
        s = max(0, i - 60)
        if i - s < 30:
            return pd.DataFrame()
        return pd.DataFrame({
            'Open': d['open'][s:i+1], 'High': d['high'][s:i+1],
            'Low': d['low'][s:i+1], 'Close': d['close'][s:i+1], 'Volume': d['vol'][s:i+1]
        })
    
    def run(self, start_date, end_date, verbose=False):
        data = self.load_data(start_date, end_date)
        stocks, dates = data['stocks'], data['dates']
        
        cash = self.initial_capital
        positions = {}
        trades = []
        equity = []
        
        for i, date in enumerate(dates):
            if verbose and (i + 1) % 100 == 0:
                print(f"Day {i+1}/{len(dates)}")
            
            # 市场环境
            new_regime = self.detect_regime(stocks, i)
            if new_regime != self.current_regime:
                old_strats = self.STRATEGY_MAP.get(self.current_regime, [])
                new_strats = self.STRATEGY_MAP.get(new_regime, [])
                for code in list(positions.keys()):
                    if positions[code].get('strategy', '') not in new_strats:
                        p = self.get_price(stocks, code, i)
                        if p > 0:
                            r = (p - positions[code]['cost']) / positions[code]['cost']
                            cash += p * positions[code]['qty'] * 0.999
                            trades.append({'return': r, 'action': 'sell'})
                            del positions[code]
                self.current_regime = new_regime
                self.selected_strategies = self.STRATEGY_MAP.get(new_regime, self.STRATEGY_MAP['震荡市'])
            
            # 止盈止损
            for code in list(positions.keys()):
                p = self.get_price(stocks, code, i)
                if p > 0:
                    r = (p - positions[code]['cost']) / positions[code]['cost']
                    if r >= self.tp or r <= -self.sl:
                        cash += p * positions[code]['qty'] * 0.999
                        trades.append({'return': r, 'action': 'sell'})
                        del positions[code]
            
            # 买入
            if len(positions) < self.max_positions and cash > 10000:
                sigs = self.get_signals(stocks, i, set(positions.keys()))
                sigs.sort(key=lambda x: x['score'], reverse=True)
                for s in sigs[:3]:
                    if len(positions) >= self.max_positions or cash < 10000:
                        break
                    q = int(cash * self.position_pct / s['price'] / 100) * 100
                    if q > 0 and s['price'] * q * 1.001 <= cash:
                        cash -= s['price'] * q * 1.001
                        positions[s['code']] = {'cost': s['price'], 'qty': q, 'strategy': self.selected_strategies[0]}
                        trades.append({'action': 'buy'})
            
            # 权益
            total = cash + sum(self.get_price(stocks, c, i) * p['qty'] for c, p in positions.items() if self.get_price(stocks, c, i) > 0)
            equity.append(total)
        
        return self._calc(equity, trades)
    
    def _calc(self, equity, trades):
        final = equity[-1] if equity else self.initial_capital
        ret = (final - self.initial_capital) / self.initial_capital * 100
        years = len(equity) / 252 if equity else 1
        annual = ((final / self.initial_capital) ** (1/years) - 1) * 100 if years > 0 else 0
        
        peak = self.initial_capital
        max_dd = 0
        for e in equity:
            if e > peak:
                peak = e
            dd = (peak - e) / peak * 100
            if dd > max_dd:
                max_dd = dd
        
        sells = [t for t in trades if t['action'] == 'sell']
        wins = sum(1 for t in sells if t.get('return', 0) > 0)
        win_rate = wins / len(sells) * 100 if sells else 0
        
        return {
            'total_return': ret, 'annual_return': annual, 'max_drawdown': max_dd,
            'win_rate': win_rate, 'total_trades': len(trades)
        }


def optimize():
    print("参数优化...")
    
    # 测试关键参数组合
    results = []
    
    # 1. 止盈止损组合
    for tp in [0.15, 0.20, 0.25, 0.30]:
        for sl in [0.08, 0.10, 0.15]:
            if tp <= sl:
                continue
            bt = VectorizedBacktest(tp=tp, sl=sl)
            r = bt.run('20191001', '20191231')
            score = r['total_return'] * 2 - r['max_drawdown']
            results.append((score, tp, sl, r))
            print(f"tp={tp}, sl={sl} -> 收益:{r['total_return']:.1f}%, 回撤:{r['max_drawdown']:.1f}%")
    
    # 找最佳
    results.sort(key=lambda x: x[0], reverse=True)
    best = results[0]
    
    print("\n最佳止盈止损:")
    print(f"  止盈: {best[1]}, 止损: {best[2]}")
    print(f"  收益: {best[3]['total_return']:.1f}%")
    print(f"  回撤: {best[3]['max_drawdown']:.1f}%")
    print(f"  胜率: {best[3]['win_rate']:.0f}%")
    
    # 2. 用最佳参数测试信号阈值
    print("\n信号阈值优化...")
    best_tp, best_sl = best[1], best[2]
    for th in [30, 40, 50, 60]:
        bt = VectorizedBacktest(tp=best_tp, sl=best_sl, signal_threshold=th)
        r = bt.run('20191001', '20191231')
        print(f"阈值={th} -> 收益:{r['total_return']:.1f}%, 交易数:{r['total_trades']}")
    
    # 3. 测试仓位
    print("\n仓位优化...")
    for pct in [0.1, 0.15, 0.2]:
        bt = VectorizedBacktest(tp=best_tp, sl=best_sl, position_pct=pct)
        r = bt.run('20191001', '20191231')
        print(f"仓位={pct} -> 收益:{r['total_return']:.1f}%")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default='20191001')
    parser.add_argument('--end', default='20191231')
    parser.add_argument('--optimize', action='store_true')
    args = parser.parse_args()
    
    if args.optimize:
        optimize()
    else:
        bt = VectorizedBacktest(1000000)
        r = bt.run(args.start, args.end, verbose=True)
        print("\n结果:")
        for k, v in r.items():
            print(f"  {k}: {v}")
