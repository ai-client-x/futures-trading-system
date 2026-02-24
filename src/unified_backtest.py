#!/usr/bin/env python3
"""
统一回测系统 - 优化版
按用户规范，但做了性能优化：
- 每天只处理成交额前100的股票
- 基本面数据预加载
- 快速信号计算
"""

import sqlite3
import pandas as pd
import numpy as np
from typing import List, Dict
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import importlib.util
spec = importlib.util.spec_from_file_location("signal_strength", "src/signal_strength.py")
signal_strength = importlib.util.module_from_spec(spec)
spec.loader.exec_module(signal_strength)
calc_signal_strength = signal_strength.calc_signal_strength


class OptimizedBacktest:
    """26个策略，按市场环境分组"""
    STRATEGIES = {
        '牛市': ['成交量突破', 'MACD+成交量', 'MACD策略', '突破前高', '均线发散', 
                '量价齐升', 'RSI趋势', '趋势过滤', '均线策略', '均线交叉强度',
                '收盘站均线', '成交量+均线', '突破确认', '平台突破'],
        '熊市': ['动量反转', '威廉指标', 'RSI逆势', '双底形态', '缩量回调', 'MACD背离'],
        '震荡市': ['布林带', 'RSI+均线', '布林带+RSI', '支撑阻力', '波动率突破', '均线收复']
    }
    
    def __init__(self, initial_capital=1000000, tp=0.20, sl=0.10, signal_threshold=20, 
                 max_stocks_per_day=100):
        self.initial_capital = initial_capital
        self.tp = tp
        self.sl = sl
        self.signal_threshold = signal_threshold
        self.max_stocks_per_day = max_stocks_per_day
        
        self.current_regime = '震荡市'
        self.selected_strategy = '布林带'
        
        self.positions = {}
        self.cash = initial_capital
        self.trades = []
        
        # 基本面数据（预加载）
        self.fundamentals = {}
        
    def load_fundamentals(self):
        """预加载基本面数据"""
        conn = sqlite3.connect('data/stocks.db')
        # 使用fundamentals表（当前基本面）
        df = pd.read_sql("""
            SELECT ts_code, pe, roe, dv_ratio, debt_to_assets as debt_to_asset, total_mv as market_cap
            FROM fundamentals
            WHERE pe IS NOT NULL
        """, conn)
        conn.close()
        
        for _, row in df.iterrows():
            self.fundamentals[row['ts_code']] = {
                'pe': row['pe'] or 999,
                'roe': row['roe'] or 0,
                'dv_ratio': row['dv_ratio'] or 0,
                'debt_to_asset': row['debt_to_asset'] or 100,
                'market_cap': row['market_cap'] or 0
            }
        print(f"基本面数据: {len(self.fundamentals)}只股票")
    
    def filter_by_fundamentals(self, stocks: List[str]) -> List[str]:
        """基本面筛选"""
        return [s for s in stocks if s in self.fundamentals and 
                self.fundamentals[s]['pe'] < 25 and 
                self.fundamentals[s]['roe'] > 10 and
                self.fundamentals[s]['dv_ratio'] > 1 and
                self.fundamentals[s]['debt_to_asset'] < 70 and
                self.fundamentals[s]['market_cap'] > 30]
    
    def detect_market_regime(self, stocks_data: Dict, date_idx: int, dates: List[str]) -> str:
        """检测市场环境"""
        if date_idx < 60:
            return self.current_regime
        
        # 使用沪深300成分股的平均涨幅
        closes = []
        sample_size = min(300, len(stocks_data))
        
        for i, (code, data) in enumerate(stocks_data.items()):
            if i >= sample_size:
                break
            if date_idx >= 0 and date_idx < len(data['close']) and data['close'][date_idx] > 0:
                start_idx = date_idx - 60
                if start_idx >= 0 and data['close'][start_idx] > 0:
                    ret = (data['close'][date_idx] - data['close'][start_idx]) / data['close'][start_idx]
                    closes.append(ret)
        
        if len(closes) < 30:
            return self.current_regime
        
        avg_ret = np.mean(closes)
        if avg_ret > 0.15:
            return '牛市'
        elif avg_ret < -0.15:
            return '熊市'
        return '震荡市'
    
    def select_best_strategy(self, regime: str) -> str:
        """选择1个最优策略"""
        return self.STRATEGIES.get(regime, self.STRATEGIES['震荡市'])[0]
    
    def calculate_signal(self, hist: pd.DataFrame, strategy: str) -> float:
        """计算信号分数"""
        if hist.empty or len(hist) < 30:
            return 0
        try:
            return calc_signal_strength(hist, strategy, 'buy')
        except:
            return 0
    
    def get_signals(self, stocks_data: Dict, date_idx: int, candidates: List[str], strategy: str) -> List[Dict]:
        """获取信号股票"""
        signals = []
        
        for code in candidates[:self.max_stocks_per_day]:
            if code not in stocks_data:
                continue
            
            data = stocks_data[code]
            if date_idx >= len(data['close']) or data['close'][date_idx] <= 0:
                continue
            
            start_idx = max(0, date_idx - 60)
            if date_idx - start_idx < 30:
                continue
            
            hist = pd.DataFrame({
                'Open': data['open'][start_idx:date_idx+1],
                'High': data['high'][start_idx:date_idx+1],
                'Low': data['low'][start_idx:date_idx+1],
                'Close': data['close'][start_idx:date_idx+1],
                'Volume': data['vol'][start_idx:date_idx+1]
            })
            
            score = self.calculate_signal(hist, strategy)
            if score >= self.signal_threshold:
                signals.append({
                    'code': code,
                    'price': data['close'][date_idx],
                    'score': score
                })
        
        signals.sort(key=lambda x: x['score'], reverse=True)
        return signals
    
    def load_data(self, start_date: str, end_date: str) -> Dict:
        """加载日线数据 - 只加载每日成交额前100的股票"""
        conn = sqlite3.connect('data/stocks.db')
        
        # 获取交易日
        dates_df = pd.read_sql(f"""
            SELECT DISTINCT trade_date FROM daily 
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            ORDER BY trade_date
        """, conn)
        dates = [str(d) for d in dates_df['trade_date'].tolist()]
        
        # 获取每日成交额前100的股票
        top_stocks_df = pd.read_sql(f"""
            SELECT trade_date, ts_code, open, high, low, close, vol, amount
            FROM daily 
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            AND amount >= 300000  -- 日成交额>=30万
            ORDER BY trade_date, amount DESC
        """, conn)
        conn.close()
        
        # 只取每日成交额前100
        top_stocks_df = top_stocks_df.groupby('trade_date').head(self.max_stocks_per_day)
        
        # 构建数据
        date_to_idx = {d: i for i, d in enumerate(dates)}
        
        stocks_data = {}
        for code in top_stocks_df['ts_code'].unique():
            code_df = top_stocks_df[top_stocks_df['ts_code'] == code].copy()
            code_df['date_idx'] = code_df['trade_date'].map(date_to_idx)
            code_df = code_df.dropna(subset=['date_idx'])
            code_df['date_idx'] = code_df['date_idx'].astype(int)
            
            n = len(dates)
            stocks_data[code] = {
                'open': np.zeros(n),
                'high': np.zeros(n),
                'low': np.zeros(n),
                'close': np.zeros(n),
                'vol': np.zeros(n),
            }
            
            for _, row in code_df.iterrows():
                idx = int(row['date_idx'])
                if 0 <= idx < n:
                    stocks_data[code]['open'][idx] = row['open']
                    stocks_data[code]['high'][idx] = row['high']
                    stocks_data[code]['low'][idx] = row['low']
                    stocks_data[code]['close'][idx] = row['close']
                    stocks_data[code]['vol'][idx] = row['vol']
            
            # 前向填充
            for key in stocks_data[code]:
                last = 0
                for i in range(n):
                    if stocks_data[code][key][i] == 0:
                        stocks_data[code][key][i] = last
                    else:
                        last = stocks_data[code][key][i]
        
        return {'stocks': stocks_data, 'dates': dates}
    
    def run(self, start_date: str, end_date: str, verbose: bool = True):
        """运行回测"""
        if verbose:
            print(f"加载数据: {start_date} ~ {end_date}")
        
        # 预加载基本面
        self.load_fundamentals()
        
        # 加载日线数据
        data = self.load_data(start_date, end_date)
        stocks_data = data['stocks']
        dates = data['dates']
        
        if verbose:
            print(f"股票数: {len(stocks_data)}, 交易日: {len(dates)}")
        
        self.cash = self.initial_capital
        self.positions = {}
        self.trades = []
        
        for i, date in enumerate(dates):
            if verbose and (i + 1) % 100 == 0:
                print(f"Day {i+1}/{len(dates)}")
            
            # === 开盘前 ===
            # 1. 检测市场环境（每天）
            new_regime = self.detect_market_regime(stocks_data, i, dates)
            
            # 2. 环境变化时卖出
            if new_regime != self.current_regime:
                self.current_regime = new_regime
                self.selected_strategy = self.select_best_strategy(new_regime)
                
                # 卖出不适用新环境的持仓
                valid_strategies = self.STRATEGIES.get(new_regime, [])
                for code in list(self.positions.keys()):
                    if self.positions[code]['strategy'] not in valid_strategies:
                        self._sell_stock(stocks_data, i, code)
            
            # 3. 基本面筛选
            candidates = self.filter_by_fundamentals(list(stocks_data.keys()))
            
            # 4. 检查持仓
            position_value = sum(p['cost'] * p['qty'] for p in self.positions.values())
            total_assets = self.cash + position_value
            position_ratio = position_value / total_assets if total_assets > 0 else 0
            
            # 5. 未满仓时买入
            if position_ratio < 0.9 and self.cash > 10000 and candidates:
                strategy = self.select_best_strategy(self.current_regime)
                signals = self.get_signals(stocks_data, i, candidates, strategy)
                
                # 调试
                if i < 5:
                    print(f"Day {i}: regime={self.current_regime}, strategy={strategy}, candidates={len(candidates)}, signals={len(signals)}")
                
                # 每有10%未持仓买1只
                target = int(total_assets * 0.1 / (self.initial_capital * 0.1))
                current = len(self.positions)
                
                for sig in signals[:max(0, target - current)]:
                    if self.cash >= self.initial_capital * 0.1:
                        self._buy_stock(stocks_data, i, sig['code'], sig['price'])
            
            # === 开盘时 ===
            # 6. 止盈止损检查（T+1限制）
            for code in list(self.positions.keys()):
                pos = self.positions[code]
                days_held = pos.get('days_held', 0)
                
                if days_held < 1:  # T+1限制
                    self.positions[code]['days_held'] = days_held + 1
                    continue
                
                price = stocks_data[code]['close'][i] if code in stocks_data else pos['cost']
                ret = (price - pos['cost']) / pos['cost']
                
                if ret >= self.tp or ret <= -self.sl:
                    self._sell_stock(stocks_data, i, code)
                else:
                    self.positions[code]['days_held'] = days_held + 1
        
        return self._calculate_results()
    
    def _buy_stock(self, stocks_data: Dict, date_idx: int, code: str, price: float):
        """买入"""
        qty = int(self.initial_capital * 0.1 / price / 100) * 100
        if qty <= 0:
            return
        
        cost = price * qty * 1.001
        if cost > self.cash:
            qty = int(self.cash / price / 100) * 100
            cost = price * qty * 1.001
        
        if qty <= 0:
            return
        
        self.cash -= cost
        self.positions[code] = {
            'cost': price, 'qty': qty, 'strategy': self.selected_strategy,
            'days_held': 0
        }
    
    def _sell_stock(self, stocks_data: Dict, date_idx: int, code: str):
        """卖出"""
        if code not in self.positions:
            return
        
        pos = self.positions[code]
        price = stocks_data[code]['close'][date_idx] if code in stocks_data else pos['cost']
        
        revenue = price * pos['qty'] * 0.999
        ret = (price - pos['cost']) / pos['cost']
        
        self.trades.append({'code': code, 'return': ret, 'regime': self.current_regime})
        self.cash += revenue
        del self.positions[code]
    
    def _calculate_results(self) -> Dict:
        """计算结果"""
        position_value = sum(p['cost'] * p['qty'] for p in self.positions.values())
        final_assets = self.cash + position_value
        total_return = (final_assets - self.initial_capital) / self.initial_capital
        
        returns = [t['return'] for t in self.trades]
        
        return {
            'initial_capital': self.initial_capital,
            'final_assets': final_assets,
            'total_return': total_return,
            'total_trades': len(self.trades),
            'win_rate': sum(1 for r in returns if r > 0) / len(returns) if returns else 0,
            'avg_return': np.mean(returns) if returns else 0,
            'final_positions': len(self.positions)
        }


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default='20191001')
    parser.add_argument('--end', default='20191231')
    parser.add_argument('--capital', type=float, default=1000000)
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    
    bt = OptimizedBacktest(initial_capital=args.capital)
    results = bt.run(args.start, args.end, verbose=True)
    
    print("\n" + "="*50)
    print("回测结果")
    print("="*50)
    print(f"初始资金: {results['initial_capital']:,.0f}")
    print(f"最终资产: {results['final_assets']:,.0f}")
    print(f"总收益: {results['total_return']*100:.2f}%")
    print(f"交易次数: {results['total_trades']}")
    print(f"胜率: {results['win_rate']*100:.1f}%")


if __name__ == "__main__":
    main()
