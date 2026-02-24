#!/usr/bin/env python3
"""
统一回测系统 - 与实盘流程完全一致版
按用户规范：
1. 开盘前：获取活跃股票→基本面筛选→市场环境检测→选择1个最优策略→信号评分→买入
2. 开盘时：日线数据检查卖出/加仓（T+1限制）
"""

import sqlite3
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import importlib.util
spec = importlib.util.spec_from_file_location("signal_strength", "src/signal_strength.py")
signal_strength = importlib.util.module_from_spec(spec)
spec.loader.exec_module(signal_strength)
calc_signal_strength = signal_strength.calc_signal_strength


class UnifiedBacktest:
    """26个策略，按市场环境分组"""
    STRATEGIES = {
        '牛市': ['成交量突破', 'MACD+成交量', 'MACD策略', '突破前高', '均线发散', 
                '量价齐升', 'RSI趋势', '趋势过滤', '均线策略', '均线交叉强度',
                '收盘站均线', '成交量+均线', '突破确认', '平台突破'],
        '熊市': ['动量反转', '威廉指标', 'RSI逆势', '双底形态', '缩量回调', 'MACD背离'],
        '震荡市': ['布林带', 'RSI+均线', '布林带+RSI', '支撑阻力', '波动率突破', '均线收复']
    }
    
    def __init__(self, initial_capital=1000000, tp=0.20, sl=0.10, signal_threshold=40):
        self.initial_capital = initial_capital
        self.tp = tp  # 止盈 20%
        self.sl = sl  # 止损 10%
        self.signal_threshold = signal_threshold
        
        self.current_regime = '震荡市'
        self.selected_strategy = '布林带'  # 默认策略
        
        # 持仓: {code: {'cost': price, 'qty': qty, 'strategy': str, 'buy_date': date, 'days_held': 0}}
        self.positions = {}
        self.cash = initial_capital
        
        # 交易记录
        self.trades = []
        
        # 加载基本面数据
        self.fundamentals = {}
        
    def load_fundamentals(self, date: str):
        """加载某天的基本面数据（简化：预加载）"""
        # 预加载所有基本面数据到内存
        if not self.fundamentals:
            conn = sqlite3.connect('data/stocks.db')
            df = pd.read_sql("""
                SELECT ts_code, pe, roe, dv_ratio, debt_to_asset, market_cap, report_date
                FROM fundamentals_history
                WHERE report_date IS NOT NULL
            """, conn)
            conn.close()
            
            # 按股票和报告日期排序，取最新的
            df = df.sort_values(['ts_code', 'report_date'])
            df = df.drop_duplicates('ts_code', keep='last')
            
            for _, row in df.iterrows():
                self.fundamentals[row['ts_code']] = {
                    'pe': row['pe'] or 999,
                    'roe': row['roe'] or 0,
                    'dv_ratio': row['dv_ratio'] or 0,
                    'debt_to_asset': row['debt_to_asset'] or 100,
                    'market_cap': row['market_cap'] or 0
                }
    
    def get_active_stocks(self, date: str) -> List[str]:
        """获取活跃股票：60天日均成交额>=3000万"""
        conn = sqlite3.connect('data/stocks.db')
        df = pd.read_sql(f"""
            SELECT ts_code FROM daily
            WHERE trade_date >= date('{date}', '-60 days')
            AND trade_date <= '{date}'
            GROUP BY ts_code
            HAVING AVG(amount) >= 30000000
        """, conn)
        conn.close()
        return df['ts_code'].tolist()
    
    def filter_by_fundamentals(self, stocks: List[str]) -> List[str]:
        """基本面筛选：PE<25, ROE>10%, 股息率>1%, 负债<70%, 市值>30亿"""
        filtered = []
        for code in stocks:
            if code not in self.fundamentals:
                continue
            f = self.fundamentals[code]
            if (f['pe'] < 25 and f['roe'] > 10 and f['dv_ratio'] > 1 
                and f['debt_to_asset'] < 70 and f['market_cap'] > 30):
                filtered.append(code)
        return filtered
    
    def detect_market_regime(self, stocks_data: Dict, date_idx: int) -> str:
        """检测市场环境：基于沪深300指数走势"""
        # 简化：使用所有股票的平均价格变化
        closes = []
        for code, data in stocks_data.items():
            if date_idx >= 0 and date_idx < len(data['close']):
                price = data['close'][date_idx]
                if price > 0:
                    # 计算60日涨幅
                    start_idx = max(0, date_idx - 60)
                    if start_idx < date_idx and data['close'][start_idx] > 0:
                        ret = (price - data['close'][start_idx]) / data['close'][start_idx] * 100
                        closes.append(ret)
        
        if len(closes) < 30:
            return self.current_regime
        
        avg_ret = np.mean(closes)
        if avg_ret > 15:
            return '牛市'
        elif avg_ret < -15:
            return '熊市'
        return '震荡市'
    
    def select_best_strategy(self, regime: str) -> str:
        """选择1个最优策略"""
        strategies = self.STRATEGIES.get(regime, self.STRATEGIES['震荡市'])
        # 默认选择第一个策略（后续可根据回测表现优化）
        return strategies[0]
    
    def calculate_signal(self, hist: pd.DataFrame, strategy: str) -> float:
        """计算信号分数"""
        if hist.empty or len(hist) < 30:
            return 0
        try:
            return calc_signal_strength(hist, strategy, 'buy')
        except:
            return 0
    
    def get_signals(self, stocks_data: Dict, date_idx: int, candidates: List[str], strategy: str) -> List[Dict]:
        """获取信号股票列表，按分数排序"""
        signals = []
        for code in candidates:
            if code not in stocks_data:
                continue
            data = stocks_data[code]
            if date_idx >= len(data['close']) or data['close'][date_idx] <= 0:
                continue
            
            # 获取历史数据
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
        
        # 按分数排序
        signals.sort(key=lambda x: x['score'], reverse=True)
        return signals
    
    def load_daily_data(self, start_date: str, end_date: str) -> Dict:
        """加载日线数据"""
        conn = sqlite3.connect('data/stocks.db')
        
        # 获取交易日
        dates_df = pd.read_sql(f"""
            SELECT DISTINCT trade_date FROM daily 
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            ORDER BY trade_date
        """, conn)
        dates = [str(d) for d in dates_df['trade_date'].tolist()]
        
        # 获取所有股票数据
        daily_df = pd.read_sql(f"""
            SELECT trade_date, ts_code, open, high, low, close, vol, amount
            FROM daily 
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            ORDER BY trade_date, amount DESC
        """, conn)
        conn.close()
        
        # 构建股票数据字典
        date_to_idx = {d: i for i, d in enumerate(dates)}
        
        stocks_data = {}
        for code in daily_df['ts_code'].unique():
            code_df = daily_df[daily_df['ts_code'] == code].copy()
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
                'amount': np.zeros(n)
            }
            
            for _, row in code_df.iterrows():
                idx = int(row['date_idx'])
                if 0 <= idx < n:
                    stocks_data[code]['open'][idx] = row['open']
                    stocks_data[code]['high'][idx] = row['high']
                    stocks_data[code]['low'][idx] = row['low']
                    stocks_data[code]['close'][idx] = row['close']
                    stocks_data[code]['vol'][idx] = row['vol']
                    stocks_data[code]['amount'][idx] = row['amount']
            
            # 前向填充
            for key in ['open', 'high', 'low', 'close', 'vol', 'amount']:
                last = 0
                for i in range(n):
                    if stocks_data[code][key][i] == 0:
                        stocks_data[code][key][i] = last
                    else:
                        last = stocks_data[code][key][i]
        
        return {'stocks': stocks_data, 'dates': dates}
    
    def check_sell_signals(self, stocks_data: Dict, date_idx: int) -> List[str]:
        """检查卖出信号（日线数据）"""
        to_sell = []
        for code, pos in self.positions.items():
            if code not in stocks_data:
                continue
            
            data = stocks_data[code]
            if date_idx >= len(data['close']) or data['close'][date_idx] <= 0:
                continue
            
            # T+1: 买入后第二天才能卖出
            if pos.get('days_held', 0) < 1:
                continue
            
            # 获取历史数据
            hist = pd.DataFrame({
                'Open': data['open'][max(0, date_idx-30):date_idx+1],
                'High': data['high'][max(0, date_idx-30):date_idx+1],
                'Low': data['low'][max(0, date_idx-30):date_idx+1],
                'Close': data['close'][max(0, date_idx-30):date_idx+1],
                'Volume': data['vol'][max(0, date_idx-30):date_idx+1]
            })
            
            # 计算当前收益率
            current_price = data['close'][date_idx]
            ret = (current_price - pos['cost']) / pos['cost']
            
            # 止盈/止损
            if ret >= self.tp or ret <= -self.sl:
                to_sell.append(code)
                continue
            
            # 策略卖出信号（简化：使用策略的评分）
            score = self.calculate_signal(hist, pos['strategy'])
            if score < self.signal_threshold * 0.5:  # 信号减弱
                to_sell.append(code)
        
        return to_sell
    
    def check_buy_signals(self, stocks_data: Dict, date_idx: int, candidates: List[str], strategy: str) -> List[Dict]:
        """检查加仓信号"""
        # 简化：开盘前已经选好股票，这里只是补充买入
        return []
    
    def run(self, start_date: str, end_date: str, verbose: bool = True):
        """运行回测"""
        if verbose:
            print(f"加载数据: {start_date} ~ {end_date}")
        
        data = self.load_daily_data(start_date, end_date)
        stocks_data = data['stocks']
        dates = data['dates']
        
        # 初始化
        self.cash = self.initial_capital
        self.positions = {}
        self.trades = []
        
        for i, date in enumerate(dates):
            if verbose and (i + 1) % 100 == 0:
                print(f"Day {i+1}/{len(dates)}: {date}")
            
            # ========== 开盘前 ==========
            # 1. 加载当天基本面数据
            self.load_fundamentals(date)
            
            # 2. 获取活跃股票
            active_stocks = self.get_active_stocks(date)
            
            # 3. 基本面筛选
            candidates = self.filter_by_fundamentals(active_stocks)
            
            # 4. 检测市场环境
            new_regime = self.detect_market_regime(stocks_data, i)
            
            # 5. 环境变化时卖出
            if new_regime != self.current_regime:
                old_strategy = self.selected_strategy
                self.current_regime = new_regime
                self.selected_strategy = self.select_best_strategy(new_regime)
                
                if verbose:
                    print(f"  环境变化: {self.current_regime} -> {new_regime}, 策略: {old_strategy} -> {self.selected_strategy}")
                
                # 卖出不适用新环境的持仓
                for code in list(self.positions.keys()):
                    if self.positions[code]['strategy'] != self.selected_strategy:
                        self.sell_stock(stocks_data, i, code)
            
            # 6. 检查持仓
            position_value = sum(p['cost'] * p['qty'] for p in self.positions.values())
            total_assets = self.cash + position_value
            position_ratio = position_value / total_assets if total_assets > 0 else 0
            
            # 7. 未满仓时买入
            if position_ratio < 0.9 and self.cash > 10000:
                # 选择最优策略
                strategy = self.select_best_strategy(self.current_regime)
                
                # 获取信号
                signals = self.get_signals(stocks_data, i, candidates, strategy)
                
                # 买入：每有10%未持仓就买1只
                target_positions = int(total_assets * 0.1 / (self.initial_capital * 0.1))
                current_positions = len(self.positions)
                to_buy = min(target_positions - current_positions, len(signals))
                
                for signal in signals[:to_buy]:
                    if self.cash >= self.initial_capital * 0.1:
                        self.buy_stock(stocks_data, i, signal['code'], signal['price'])
            
            # ========== 开盘时 ==========
            # 检查卖出信号
            to_sell = self.check_sell_signals(stocks_data, i)
            for code in to_sell:
                self.sell_stock(stocks_data, i, code)
            
            # 更新持仓天数
            for code in self.positions:
                self.positions[code]['days_held'] = self.positions[code].get('days_held', 0) + 1
        
        # 计算结果
        return self.calculate_results()
    
    def buy_stock(self, stocks_data: Dict, date_idx: int, code: str, price: float):
        """买入股票"""
        qty = int(self.initial_capital * 0.1 / price / 100) * 100
        if qty <= 0:
            return
        
        cost = price * qty * 1.001  # 考虑手续费
        if cost > self.cash:
            qty = int(self.cash / price / 100) * 100
            cost = price * qty * 1.001
        
        if qty <= 0:
            return
        
        self.cash -= cost
        self.positions[code] = {
            'cost': price,
            'qty': qty,
            'strategy': self.selected_strategy,
            'buy_date': date_idx,
            'days_held': 0
        }
    
    def sell_stock(self, stocks_data: Dict, date_idx: int, code: str):
        """卖出股票"""
        if code not in self.positions:
            return
        
        pos = self.positions[code]
        price = stocks_data[code]['close'][date_idx] if code in stocks_data else pos['cost']
        
        revenue = price * pos['qty'] * 0.999
        ret = (price - pos['cost']) / pos['cost']
        
        self.trades.append({
            'code': code,
            'return': ret,
            'strategy': pos['strategy'],
            'regime': self.current_regime
        })
        
        self.cash += revenue
        del self.positions[code]
    
    def calculate_results(self) -> Dict:
        """计算回测结果"""
        position_value = sum(p['cost'] * p['qty'] for p in self.positions.values())
        final_assets = self.cash + position_value
        total_return = (final_assets - self.initial_capital) / self.initial_capital
        
        returns = [t['return'] for t in self.trades]
        
        # 计算最大回撤
        equity = [self.initial_capital]
        for t in self.trades:
            equity.append(equity[-1] * (1 + t['return']))
        
        max_dd = 0
        peak = equity[0]
        for e in equity:
            if e > peak:
                peak = e
            dd = (peak - e) / peak
            if dd > max_dd:
                max_dd = dd
        
        return {
            'initial_capital': self.initial_capital,
            'final_assets': final_assets,
            'total_return': total_return,
            'total_trades': len(self.trades),
            'win_rate': sum(1 for r in returns if r > 0) / len(returns) if returns else 0,
            'avg_return': np.mean(returns) if returns else 0,
            'max_drawdown': max_dd,
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
    
    bt = UnifiedBacktest(initial_capital=args.capital)
    results = bt.run(args.start, args.end, verbose=args.verbose)
    
    print("\n" + "="*50)
    print("回测结果")
    print("="*50)
    print(f"初始资金: {results['initial_capital']:,.0f}")
    print(f"最终资产: {results['final_assets']:,.0f}")
    print(f"总收益: {results['total_return']*100:.2f}%")
    print(f"交易次数: {results['total_trades']}")
    print(f"胜率: {results['win_rate']*100:.1f}%")
    print(f"最大回撤: {results['max_drawdown']*100:.2f}%")
    print(f"最终持仓: {results['final_positions']}只")


if __name__ == "__main__":
    main()
