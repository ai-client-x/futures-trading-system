#!/usr/bin/env python3
"""
多策略对比回测
包含:
1. 纯技术面策略
2. 混合策略 (技术面+风控)
3. 突破策略

回测期: 2020-2024
"""

import os
import sys
import json
import sqlite3
import logging
from datetime import datetime
from typing import Dict, List

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 添加项目路径并导入config
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src.config import config

DB_PATH = "data/stocks.db"
INITIAL_CAPITAL = config.initial_capital

# 回测期间 (从config读取)
DEVELOP_START = config.develop_start
DEVELOP_END = config.develop_end  
BACKTEST_START = config.backtest_start
BACKTEST_END = config.backtest_end


class TradingCosts:
    """交易成本"""
    COMMISSION = 0.00015
    MIN_COMM = 5
    STAMP = 0.001
    TRANSFER = 0.00002
    SLIPPAGE = 0.0005
    
    @classmethod
    def buy_cost(cls, amount):
        comm = max(amount * cls.COMMISSION, cls.MIN_COMM)
        return comm + amount * cls.TRANSFER + amount * cls.SLIPPAGE
    
    @classmethod
    def sell_cost(cls, amount):
        comm = max(amount * cls.COMMISSION, cls.MIN_COMM)
        return comm + amount * cls.STAMP + amount * cls.TRANSFER + amount * cls.SLIPPAGE


class MultiStrategyBacktest:
    """多策略回测"""
    
    def __init__(self):
        self.db_path = DB_PATH
        self.initial_capital = INITIAL_CAPITAL
        
        # 测试股票池 (从config读取，如果为空则使用默认列表)
        # 这里使用基本面筛选后的候选股票作为默认池
        default_stocks = [
            '600519.SH', '000858.SZ', '601318.SH', '300750.SZ', '002594.SZ',
            '600036.SH', '600900.SH', '601888.SH', '601319.SH', '601601.SH',
            '000651.SZ', '600887.SH', '603605.SH', '603369.SH', '000538.SZ',
            '600938.SH', '601021.SH', '603195.SH', '603855.SH', '603043.SH',
            '603619.SH', '688628.SH', '002714.SZ', '002489.SZ', '002895.SZ',
            '002605.SZ', '002351.SZ', '603008.SH', '002705.SZ', '002832.SZ'
        ]
        
        # 从config读取股票池
        config_stocks = [s['code'] + ('.SH' if s['code'].startswith('6') else '.SZ') 
                        for s in config.stock_pool] if config.stock_pool else []
        
        self.stock_pool = config_stocks if config_stocks else default_stocks
    
    def _conn(self):
        return sqlite3.connect(self.db_path)
    
    def get_price_data(self, code, start, end):
        """获取价格数据"""
        conn = self._conn()
        df = pd.read_sql(f"""
            SELECT trade_date, close FROM daily
            WHERE ts_code = '{code}' AND trade_date >= '{start}' AND trade_date <= '{end}'
            ORDER BY trade_date
        """, conn)
        conn.close()
        return df
    
    def get_all_prices(self, codes, start, end):
        """批量获取价格"""
        conn = self._conn()
        placeholders = ','.join([f"'{c}'" for c in codes])
        df = pd.read_sql(f"""
            SELECT ts_code, trade_date, close FROM daily
            WHERE ts_code IN ({placeholders}) AND trade_date >= '{start}' AND trade_date <= '{end}'
            ORDER BY ts_code, trade_date
        """, conn)
        conn.close()
        return df
    
    # ============ 策略1: 趋势跟踪 ============
    def trend_strategy(self, df):
        """均线金叉死叉"""
        if df is None or len(df) < 60:
            return 'hold'
        
        close = df['close']
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        
        if len(df) < 2:
            return 'hold'
        
        if ma5.iloc[-1] > ma20.iloc[-1] and ma5.iloc[-2] <= ma20.iloc[-2]:
            return 'buy'
        if ma5.iloc[-1] < ma20.iloc[-1] and ma5.iloc[-2] >= ma20.iloc[-2]:
            return 'sell'
        return 'hold'
    
    # ============ 策略2: 突破策略 ============
    def breakout_strategy(self, df):
        """20日新高突破"""
        if df is None or len(df) < 25:
            return 'hold'
        
        close = df['close']
        high20 = close.rolling(20).max()
        
        if close.iloc[-1] > high20.iloc[-2] and close.iloc[-2] <= high20.iloc[-2]:
            return 'buy'
        if close.iloc[-1] < close.iloc[-20] * 0.95:  # 跌破20日最低
            return 'sell'
        return 'hold'
    
    # ============ 策略3: MACD策略 ============
    def macd_strategy(self, df):
        """MACD金叉死叉"""
        if df is None or len(df) < 60:
            return 'hold'
        
        close = df['close']
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9).mean()
        
        if len(df) < 2:
            return 'hold'
        
        if macd.iloc[-1] > signal.iloc[-1] and macd.iloc[-2] <= signal.iloc[-2]:
            return 'buy'
        if macd.iloc[-1] < signal.iloc[-1] and macd.iloc[-2] >= signal.iloc[-2]:
            return 'sell'
        return 'hold'
    
    # ============ 策略4: 动量策略 ============
    def momentum_strategy(self, df):
        """20日动量"""
        if df is None or len(df) < 30:
            return 'hold'
        
        close = df['close']
        mom = close / close.shift(20) - 1
        
        if mom.iloc[-1] > 0.1:  # 10%以上动量
            return 'buy'
        if mom.iloc[-1] < -0.05:
            return 'sell'
        return 'hold'
    
    def run_strategy(self, name, strategy_func, start_date, end_date):
        """运行单个策略回测"""
        logger.info(f"运行策略: {name}")
        
        capital = self.initial_capital
        position = None
        trades = []
        
        dates = pd.date_range(start=start_date, end=end_date, freq='W-FRI')
        
        for date in dates:
            date_str = date.strftime('%Y%m%d')
            
            if position:
                # 检查卖出
                df = self.get_price_data(position['code'], start_date, date_str)
                if df is not None and len(df) >= 20:
                    signal = strategy_func(df)
                    
                    # 止盈止损
                    current_price = df.iloc[-1]['close']
                    cost = position['cost']
                    
                    if (current_price <= cost * 0.97 or  # 3%止损
                        current_price >= cost * 1.05 or  # 5%止盈
                        signal == 'sell'):
                        
                        revenue = current_price * position['qty'] - TradingCosts.sell_cost(current_price * position['qty'])
                        capital += revenue
                        trades.append({'date': date_str, 'action': 'sell', 'price': current_price})
                        position = None
            
            # 检查买入
            if not position and capital > 0:
                # 遍历股票池找信号
                for code in self.stock_pool[:15]:
                    df = self.get_price_data(code, start_date, date_str)
                    if df is not None and len(df) >= 60:
                        signal = strategy_func(df)
                        if signal == 'buy':
                            price = df.iloc[-1]['close']
                            qty = int(capital * 0.3 / price / 100) * 100
                            if qty > 0:
                                cost = price * qty + TradingCosts.buy_cost(price * qty)
                                if cost <= capital:
                                    capital -= cost
                                    position = {'code': code, 'qty': qty, 'cost': price}
                                    trades.append({'date': date_str, 'action': 'buy', 'price': price, 'code': code})
                                    break
        
        # 最终清仓
        if position:
            df = self.get_price_data(position['code'], start_date, end_date)
            if df is not None and len(df) > 0:
                price = df.iloc[-1]['close']
                capital += price * position['qty'] - TradingCosts.sell_cost(price * position['qty'])
        
        total_return = (capital - self.initial_capital) / self.initial_capital
        years = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(start_date, '%Y%m%d')).days / 365
        
        return {
            'strategy': name,
            'initial_capital': self.initial_capital,
            'final_assets': capital,
            'total_return': total_return * 100,
            'annual_return': ((1 + total_return) ** (1/years) - 1) * 100 if years > 0 else 0,
            'trade_count': len(trades)
        }
    
    def run_all(self):
        """运行所有策略"""
        strategies = [
            ("趋势跟踪", self.trend_strategy),
            ("MACD策略", self.macd_strategy),
            ("动量策略", self.momentum_strategy),
            ("突破策略", self.breakout_strategy),
        ]
        
        results = {'develop': {}, 'backtest': {}}
        
        # 开发期
        logger.info("=" * 50)
        logger.info("开发期 (2020-2022)")
        logger.info("=" * 50)
        
        for name, func in strategies:
            result = self.run_strategy(name, func, DEVELOP_START, DEVELOP_END)
            results['develop'][name] = result
        
        # 回测期
        logger.info("=" * 50)
        logger.info("回测期 (2023-2024)")
        logger.info("=" * 50)
        
        for name, func in strategies:
            result = self.run_strategy(name, func, BACKTEST_START, BACKTEST_END)
            results['backtest'][name] = result
        
        return results


def main():
    print("="*70)
    print("📊 多策略对比回测 (2020-2024)")
    print("="*70)
    print(f"初始资金: ¥{INITIAL_CAPITAL:,}")
    print(f"股票池: 30只基本面候选股")
    print(f"风控: 3%止损, 5%止盈")
    print("="*70)
    
    engine = MultiStrategyBacktest()
    results = engine.run_all()
    
    # 打印结果
    print("\n" + "="*70)
    print("📈 回测结果")
    print("="*70)
    
    for period in ['develop', 'backtest']:
        period_name = '开发期 (2020-2022)' if period == 'develop' else '回测期 (2023-2024)'
        print(f"\n【{period_name}】")
        
        total_return = 0
        for name, result in results[period].items():
            print(f"  {result['strategy']:10s}: 收益={result['total_return']:>7.2f}%, 年化={result['annual_return']:>7.2f}%, 交易={result['trade_count']}")
            total_return += result['total_return']
        
        # 综合策略 (平均)
        avg_return = total_return / len(results[period])
        print(f"  {'综合策略':10s}: 收益={avg_return:>7.2f}%")
    
    print("\n" + "="*70)
    
    # 保存
    filepath = f"backtest_results/multi_strategy_compare_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filepath, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"已保存: {filepath}")


if __name__ == "__main__":
    main()
