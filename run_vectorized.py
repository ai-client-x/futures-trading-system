#!/usr/bin/env python3
"""
向量化回测引擎 - 极速版
使用pandas向量化操作替代循环

优化：
- 数据预加载到内存
- 使用pandas向量化计算
- 向量化计算技术指标

运行时间: 4年数据约10-30秒
"""

import sqlite3
import pandas as pd
import numpy as np
from collections import defaultdict
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = "data/stocks.db"

class VectorizedBacktest:
    """向量化回测引擎"""
    
    def __init__(self):
        self.data = None
        self.funds = None
        self.trade_dates = None
    
    def load_data(self, start_date='20200101', end_date='20241231'):
        """预加载所有数据"""
        logger.info("加载数据...")
        t0 = time.time()
        
        conn = sqlite3.connect(DB_PATH)
        
        # 加载日线数据
        self.data = pd.read_sql(f"""
            SELECT ts_code, trade_date, open, high, low, close, vol
            FROM daily
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            ORDER BY ts_code, trade_date
        """, conn)
        
        # 加载基本面
        self.funds = pd.read_sql("""
            SELECT ts_code FROM fundamentals
            WHERE pe > 0 AND pe < 25 AND roe > 10
        """, conn)['ts_code'].tolist()
        
        conn.close()
        
        # 预处理
        self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])
        self.trade_dates = sorted(self.data['trade_date'].unique())
        
        logger.info(f"加载完成: {len(self.data)}行, {len(self.trade_dates)}天, 耗时{time.time()-t0:.1f}秒")
    
    def calculate_indicators_vectorized(self, group):
        """向量化计算技术指标"""
        # 计算MA
        group = group.sort_values('trade_date')
        group['ma5'] = group['close'].rolling(5).mean()
        group['ma10'] = group['close'].rolling(10).mean()
        group['ma20'] = group['close'].rolling(20).mean()
        
        # 计算动量
        group['mom5'] = group['close'] / group['close'].shift(5) - 1
        group['mom20'] = group['close'] / group['close'].shift(20) - 1
        
        # 计算波动率
        group['volatility'] = group['close'].rolling(20).std() / group['close'].rolling(20).mean()
        
        return group
    
    def generate_signals(self):
        """向量化生成信号"""
        logger.info("计算技术指标...")
        t0 = time.time()
        
        # 按股票分组计算指标
        self.data = self.data.groupby('ts_code', group_keys=False).apply(self.calculate_indicators_vectorized)
        
        # 过滤基本面
        self.data = self.data[self.data['ts_code'].isin(self.funds)]
        
        logger.info(f"指标计算完成, 耗时{time.time()-t0:.1f}秒")
    
    def run_strategy(self, strategy='seasonal', **params):
        """
        运行策略
        
        参数:
        - strategy: 'seasonal' 季节性, 'ma' 均线, 'momentum' 动量
        - start_month: 季节性策略开始月份
        - end_month: 季节性策略结束月份
        - tp: 止盈比例
        - sl: 止损比例
        """
        logger.info(f"运行策略: {strategy}")
        t0 = time.time()
        
        # 默认参数
        start_month = params.get('start_month', 4)
        end_month = params.get('end_month', 10)
        tp = params.get('tp', 0.20)
        sl = params.get('sl', 0.08)
        
        # 初始化
        capital = 1000000
        positions = {}  # {code: {'qty':, 'cost':, 'entry_date':}}
        
        results = []
        
        for date in self.trade_dates:
            month = date.month
            
            # 判断是否在交易季节
            in_season = start_month <= month <= end_month
            
            # 获取当日信号
            day_data = self.data[self.data['trade_date'] == date].copy()
            
            # 卖出检测
            to_sell = []
            for code, pos in list(positions.items()):
                price_data = day_data[day_data['ts_code'] == code]
                if len(price_data) == 0:
                    continue
                
                price = price_data['close'].iloc[0]
                ret = (price - pos['cost']) / pos['cost']
                
                # 止盈/止损/季节结束
                if ret > tp or ret < -sl or (month > end_month and pos.get('season_end', False) == False):
                    # 标记季节结束需要卖出
                    if month > end_month and pos.get('season_end', False) == False:
                        pos['season_end'] = True
                    else:
                        to_sell.append(code)
            
            # 执行卖出
            for code in to_sell:
                price_data = day_data[day_data['ts_code'] == code]
                if len(price_data) > 0:
                    price = price_data['close'].iloc[0]
                    capital += price * positions[code]['qty'] * 0.998
                    del positions[code]
            
            # 买入检测
            if in_season and len(positions) < 3 and capital > 50000:
                # 季初买入
                if month == start_month or month == 7:
                    # 选股：MA5 > MA20 的股票
                    signals = day_data[
                        (day_data['ma5'] > day_data['ma20']) & 
                        (day_data['ma5'].notna()) &
                        (day_data['ma20'].notna())
                    ].head(3 - len(positions))
                    
                    for _, row in signals.iterrows():
                        if capital > 10000:
                            code = row['ts_code']
                            price = row['close']
                            qty = int(capital / 3 / price / 100) * 100
                            if qty > 0:
                                capital -= price * qty * 1.001
                                positions[code] = {
                                    'qty': qty, 
                                    'cost': price, 
                                    'season_end': False
                                }
            
            # 记录结果
            total_value = capital
            for code, pos in positions.items():
                price_data = day_data[day_data['ts_code'] == code]
                if len(price_data) > 0:
                    total_value += price_data['close'].iloc[0] * pos['qty']
            
            results.append({
                'date': date,
                'capital': capital,
                'positions': len(positions),
                'total_value': total_value
            })
        
        # 计算收益
        final_value = results[-1]['total_value'] if results else capital
        total_return = (final_value - 1000000) / 1000000 * 100
        annual_return = (1 + total_return/100) ** 0.25 - 1
        
        # 计算回撤
        df = pd.DataFrame(results)
        df['peak'] = df['total_value'].cummax()
        df['drawdown'] = (df['peak'] - df['total_value']) / df['peak']
        max_drawdown = df['drawdown'].max() * 100
        
        logger.info(f"策略完成, 耗时{time.time()-t0:.1f}秒")
        
        return {
            'initial_capital': 1000000,
            'final_value': final_value,
            'total_return': total_return,
            'annual_return': annual_return * 100,
            'max_drawdown': max_drawdown,
            'trade_count': len([r for r in results if r['positions'] > 0])
        }
    
    def run_ma_strategy(self, fast=5, slow=20, tp=0.20, sl=0.08):
        """均线策略"""
        logger.info(f"均线策略 MA({fast},{slow})")
        t0 = time.time()
        
        capital = 1000000
        positions = {}
        
        for date in self.trade_dates:
            day_data = self.data[self.data['trade_date'] == date].copy()
            
            # 卖出
            to_sell = []
            for code, pos in list(positions.items()):
                price_data = day_data[day_data['ts_code'] == code]
                if len(price_data) == 0:
                    continue
                
                price = price_data['close'].iloc[0]
                ret = (price - pos['cost']) / pos['cost']
                
                if ret > tp or ret < -sl:
                    to_sell.append(code)
            
            for code in to_sell:
                price_data = day_data[day_data['ts_code'] == code]
                if len(price_data) > 0:
                    capital += price_data['close'].iloc[0] * positions[code]['qty'] * 0.998
                    del positions[code]
            
            # 买入
            if len(positions) < 3 and capital > 50000:
                # 金叉信号
                signals = day_data[
                    (day_data['ma5'] > day_data['ma20']) & 
                    (day_data['ma5'].notna()) &
                    (day_data['ma20'].notna())
                ].head(5)
                
                for _, row in signals.iterrows():
                    if len(positions) >= 3:
                        break
                    code = row['ts_code']
                    if code in positions:
                        continue
                    price = row['close']
                    qty = int(capital / 3 / price / 100) * 100
                    if qty > 0:
                        capital -= price * qty * 1.001
                        positions[code] = {'qty': qty, 'cost': price}
        
        final_value = capital
        total_return = (final_value - 1000000) / 1000000 * 100
        
        logger.info(f"均线策略完成, 耗时{time.time()-t0:.1f}秒")
        
        return {
            'total_return': total_return,
            'final_value': final_value
        }


def main():
    """主函数"""
    engine = VectorizedBacktest()
    
    # 加载数据
    engine.load_data()
    
    # 计算指标
    engine.generate_signals()
    
    # 测试不同策略
    print("\n" + "="*60)
    print("向量化回测结果 (2020-2024)")
    print("="*60)
    
    # 季节性策略
    result = engine.run_strategy('seasonal', start_month=4, end_month=10, tp=0.20, sl=0.08)
    print(f"\n季节性策略 (4-10月):")
    print(f"  最终资金: {result['final_value']:,.0f}")
    print(f"  总收益: {result['total_return']:.2f}%")
    print(f"  年化收益: {result['annual_return']:.2f}%")
    print(f"  最大回撤: {result['max_drawdown']:.2f}%")
    
    # 均线策略
    result = engine.run_ma_strategy(fast=5, slow=20, tp=0.20, sl=0.08)
    print(f"\n均线策略 MA(5,20):")
    print(f"  最终资金: {result['final_value']:,.0f}")
    print(f"  总收益: {result['total_return']:.2f}%")


if __name__ == '__main__':
    main()
