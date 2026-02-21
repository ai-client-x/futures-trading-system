#!/usr/bin/env python3
"""
统一回测系统
使用实盘相同的策略和交易引擎
"""

import pandas as pd
import sys
sys.path.insert(0, '.')

from src.engines.backtest import Backtester
from src.strategies.hybrid_strategy import HybridStrategy
from src.config import config

def run_unified_backtest(
    start_date: str = '20160101',
    end_date: str = '20191231',
    initial_capital: float = 1000000,
    max_positions: int = 10
):
    """
    统一回测 - 使用实盘策略
    
    流程:
    1. 开盘前筛选候选股票 (基本面)
    2. 每日检查技术面信号
    3. 使用TradingEngine模拟交易
    """
    print("="*60)
    print("统一回测系统")
    print("="*60)
    print(f"时间: {start_date} ~ {end_date}")
    print(f"资金: {initial_capital}")
    
    # 初始化
    strategy = HybridStrategy()
    backtester = Backtester(initial_capital)
    
    # 获取所有可用股票 (和实盘一样)
    print("\n获取所有股票...")
    # 从数据库获取所有有数据的股票
    conn = strategy.get_connection()
    all_stocks = pd.read_sql("""
        SELECT DISTINCT ts_code FROM daily 
        WHERE trade_date >= ?
    """, conn, params=[start_date])
    conn.close()
    
    # 过滤成交额门槛
    conn = strategy.get_connection()
    avg_amount = pd.read_sql(f"""
        SELECT ts_code, AVG(amount) as avg_amt 
        FROM daily 
        WHERE trade_date >= ? 
        GROUP BY ts_code
        HAVING avg_amt >= 30000000
    """, conn, params=[start_date])
    conn.close()
    
    valid_stocks = avg_amount[avg_amount['avg_amt'] >= 30000000]['ts_code'].tolist()
    print(f"可用股票: {len(valid_stocks)}只 (日均成交额>=3000万)")
    
    # 转换为回测格式
    stocks = [{'code': s, 'name': s} for s in valid_stocks]
    
    # 运行回测
    print("\n开始回测...")
    result = backtester.run(
        stocks=stocks,
        start_date=start_date,
        end_date=end_date,
        strategy=strategy,
        verbose=True
    )
    
    return result.to_dict()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default='20160101')
    parser.add_argument('--end', default='20191231')
    parser.add_argument('--capital', type=float, default=1000000)
    args = parser.parse_args()
    
    result = run_unified_backtest(args.start, args.end, args.capital)
    
    print("\n" + "="*60)
    print("回测结果")
    print("="*60)
    for k, v in result.items():
        print(f"  {k}: {v}")
