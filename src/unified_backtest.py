#!/usr/bin/env python3
"""
统一回测系统
使用实盘相同的策略和交易引擎
"""

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
    
    # 获取候选股票
    print("\n获取候选股票...")
    candidates = strategy.screen_candidates(
        max_pe=25,
        min_roe=10,
        min_dv_ratio=1,
        max_debt=70,
        min_market_cap=30,
        limit=100
    )
    print(f"候选股票: {len(candidates)}只")
    
    # 转换为回测格式
    stocks = [{'code': c['ts_code'], 'name': c.get('name', c['ts_code'])} for c in candidates]
    
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
