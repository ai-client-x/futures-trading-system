#!/usr/bin/env python3
"""
实盘交易系统
每天开盘前运行
"""

import sys
sys.path.insert(0, '.')

from src.strategies.hybrid_strategy import HybridStrategy
from src.engines.trading_engine import TradingEngine
from src.config import config
import sqlite3
import pandas as pd
from datetime import datetime

def run_daily_trading():
    """
    每日实盘交易流程
    
    1. 开盘前获取所有可用股票
    2. 基本面筛选
    3. 技术面评分
    4. 选股买入
    """
    print("="*60)
    print("每日实盘交易")
    print("="*60)
    
    # 1. 获取所有可用股票 (日均成交额 >= 3000万)
    print("\n[1] 获取可用股票...")
    conn = sqlite3.connect('data/stocks.db')
    
    # 计算最近60天日均成交额
    avg_amount = pd.read_sql("""
        SELECT ts_code, AVG(amount) as avg_amt 
        FROM daily 
        WHERE trade_date >= date('now', '-60 days')
        GROUP BY ts_code
        HAVING avg_amt >= 30000000
    """, conn)
    conn.close()
    
    valid_stocks = avg_amount[avg_amount['avg_amt'] >= 30000000]['ts_code'].tolist()
    print(f"    可用股票: {len(valid_stocks)}只")
    
    # 2. 基本面筛选
    print("\n[2] 基本面筛选...")
    strategy = HybridStrategy()
    candidates = strategy.screen_candidates(
        max_pe=25,
        min_roe=10,
        min_dv_ratio=1,
        max_debt=70,
        min_market_cap=30,
        limit=100
    )
    print(f"    候选股票: {len(candidates)}只")
    
    # 3. 技术面评分
    print("\n[3] 技术面评分...")
    signals = strategy.generate_signals(candidates)
    print(f"    有效信号: {len(signals)}只")
    
    # 4. 选股买入
    print("\n[4] 选股买入...")
    engine = TradingEngine()
    
    # 按分数排序
    signals.sort(key=lambda x: x.strength if hasattr(x, 'strength') else 50, reverse=True)
    
    # 买入Top10
    for i, signal in enumerate(signals[:10]):
        if engine.portfolio.cash < 10000:
            print("    资金不足，停止买入")
            break
        
        code = signal.code
        price = signal.price  # 需要实时获取
        
        # 这里需要调用实时行情接口获取价格
        # 简化处理...
        print(f"    买入 {code}")
    
    print("\n完成!")
    
    return {
        'candidates': len(candidates),
        'signals': len(signals)
    }


if __name__ == "__main__":
    run_daily_trading()
