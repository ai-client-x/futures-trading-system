#!/usr/bin/env python3
"""
实盘交易系统
每天开盘前运行

按用户规范：
1. 开盘前：获取活跃股票→基本面筛选→市场环境检测→选择1个最优策略→信号评分→买入
2. 开盘时：tick数据检查卖出/加仓（T+1限制）
"""

import sys
sys.path.insert(0, '.')

from src.strategies.hybrid_strategy import HybridStrategy
from src.engines.trading_engine import TradingEngine
import sqlite3
import pandas as pd
from datetime import datetime


# 26个策略，按市场环境分组
STRATEGIES = {
    '牛市': ['成交量突破', 'MACD+成交量', 'MACD策略', '突破前高', '均线发散', 
            '量价齐升', 'RSI趋势', '趋势过滤', '均线策略', '均线交叉强度',
            '收盘站均线', '成交量+均线', '突破确认', '平台突破'],
    '熊市': ['动量反转', '威廉指标', 'RSI逆势', '双底形态', '缩量回调', 'MACD背离'],
    '震荡市': ['布林带', 'RSI+均线', '布林带+RSI', '支撑阻力', '波动率突破', '均线收复']
}


def get_active_stocks():
    """获取活跃股票：60天日均成交额>=3000万"""
    conn = sqlite3.connect('data/stocks.db')
    df = pd.read_sql("""
        SELECT ts_code FROM daily
        WHERE trade_date >= date('now', '-60 days')
        GROUP BY ts_code
        HAVING AVG(amount) >= 30000000
    """, conn)
    conn.close()
    return df['ts_code'].tolist()


def filter_by_fundamentals(stocks):
    """基本面筛选：PE<25, ROE>10%, 股息率>1%, 负债<70%, 市值>30亿"""
    conn = sqlite3.connect('data/stocks.db')
    placeholders = ','.join([f"'{s}'" for s in stocks])
    df = pd.read_sql(f"""
        SELECT ts_code FROM fundamentals
        WHERE ts_code IN ({placeholders})
        AND pe < 25 AND roe > 10 AND dv_ratio > 1 
        AND debt_to_asset < 70 AND market_cap > 30
    """, conn)
    conn.close()
    return df['ts_code'].tolist()


def detect_market_regime(strategy):
    """检测市场环境"""
    market_data = strategy._get_market_index()
    regime = strategy.detect_market_regime(market_data)
    return regime


def select_best_strategy(regime):
    """选择1个最优策略"""
    strategies = STRATEGIES.get(regime, STRATEGIES['震荡市'])
    return strategies[0]  # 默认选择第一个


def check_positions_for_regime_change(positions, strategy_obj, market_data):
    """检查持仓是否需要因市场环境变化而卖出"""
    current_regime = strategy_obj.detect_market_regime(market_data)
    current_strategies = STRATEGIES.get(current_regime, STRATEGIES['震荡市'])
    
    to_sell = []
    for pos in positions:
        if pos.get('strategy', '') not in current_strategies:
            to_sell.append(pos['ts_code'])
    return to_sell


def generate_signals(candidates, strategy_obj, selected_strategy):
    """生成信号"""
    signals = strategy_obj.generate_signals(candidates, selected_strategy)
    return signals


def run_pre_market():
    """
    开盘前流程（每天开盘前运行）
    
    1. 获取活跃股票列表（60天日均成交额>=3000万）
    2. 基本面筛选
    3. 识别市场环境
    4. 检查持仓 - 环境变化时卖出
    5. 未满仓时：选择1个最优策略 → 信号评分 → 买入
    """
    print("="*60)
    print("开盘前准备")
    print("="*60)
    
    # 1. 获取活跃股票
    print("\n[1] 获取活跃股票...")
    active_stocks = get_active_stocks()
    print(f"    活跃股票: {len(active_stocks)}只")
    
    # 2. 基本面筛选
    print("\n[2] 基本面筛选...")
    candidates = filter_by_fundamentals(active_stocks)
    print(f"    候选股票: {len(candidates)}只")
    
    # 3. 识别市场环境
    print("\n[3] 识别市场环境...")
    strategy = HybridStrategy()
    regime = detect_market_regime(strategy)
    print(f"    市场环境: {regime}")
    
    # 4. 检查持仓
    print("\n[4] 检查持仓...")
    engine = TradingEngine()
    positions = list(engine.portfolio.positions.values()) if hasattr(engine, 'portfolio') else []
    
    if positions:
        # 检查是否需要因环境变化而卖出
        market_data = strategy._get_market_index()
        to_sell = check_positions_for_regime_change(positions, strategy, market_data)
        for code in to_sell:
            print(f"    卖出 {code}: 市场环境变化")
    
    # 计算持仓比例
    position_value = sum(p.market_value for p in positions) if positions else 0
    total_assets = engine.portfolio.cash + position_value
    position_ratio = position_value / total_assets if total_assets > 0 else 0
    
    print(f"    持仓比例: {position_ratio*100:.1f}%")
    
    # 5. 未满仓时买入
    if position_ratio < 0.9 and engine.portfolio.cash > 10000:
        print("\n[5] 选股买入...")
        
        # 选择1个最优策略
        selected_strategy = select_best_strategy(regime)
        print(f"    选用策略: {selected_strategy}")
        
        # 生成信号
        signals = generate_signals(candidates, strategy, selected_strategy)
        print(f"    信号股票: {len(signals)}只")
        
        # 买入：每有10%未持仓就买1只
        target_positions = int(total_assets * 0.1 / (1000000 * 0.1))
        current_positions = len(positions)
        to_buy = target_positions - current_positions
        
        for i, signal in enumerate(signals[:to_buy]):
            if engine.portfolio.cash < 100000:  # 资金不足
                break
            code = signal.code
            print(f"    买入 {code}")
    else:
        print("\n    满仓，不进行买入")
    
    print("\n完成!")
    return {'candidates': len(candidates), 'regime': regime}


def run_during_market():
    """
    开盘时流程（实时运行，检查tick数据）
    
    1. 卖出检查：止盈/止损/策略信号（T+1限制）
    2. 加仓检查：满足加仓条件时买入
    """
    print("="*60)
    print("盘中监控")
    print("="*60)
    
    # TODO: 实时tick数据处理
    # - 检查卖出信号
    # - 检查加仓信号
    # - T+1限制
    
    print("盘中监控功能待实现")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['pre', 'live'], default='pre',
                        help='pre: 开盘前, live: 盘中')
    args = parser.parse_args()
    
    if args.mode == 'pre':
        run_pre_market()
    else:
        run_during_market()
