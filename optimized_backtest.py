#!/usr/bin/env python3
"""
优化版期货回测 - 严格风控
"""

import sys
import os
sys.path.append("/root/.openclaw/workspace/trading-system")

from data_fetcher import DataFetcher
from strategies import TrendFollowingStrategy
from china_futures_engine import FuturesEngine
from datetime import datetime, timedelta

def run_futures_backtest():
    print("\n" + "="*60)
    print("📊 期货策略回测 (优化版)")
    print("="*60)
    
    fetcher = DataFetcher()
    strategy = TrendFollowingStrategy()
    engine = FuturesEngine(initial_capital=500000)
    
    contracts = ["rb", "i", "j", "jm", "au"]
    
    # 获取数据
    print("\n📥 获取数据...")
    contract_data = {}
    for code in contracts:
        data = fetcher.get_futures_daily(code, 250)
        if data:
            contract_data[code] = data
            print(f"  {code}: {len(data)} 天")
    
    if not contract_data:
        print("❌ 无法获取数据")
        return
    
    # 回测参数 (优化)
    MIN_SIGNAL_STRENGTH = 0.02      # 提高入场门槛到3%
    MAX_POSITIONS = 3                 # 最多同时持有2个合约
    TAKE_PROFIT_PCT = 0.05           # 止盈3%
    STOP_LOSS_PCT = 0.03             # 止损2%
    POSITION_SIZE = 1                 # 每次1手
    
    print(f"\n🔧 参数: 入场>{MIN_SIGNAL_STRENGTH*100}%, 止盈{TAKE_PROFIT_PCT*100}%, 止损{STOP_LOSS_PCT*100}%")
    print("\n🔄 回测...")
    
    min_len = min(len(d) for d in contract_data.values())
    trading_days = min_len - 20
    years = trading_days / 252
    
    daily_assets = [engine.portfolio.total_assets]
    peak = engine.portfolio.total_assets
    
    for i in range(20, min_len):
        window_data = {code: data[:i] for code, data in contract_data.items()}
        current_data = {code: data[i] for code, data in contract_data.items()}
        
        signals = strategy.generate_signals(window_data)
        
        # 过滤强信号
        strong_signals = [s for s in signals if s['strength'] > MIN_SIGNAL_STRENGTH]
        strong_signals.sort(key=lambda x: x['strength'], reverse=True)
        strong_signals = strong_signals[:MAX_POSITIONS]  # 只选最强的
        
        # 检查现有持仓是否触发止盈/止损
        for pos_key, pos in list(engine.portfolio.positions.items()):
            if pos.current_price > 0:
                if pos.direction == "long":
                    profit_pct = (pos.current_price - pos.avg_price) / pos.avg_price
                    if profit_pct >= TAKE_PROFIT_PCT or profit_pct <= -STOP_LOSS_PCT:
                        print(f"  🎯 平仓 {pos.code}: 收益率 {profit_pct*100:.2f}%")
                        engine.close_position(pos.code, pos.direction, 
                                          pos.current_price, pos.quantity, 
                                          str(current_data[pos.code]['date']))
        
        # 执行新信号
        current_positions = len(engine.portfolio.positions)
        for sig in strong_signals:
            if current_positions >= MAX_POSITIONS:
                break
            
            code = sig['code']
            if code not in current_data:
                continue
            
            price = current_data[code]['close']
            date = str(current_data[code]['date'])
            
            # 检查是否已持有
            for pos in engine.portfolio.positions.values():
                if pos.code == code:
                    break
            else:  # 没持仓
                if sig['action'] == 'buy':
                    engine.open_position(code, "long", price, POSITION_SIZE, date)
                    current_positions += 1
                    print(f"  📈 开仓 {code} @ {price:.0f}")
        
        # 更新价格
        prices = {code: current_data[code]['close'] for code in contract_data}
        engine.update_prices(prices)
        
        # 记录
        total = engine.portfolio.total_assets
        daily_assets.append(total)
        if total > peak:
            peak = total
        
        if i % 50 == 0:
            print(f"  Day {i}: 资产 ¥{total:,.0f}")
    
    # 结果
    final = engine.get_status()
    final_assets = final['total_assets']
    initial = 500000
    
    # 年化收益率
    total_return = (final_assets / initial - 1) * 100
    annual_return = ((final_assets / initial) ** (1/years) - 1) * 100 if years > 0 else 0
    
    # 最大回撤
    peak = initial
    max_drawdown = 0
    for assets in daily_assets:
        if assets > peak:
            peak = assets
        drawdown = (peak - assets) / peak * 100
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    
    print("\n" + "="*60)
    print("📊 回测结果 (优化版)")
    print("="*60)
    print(f"回测天数: {trading_days} 天 ({years:.2f}年)")
    print(f"初始资金: ¥{initial:,}")
    print(f"最终资产: ¥{final_assets:,.0f}")
    print(f"总收益率: {total_return:+.2f}%")
    print(f"年化收益率: {annual_return:+.2f}%")
    print(f"最大回撤: {max_drawdown:.2f}%")
    print(f"交易次数: {len(engine.portfolio.trades)}")
    print("="*60)

if __name__ == "__main__":
    run_futures_backtest()
