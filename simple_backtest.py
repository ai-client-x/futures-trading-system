#!/usr/bin/env python3
"""
简化版回测 - 期货
Simple Backtest - China Futures
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
    print("📊 期货策略回测")
    print("="*60)
    
    # 初始化
    fetcher = DataFetcher()
    strategy = TrendFollowingStrategy()
    engine = FuturesEngine(initial_capital=500000)
    
    # 期货合约
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
    
    # 回测
    print("\n🔄 逐日回测...")
    min_len = min(len(d) for d in contract_data.values())
    
    signals_generated = 0
    
    for i in range(20, min_len):
        # 历史数据
        window_data = {}
        for code, data in contract_data.items():
            window_data[code] = data[:i]
        
        # 当日数据
        current_data = {}
        for code, data in contract_data.items():
            current_data[code] = data[i]
        
        # 生成信号
        signals = strategy.generate_signals(window_data)
        
        # 执行
        for sig in signals:
            code = sig['code']
            if code not in current_data:
                continue
            
            price = current_data[code]['close']
            date = str(current_data[code]['date'])
            
            # 期货每次1手
            if sig['action'] == 'buy' and sig['strength'] > 0.015:
                engine.open_position(code, "long", price, 1, date)
                signals_generated += 1
            elif sig['action'] == 'sell' and sig['strength'] > 0.015:
                engine.close_position(code, "long", price, 1, date)
                signals_generated += 1
        
        # 更新价格
        prices = {code: current_data[code]['close'] for code in contract_data}
        engine.update_prices(prices)
        
        # 止损检查
        stops = engine.check_stop_loss()
        for stop in stops:
            engine.close_position(stop['code'], stop['direction'], 
                               prices.get(stop['code'], 0), stop['quantity'], str(date))
        
        if i % 50 == 0:
            status = engine.get_status()
            print(f"  Day {i}: 资产 ¥{status['total_assets']:,.0f} ({status['profit_pct']:+.2f}%)")
    
    # 结果
    final = engine.get_status()
    print("\n" + "="*60)
    print("📊 期货回测结果")
    print("="*60)
    print(f"初始资金: ¥500,000")
    print(f"最终资产: ¥{final['total_assets']:,.0f}")
    print(f"总收益率: {final['profit_pct']:+.2f}%")
    print(f"交易次数: {signals_generated}")
    print("="*60)

if __name__ == "__main__":
    run_futures_backtest()
