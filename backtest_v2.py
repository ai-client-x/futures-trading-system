#!/usr/bin/env python3
"""
期货策略回测 - 修正版
遵循量化交易最佳实践：
1. 用次日开盘价成交（避免未来函数）
2. 预留滑点
3. 严格风控（止盈止损）
4. 参数简化
"""

import sys
import os
sys.path.append("/root/.openclaw/workspace/trading-system")

from data_fetcher import DataFetcher
from strategies import TrendFollowingStrategy
from china_futures_engine import FuturesEngine
from datetime import datetime, timedelta

# ============== 策略参数（可配置）==============
PARAMS = {
    "ma_period": 20,        # 均线周期
    "threshold": 0.02,     # 突破阈值 2%
    "max_positions": 2,     # 最多持仓数
    "take_profit": 0.05,   # 止盈 5%
    "stop_loss": 0.03,     # 止损 3%
    "position_size": 1,     # 每次1手
    "slippage": 0.005,     # 滑点 0.5%
}

# ============== 期货合约池 ==============
CONTRACTS = ["rb", "i", "j", "jm", "au"]

def run_backtest(initial_capital=100000):
    print("\n" + "="*60)
    print("📊 期货策略回测 (修正版)")
    print("="*60)
    
    # 参数
    ma_period = PARAMS["ma_period"]
    threshold = PARAMS["threshold"]
    max_positions = PARAMS["max_positions"]
    take_profit = PARAMS["take_profit"]
    stop_loss = PARAMS["stop_loss"]
    position_size = PARAMS["position_size"]
    slippage = PARAMS["slippage"]
    
    print(f"\n📈 合约池: {CONTRACTS}")
    print(f"💰 初始资金: ¥{initial_capital:,}")
    print(f"⚙️ 参数: MA{ma_period}, 阈值{threshold*100}%, 持仓{max_positions}, 止盈{take_profit*100}%, 止损{stop_loss*100}%, 滑点{slippage*100}%")
    
    # 初始化
    fetcher = DataFetcher()
    strategy = TrendFollowingStrategy()
    engine = FuturesEngine(initial_capital=initial_capital)
    
    # 获取数据
    print("\n📥 获取数据...")
    contract_data = {}
    for code in CONTRACTS:
        data = fetcher.get_futures_daily(code, 250)
        if data:
            contract_data[code] = data
            print(f"  {code}: {len(data)} 天")
    
    if not contract_data:
        print("❌ 无法获取数据")
        return
    
    # 回测
    print("\n🔄 回测...")
    min_len = min(len(d) for d in contract_data.values())
    trading_days = min_len - ma_period - 1
    years = trading_days / 252
    
    daily_assets = [initial_capital]
    peak = initial_capital
    
    for i in range(ma_period, min_len - 1):
        # 历史数据
        window_data = {code: data[:i] for code, data in contract_data.items()}
        
        # 次日开盘价（用于成交）
        next_day_data = {code: data[i] for code, data in contract_data.items()}
        
        # 生成信号
        signals = strategy.generate_signals(window_data)
        
        # 过滤强信号
        strong_signals = [s for s in signals if s['strength'] > threshold]
        strong_signals.sort(key=lambda x: x['strength'], reverse=True)
        strong_signals = strong_signals[:max_positions]
        
        # 检查止盈止损
        for pos_key, pos in list(engine.portfolio.positions.items()):
            if pos.current_price > 0:
                if pos.direction == "long":
                    profit_pct = (pos.current_price - pos.avg_price) / pos.avg_price
                    if profit_pct >= take_profit or profit_pct <= -stop_loss:
                        close_price = pos.current_price * (1 - slippage)
                        engine.close_position(pos.code, pos.direction, 
                                          close_price, pos.quantity, 
                                          str(next_day_data[pos.code]['date']))
        
        # 执行新信号
        current_positions = len(engine.portfolio.positions)
        for sig in strong_signals:
            if current_positions >= max_positions:
                break
            
            code = sig['code']
            if code not in next_day_data:
                continue
            
            # 用次日开盘价 + 滑点
            open_price = next_day_data[code]['open']
            buy_price = open_price * (1 + slippage)
            date = str(next_day_data[code]['date'])
            
            # 检查是否已持有
            for pos in engine.portfolio.positions.values():
                if pos.code == code:
                    break
            else:
                if sig['action'] == 'buy':
                    engine.open_position(code, "long", buy_price, position_size, date)
                    current_positions += 1
        
        # 更新价格（用收盘价更新市值）
        current_data = {code: contract_data[code][i] for code in contract_data}
        prices = {code: current_data[code]['close'] for code in contract_data}
        engine.update_prices(prices)
        
        # 记录
        total = engine.portfolio.total_assets
        daily_assets.append(total)
        if total > peak:
            peak = total
        
        if i % 50 == 0:
            print(f"  Day {i}: 资产 ¥{total:,.0f}")
    
    # 计算指标
    final = engine.get_status()
    final_assets = final['total_assets']
    
    total_return = (final_assets / initial_capital - 1) * 100
    annual_return = ((final_assets / initial_capital) ** (1/years) - 1) * 100 if years > 0 else 0
    
    # 最大回撤
    peak = initial_capital
    max_drawdown = 0
    for assets in daily_assets:
        if assets > peak:
            peak = assets
        drawdown = (peak - assets) / peak * 100
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    
    # 打印结果
    print("\n" + "="*60)
    print("📊 回测结果")
    print("="*60)
    print(f"初始资金: ¥{initial_capital:,}")
    print(f"最终资产: ¥{final_assets:,.0f}")
    print(f"总收益率: {total_return:+.2f}%")
    print(f"年化收益率: {annual_return:+.2f}%")
    print(f"最大回撤: {max_drawdown:.2f}%")
    print(f"交易次数: {len(engine.portfolio.trades)}")
    print("="*60)
    
    return {
        "initial": initial_capital,
        "final": final_assets,
        "annual": annual_return,
        "max_dd": max_drawdown,
        "trades": len(engine.portfolio.trades)
    }

if __name__ == "__main__":
    # 测试不同资金规模
    for capital in [10000, 50000, 100000, 1000000]:
        run_backtest(capital)
        print()
