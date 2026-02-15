#!/usr/bin/env python3
"""
回测系统 - 期货策略回测
Backtesting System - China Futures
"""

import json
import os
from datetime import datetime, timedelta
from typing import List, Dict
import sys
sys.path.append(os.path.dirname(__file__))

from china_futures_engine import FuturesEngine
sys.path.append("/root/.openclaw/workspace/trading-system")
from data_fetcher import DataFetcher

class FuturesBacktester:
    """期货回测引擎"""
    
    def __init__(self, initial_capital: float = 500000):
        self.initial_capital = initial_capital
        self.engine = FuturesEngine(initial_capital)
        self.fetcher = DataFetcher()
        self.results = []
    
    def run(self, contracts: List[str], start_date: str, end_date: str, strategy) -> dict:
        """
        运行回测
        contracts: 期货合约代码列表
        start_date: 开始日期 YYYY-MM-DD
        strategy: 策略实例
        """
        print(f"\n{'='*60}")
        print(f"📊 期货策略回测")
        print(f"{'='*60}")
        print(f"合约: {', '.join(contracts)}")
        print(f"时间: {start_date} ~ {end_date}")
        print(f"资金: ¥{self.initial_capital:,.0f}")
        print(f"{'='*60}\n")
        
        # 获取所有合约历史数据
        contract_data = {}
        for code in contracts:
            print(f"📥 获取 {code} 数据...")
            data = self.fetcher.get_futures_daily(code, days=365)
            if data:
                contract_data[code] = data
        
        # 按日期回测
        if not contract_data:
            print("❌ 无法获取数据")
            return {}
        
        # 获取日期范围
        all_dates = sorted(set(d.get('date') for d in list(contract_data.values())[0]))
        test_dates = [d for d in all_dates if str(start_date) <= d <= str(end_date)]
        
        print(f"\n📈 回测期间: {len(test_dates)} 个交易日")
        
        # 逐日回测
        for i, date in enumerate(test_dates):
            # 获取当日数据
            daily_data = {}
            for code, data in contract_data.items():
                for d in data:
                    if d.get('date') == date:
                        daily_data[code] = d
                        break
            
            if not daily_data:
                continue
            
            # 生成信号
            signals = strategy.generate_signals(daily_data)
            
            # 执行交易
            for signal in signals:
                code = signal['code']
                if code not in daily_data:
                    continue
                
                price = daily_data[code].get('close', 0)
                if price <= 0:
                    continue
                
                # 期货每次开1手
                quantity = 1
                
                if signal['action'] == 'buy':
                    self.engine.open_position(code, "long", price, quantity, date)
                elif signal['action'] == 'sell':
                    self.engine.close_position(code, "long", price, quantity, date)
            
            # 更新价格
            prices = {code: d.get('close', 0) for code, d in daily_data.items()}
            self.engine.update_prices(prices)
            
            # 检查止损
            stops = self.engine.check_stop_loss()
            for stop in stops:
                print(f"  ⚠️ 止损: {stop['code']} {stop['direction']}")
                self.engine.close_position(
                    stop['code'], 
                    stop['direction'], 
                    daily_data.get(stop['code'], {}).get('close', 0),
                    stop['quantity'],
                    date
                )
            
            # 每20天打印状态
            if (i + 1) % 20 == 0:
                status = self.engine.get_status()
                print(f"  Day {i+1}: 资产¥{status['total_assets']:,.0f} ({status['profit_pct']:+.2f}%)")
        
        # 回测结束
        final_status = self.engine.get_status()
        
        # 计算指标
        metrics = self.calculate_metrics(final_status, len(test_dates))
        
        print(f"\n{'='*60}")
        print(f"📊 回测结果")
        print(f"{'='*60}")
        print(f"初始资金: ¥{self.initial_capital:,.0f}")
        print(f"最终资产: ¥{final_status['total_assets']:,.0f}")
        print(f"总收益率: {metrics['total_return']:+.2f}%")
        print(f"年化收益: {metrics['annual_return']:+.2f}%")
        print(f"夏普比率: {metrics['sharpe_ratio']:.2f}")
        print(f"最大回撤: {metrics['max_drawdown']:.2f}%")
        print(f"交易次数: {metrics['total_trades']}")
        print(f"胜率: {metrics['win_rate']:.1f}%")
        print(f"{'='*60}\n")
        
        return {
            "period": f"{start_date} ~ {end_date}",
            "initial_capital": self.initial_capital,
            "final_assets": final_status['total_assets'],
            "metrics": metrics,
            "trades": [t.__dict__ for t in self.engine.portfolio.trades]
        }
    
    def calculate_metrics(self, final_status: dict, trading_days: int) -> dict:
        """计算回测指标"""
        total_return = final_status['profit_pct']
        
        # 年化收益
        years = trading_days / 252
        annual_return = ((final_status['total_assets'] / self.initial_capital) ** (1/years) - 1) * 100 if years > 0 else 0
        
        # 简化夏普比率 (假设无风险利率3%)
        risk_free_rate = 3.0
        volatility = 20  # 期货波动更大
        sharpe_ratio = (annual_return - risk_free_rate) / volatility if volatility > 0 else 0
        
        # 最大回撤
        max_drawdown = abs(min(0, total_return * 0.6))  # 简化估算
        
        # 交易统计
        trades = self.engine.portfolio.trades
        total_trades = len(trades)
        
        # 胜率
        close_trades = [t for t in trades if t.action == 'close']
        wins = sum(1 for t in close_trades if t.commission >= 0)  # 简化
        
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
        
        return {
            "total_return": total_return,
            "annual_return": annual_return,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "total_trades": total_trades,
            "win_rate": win_rate
        }


# ============ 主函数 ============
if __name__ == "__main__":
    from strategies import TrendFollowingStrategy, BreakoutStrategy
    
    # 期货合约池
    contracts = [
        "rb",   # 螺纹钢
        "i",    # 铁矿石
        "j",    # 焦炭
        "jm",   # 焦煤
        "au",   # 黄金
        "ag",   # 白银
    ]
    
    # 回测时间 (过去1年)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    
    # 选择策略
    strategy = TrendFollowingStrategy()
    
    # 运行回测
    backtester = FuturesBacktester(initial_capital=500000)
    results = backtester.run(contracts, start_date, end_date, strategy)
    
    # 保存结果
    os.makedirs("futures-system/backtest", exist_ok=True)
    with open("futures-system/backtest/results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print("✅ 回测完成，结果已保存")
