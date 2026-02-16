#!/usr/bin/env python3
"""
期货量化交易系统 v2
Futures Quantitative Trading System
"""

import os
import sys

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import config
from src.models import Signal, FuturesTrade, FuturesPosition, FuturesPortfolio
from src.engines.futures_engine import FuturesEngine
from src.engines.backtest import FuturesBacktester
from src.risk.manager import FuturesRiskManager
from src.signals.generator import FuturesSignalGenerator

__all__ = [
    'config',
    'Signal', 'FuturesTrade', 'FuturesPosition', 'FuturesPortfolio',
    'FuturesEngine', 'FuturesBacktester',
    'FuturesRiskManager',
    'FuturesSignalGenerator'
]


def main():
    """主函数 - 演示"""
    from src.engines.futures_engine import FuturesEngine
    
    engine = FuturesEngine(initial_capital=500000)
    
    # 模拟开仓
    engine.open_position("rb", "long", 4000, 5, "2026-02-15")
    engine.open_position("i", "short", 800, 2, "2026-02-15")
    
    # 模拟更新价格
    mock_prices = {
        "rb": 4050,
        "i": 780,
        "j": 2100,
        "jm": 1450,
        "au": 450,
        "ag": 5500,
        "cu": 73500,
        "al": 19800
    }
    engine.update_prices(mock_prices)
    
    # 打印状态
    engine.print_status()
    
    # 检查止损
    from src.risk.manager import FuturesRiskManager
    risk_manager = FuturesRiskManager()
    risk_manager.peak_assets = engine.portfolio.total_assets
    stops = risk_manager.check_stop_loss(engine)
    if stops:
        print(f"⚠️ 触发止损/止盈: {stops}")
    
    print("✅ 期货交易系统 v2 初始化完成")


if __name__ == "__main__":
    main()
