#!/usr/bin/env python3
"""
股票量化交易系统 v2
Quantitative Trading System
"""

import os
import sys

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import config
from src.models import Signal, Trade, Position, Portfolio
from src.engines.trading_engine import TradingEngine
from src.engines.backtest import Backtester
from src.risk.manager import RiskManager
from src.signals.generator import CompositeSignalGenerator
from src.strategies.base import TrendStrategy, MeanReversionStrategy, BreakoutStrategy

__all__ = [
    'config',
    'Signal', 'Trade', 'Position', 'Portfolio',
    'TradingEngine', 'Backtester',
    'RiskManager',
    'CompositeSignalGenerator',
    'TrendStrategy', 'MeanReversionStrategy', 'BreakoutStrategy'
]
