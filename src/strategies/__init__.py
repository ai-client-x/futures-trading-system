# -*- coding: utf-8 -*-
"""
策略模块
Strategies Module
"""

from .base import Strategy as BaseStrategy
from .williams_strategy import WilliamsStrategy, create_williams_strategy
from .momentum_reversal_strategy import MomentumReversalStrategy, create_momentum_reversal_strategy
from .bollinger_strategy import BollingerStrategy, create_bollinger_strategy
from .adaptive_strategy import AdaptiveStrategy, create_adaptive_strategy, run_adaptive_strategy
from .hybrid_strategy import HybridStrategy

__all__ = [
    'BaseStrategy',
    'WilliamsStrategy',
    'create_williams_strategy',
    'MomentumReversalStrategy',
    'create_momentum_reversal_strategy',
    'BollingerStrategy',
    'create_bollinger_strategy',
    'AdaptiveStrategy',
    'create_adaptive_strategy',
    'run_adaptive_strategy',
    'HybridStrategy',
]
