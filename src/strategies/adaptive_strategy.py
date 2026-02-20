#!/usr/bin/env python3
"""
自适应市场策略
Adaptive Market Strategy

根据市场状态（牛市/熊市/震荡市）自动切换最适合的策略：
- 牛市：威廉指标策略
- 熊市：动量反转策略  
- 震荡市：布林带策略
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd

from ..models import Signal
from ..market_regime import MarketRegimeDetector
from .base import Strategy
from .williams_strategy import WilliamsStrategy, create_williams_strategy
from .momentum_reversal_strategy import MomentumReversalStrategy, create_momentum_reversal_strategy
from .bollinger_strategy import BollingerStrategy, create_bollinger_strategy

logger = logging.getLogger(__name__)


class AdaptiveStrategy(Strategy):
    """
    自适应市场策略
    
    根据市场状态自动切换策略：
    - 牛市 (bull): 威廉指标策略
    - 熊市 (bear): 动量反转策略
    - 震荡市 (consolidation): 布林带策略
    """
    
    def __init__(self, 
                 index_code: str = "000300",
                 use_full_capital: bool = True,
                 **strategy_params):
        """
        初始化自适应策略
        
        Args:
            index_code: 用于判断市场状态的指数代码 (默认沪深300)
            use_full_capital: 是否全仓投入 (默认True)
            **strategy_params: 各子策略的参数
        """
        super().__init__("Adaptive")
        
        self.index_code = index_code
        self.use_full_capital = use_full_capital
        self.regime_detector = MarketRegimeDetector()
        
        # 初始化各策略（使用优化后的参数）
        # 威廉指标策略参数（来自历史测试最佳参数）
        williams_params = strategy_params.get('williams', {})
        self.williams_strategy = create_williams_strategy(
            period=williams_params.get('period', 14),
            oversold=williams_params.get('oversold', -90),
            overbought=williams_params.get('overbought', -10),
            tp=williams_params.get('tp', 0.28),
            sl=williams_params.get('sl', 0.15)
        )
        
        # 动量反转策略参数
        momentum_params = strategy_params.get('momentum', {})
        self.momentum_strategy = create_momentum_reversal_strategy(
            rsi_period=momentum_params.get('rsi_period', 14),
            rsi_oversold=momentum_params.get('rsi_oversold', 25),
            lookback=momentum_params.get('lookback', 20),
            volume_ma_period=momentum_params.get('volume_ma_period', 20),
            tp=momentum_params.get('tp', 0.15),
            sl=momentum_params.get('sl', 0.08)
        )
        
        # 布林带策略参数
        bollinger_params = strategy_params.get('bollinger', {})
        self.bollinger_strategy = create_bollinger_strategy(
            period=bollinger_params.get('period', 20),
            std_dev=bollinger_params.get('std_dev', 2.0),
            tp=bollinger_params.get('tp', 0.10),
            sl=bollinger_params.get('sl', 0.05)
        )
        
        # 当前市场状态
        self.current_regime = None
        self.current_strategy = None
        self.regime_confidence = 0
        
        logger.info("自适应策略初始化完成")
    
    def detect_market_regime(self, index_data: pd.DataFrame) -> Dict:
        """
        检测市场状态
        
        Args:
            index_data: 指数数据
        
        Returns:
            市场状态信息
        """
        result = self.regime_detector.detect(index_data)
        self.current_regime = result['regime']
        self.regime_confidence = result['confidence']
        
        # 根据市场状态选择策略
        strategy_map = {
            'bull': ('williams', self.williams_strategy),
            'bear': ('momentum', self.momentum_strategy),
            'consolidation': ('bollinger', self.bollinger_strategy)
        }
        
        strategy_name, strategy = strategy_map.get(self.current_regime, ('bollinger', self.bollinger_strategy))
        self.current_strategy = strategy
        
        regime_name = self.regime_detector.get_regime_name(self.current_regime)
        
        logger.info(f"市场状态: {regime_name} (置信度: {self.regime_confidence:.1f}%)")
        logger.info(f"当前策略: {strategy_name}")
        
        return {
            'regime': self.current_regime,
            'regime_name': regime_name,
            'confidence': self.regime_confidence,
            'strategy_name': strategy_name,
            'strategy': strategy,
            'reason': result.get('reason', '')
        }
    
    def generate_signals(self, df: pd.DataFrame, code: str, name: str = "",
                        index_data: pd.DataFrame = None) -> Optional[Signal]:
        """
        生成交易信号（实现基类抽象方法）
        
        Args:
            df: 股票数据
            code: 股票代码
            name: 股票名称
            index_data: 指数数据（用于判断市场状态）
        
        Returns:
            Signal 或 None
        """
        return self.generate(df, code, name, index_data)
    
    def generate(self, df: pd.DataFrame, code: str, name: str = "", 
                index_data: pd.DataFrame = None) -> Optional[Signal]:
        """
        生成交易信号
        
        Args:
            df: 股票数据
            code: 股票代码
            name: 股票名称
            index_data: 指数数据（用于判断市场状态）
        
        Returns:
            Signal 或 None
        """
        # 如果没有提供指数数据，使用默认逻辑
        if index_data is not None and (self.current_regime is None or self.current_strategy is None):
            self.detect_market_regime(index_data)
        
        # 如果仍未确定策略，默认使用布林带策略
        if self.current_strategy is None:
            self.current_strategy = self.bollinger_strategy
            self.current_regime = 'consolidation'
        
        # 调用当前策略生成信号
        signal = self.current_strategy.generate(df, code, name)
        
        # 在信号中添加市场状态信息
        if signal:
            signal.indicators['market_regime'] = self.current_regime
            signal.indicators['regime_confidence'] = self.regime_confidence
            signal.indicators['active_strategy'] = self.current_strategy.name
        
        return signal
    
    def get_current_state(self) -> Dict:
        """获取当前市场状态和策略信息"""
        return {
            'regime': self.current_regime,
            'regime_name': self.regime_detector.get_regime_name(self.current_regime) if self.current_regime else '未知',
            'confidence': self.regime_confidence,
            'active_strategy': self.current_strategy.name if self.current_strategy else '未选择',
            'use_full_capital': self.use_full_capital
        }
    
    def get_strategy_params(self) -> Dict:
        """获取各策略的参数"""
        return {
            'williams': self.williams_strategy.get_params(),
            'momentum': self.momentum_strategy.get_params(),
            'bollinger': self.bollinger_strategy.get_params()
        }
    
    def set_strategy_params(self, strategy_type: str, **params):
        """
        设置特定策略的参数
        
        Args:
            strategy_type: 'williams' | 'momentum' | 'bollinger'
            **params: 要修改的参数
        """
        strategy_map = {
            'williams': self.williams_strategy,
            'momentum': self.momentum_strategy,
            'bollinger': self.bollinger_strategy
        }
        
        strategy = strategy_map.get(strategy_type)
        if strategy:
            strategy.set_params(**params)
            logger.info(f"更新策略 {strategy_type} 参数: {params}")


# 策略工厂函数
def create_adaptive_strategy(**params) -> AdaptiveStrategy:
    """创建自适应策略"""
    return AdaptiveStrategy(**params)


# ============== 便捷函数 ==============

def run_adaptive_strategy(stock_df: pd.DataFrame, 
                         index_df: pd.DataFrame,
                         code: str, 
                         name: str = "") -> Optional[Signal]:
    """
    便捷函数：运行自适应策略
    
    Args:
        stock_df: 股票数据
        index_df: 指数数据
        code: 股票代码
        name: 股票名称
    
    Returns:
        Signal 或 None
    """
    strategy = AdaptiveStrategy()
    strategy.detect_market_regime(index_df)
    return strategy.generate(stock_df, code, name, index_df)


if __name__ == "__main__":
    # 测试
    import random
    import math
    
    logging.basicConfig(level=logging.INFO)
    
    print("=" * 60)
    print("自适应市场策略测试")
    print("=" * 60)
    
    # 测试1：模拟牛市数据
    print("\n=== 测试1: 牛市环境 ===")
    bull_index = pd.DataFrame({
        'Close': [100 + i * 0.8 for i in range(150)],
        'High': [100 + i * 0.8 + 5 for i in range(150)],
        'Low': [100 + i * 0.8 - 5 for i in range(150)],
        'Open': [100 + i * 0.8 for i in range(150)],
        'Volume': [1000000] * 150
    })
    
    stock = pd.DataFrame({
        'Close': [50 + i * 0.3 for i in range(50)],
        'High': [50 + i * 0.3 + 2 for i in range(50)],
        'Low': [50 + i * 0.3 - 2 for i in range(50)],
        'Open': [50 + i * 0.3 for i in range(50)],
        'Volume': [1000000] * 50
    })
    
    strategy = AdaptiveStrategy()
    result = strategy.detect_market_regime(bull_index)
    print(f"市场状态: {result['regime_name']}")
    print(f"使用策略: {result['strategy_name']}")
    
    signal = strategy.generate(stock, "000001", "测试股票", bull_index)
    if signal:
        print(f"信号: {signal.action}, 价格: {signal.price}, 强度: {signal.strength}")
        print(f"原因: {signal.reason}")
    
    # 测试2：模拟熊市数据
    print("\n=== 测试2: 熊市环境 ===")
    bear_index = pd.DataFrame({
        'Close': [100 - i * 0.8 for i in range(150)],
        'High': [100 - i * 0.8 + 5 for i in range(150)],
        'Low': [100 - i * 0.8 - 5 for i in range(150)],
        'Open': [100 - i * 0.8 for i in range(150)],
        'Volume': [1000000] * 150
    })
    
    strategy2 = AdaptiveStrategy()
    result2 = strategy2.detect_market_regime(bear_index)
    print(f"市场状态: {result2['regime_name']}")
    print(f"使用策略: {result2['strategy_name']}")
    
    # 测试3：模拟震荡市数据
    print("\n=== 测试3: 震荡市环境 ===")
    consolidate_index = pd.DataFrame({
        'Close': [100 + math.sin(i/10) * 10 for i in range(150)],
        'High': [100 + math.sin(i/10) * 10 + 5 for i in range(150)],
        'Low': [100 + math.sin(i/10) * 10 - 5 for i in range(150)],
        'Open': [100 + math.sin(i/10) * 10 for i in range(150)],
        'Volume': [1000000] * 150
    })
    
    strategy3 = AdaptiveStrategy()
    result3 = strategy3.detect_market_regime(consolidate_index)
    print(f"市场状态: {result3['regime_name']}")
    print(f"使用策略: {result3['strategy_name']}")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
