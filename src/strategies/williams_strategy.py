#!/usr/bin/env python3
"""
威廉指标策略 (Williams %R)
适用于牛市环境

策略逻辑：
- WR < -90 时为超卖区域，考虑买入
- WR > -10 时为超买区域，考虑卖出
"""

import logging
from datetime import datetime
from typing import Optional, List
import pandas as pd
import numpy as np

from ..models import Signal
from .base import Strategy

logger = logging.getLogger(__name__)


class WilliamsStrategy(Strategy):
    """
    威廉指标策略
    
    参数:
    - period: WR周期 (默认14)
    - oversold: 超卖阈值 (默认-90)
    - overbought: 超买阈值 (默认-10)
    - tp: 止盈比例 (默认0.28)
    - sl: 止损比例 (默认0.15)
    """
    
    def __init__(self, 
                 period: int = 14,
                 oversold: float = -90,
                 overbought: float = -10,
                 tp: float = 0.28,
                 sl: float = 0.15):
        super().__init__("Williams")
        self.period = period
        self.oversold = oversold
        self.overbought = overbought
        self.tp = tp  # 止盈比例
        self.sl = sl  # 止损比例
    
    def calculate_wr(self, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
        """
        计算威廉指标
        
        WR = (HHN - C) / (HHN - LLN) * -100
        其中:
        - HHN = N日内最高价
        - LLN = N日内最低价
        - C = 收盘价
        """
        highest = high.rolling(window=self.period).max()
        lowest = low.rolling(window=self.period).min()
        
        wr = ((highest - close) / (highest - lowest)) * -100
        return wr
    
    def generate_signals(self, df: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """生成交易信号（实现基类抽象方法）"""
        return self.generate(df, code, name)
    
    def generate(self, df: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """生成交易信号"""
        if df is None or len(df) < self.period + 5:
            return None
        
        try:
            close = df['Close']
            high = df['High']
            low = df['Low']
            
            # 计算威廉指标
            wr = self.calculate_wr(high, low, close)
            
            # 获取最新值
            current_wr = wr.iloc[-1]
            prev_wr = wr.iloc[-2]
            
            if pd.isna(current_wr):
                return None
            
            current_price = close.iloc[-1]
            
            # 计算止盈止损价格
            tp_price = current_price * (1 + self.tp)
            sl_price = current_price * (1 - self.sl)
            
            # 交易逻辑
            # 买入信号：WR从超卖区域回升（从<-90回升到>-90）
            buy_condition = prev_wr <= self.oversold and current_wr > self.oversold
            
            # 卖出信号：WR进入超买区域
            sell_condition = current_wr > self.overbought
            
            indicators = {
                'wr': round(current_wr, 2),
                'wr_period': self.period,
                'tp_price': round(tp_price, 2),
                'sl_price': round(sl_price, 2),
                'tp_pct': self.tp * 100,
                'sl_pct': self.sl * 100
            }
            
            if buy_condition:
                return Signal(
                    code=code,
                    name=name,
                    action="buy",
                    price=round(current_price, 2),
                    strength=min(100, 50 + abs(current_wr - self.oversold)),
                    reason=f"威廉指标超卖回升(WR={current_wr:.1f}), 止盈{tp_price:.2f}, 止损{sl_price:.2f}",
                    timestamp=datetime.now().isoformat(),
                    indicators=indicators
                )
            
            elif sell_condition:
                return Signal(
                    code=code,
                    name=name,
                    action="sell",
                    price=round(current_price, 2),
                    strength=min(100, 30 + abs(current_wr - self.overbought)),
                    reason=f"威廉指标超买(WR={current_wr:.1f})",
                    timestamp=datetime.now().isoformat(),
                    indicators=indicators
                )
            
            else:
                return Signal(
                    code=code,
                    name=name,
                    action="hold",
                    price=round(current_price, 2),
                    strength=50,
                    reason=f"威廉指标中性(WR={current_wr:.1f})",
                    timestamp=datetime.now().isoformat(),
                    indicators=indicators
                )
                
        except Exception as e:
            logger.error(f"威廉指标策略错误 {code}: {e}")
            return None
    
    def get_params(self) -> dict:
        """获取策略参数"""
        return {
            'period': self.period,
            'oversold': self.oversold,
            'overbought': self.overbought,
            'tp': self.tp,
            'sl': self.sl
        }
    
    def set_params(self, **kwargs):
        """设置策略参数"""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)


# 策略工厂函数
def create_williams_strategy(**params) -> WilliamsStrategy:
    """创建威廉指标策略"""
    default_params = {
        'period': 14,
        'oversold': -90,
        'overbought': -10,
        'tp': 0.28,
        'sl': 0.15
    }
    default_params.update(params)
    return WilliamsStrategy(**default_params)


if __name__ == "__main__":
    # 测试
    import random
    
    print("=== 威廉指标策略测试 ===")
    
    # 模拟数据：先下跌后反弹（买入信号）
    prices = [100]
    for i in range(30):
        prices.append(prices[-1] - 2)  # 下跌
    
    # 威廉指标需要最高价和最低价
    high_prices = [p + random.uniform(0, 3) for p in prices]
    low_prices = [p - random.uniform(0, 3) for p in prices]
    
    df = pd.DataFrame({
        'Open': prices,
        'High': high_prices,
        'Low': low_prices,
        'Close': prices,
        'Volume': [1000000] * len(prices)
    })
    
    strategy = WilliamsStrategy()
    signal = strategy.generate(df, "000001", "测试股票")
    
    if signal:
        print(f"信号: {signal.action}")
        print(f"价格: {signal.price}")
        print(f"强度: {signal.strength}")
        print(f"原因: {signal.reason}")
        print(f"指标: {signal.indicators}")
