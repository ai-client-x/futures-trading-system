#!/usr/bin/env python3
"""
布林带策略 (Bollinger Bands)
适用于震荡市环境

策略逻辑：
- 价格触及下轨买入（超卖）
- 价格触及上轨卖出（超买）
- 价格突破中轨确认趋势
"""

import logging
from datetime import datetime
from typing import Optional, List
import pandas as pd
import numpy as np

from ..models import Signal
from .base import Strategy

logger = logging.getLogger(__name__)


class BollingerStrategy(Strategy):
    """
    布林带策略
    
    适用于震荡市的区间波动策略
    
    参数:
    - period: 布林带周期 (默认20)
    - std_dev: 标准差倍数 (默认2)
    - tp: 止盈比例 (默认0.10)
    - sl: 止损比例 (默认0.05)
    """
    
    def __init__(self, 
                 period: int = 20,
                 std_dev: float = 2.0,
                 tp: float = 0.10,
                 sl: float = 0.05):
        super().__init__("Bollinger")
        self.period = period
        self.std_dev = std_dev
        self.tp = tp
        self.sl = sl
    
    def calculate_bands(self, close: pd.Series) -> tuple:
        """
        计算布林带
        
        Returns:
            (upper_band, middle_band, lower_band)
        """
        middle = close.rolling(window=self.period).mean()
        std = close.rolling(window=self.period).std()
        
        upper = middle + (std * self.std_dev)
        lower = middle - (std * self.std_dev)
        
        return upper, middle, lower
    
    def calculate_bandwidth(self, upper: pd.Series, lower: pd.Series, middle: pd.Series) -> pd.Series:
        """计算布林带宽度（波动性指标）"""
        return (upper - lower) / middle * 100
    
    def generate_signals(self, df: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """生成交易信号（实现基类抽象方法）"""
        return self.generate(df, code, name)
    
    def generate(self, df: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """生成交易信号"""
        if df is None or len(df) < self.period + 5:
            return None
        
        try:
            close = df['Close']
            
            # 计算布林带
            upper, middle, lower = self.calculate_bands(close)
            
            # 获取最新值
            current_price = close.iloc[-1]
            prev_price = close.iloc[-2]
            
            current_upper = upper.iloc[-1]
            current_middle = middle.iloc[-1]
            current_lower = lower.iloc[-1]
            
            prev_upper = upper.iloc[-2]
            prev_middle = middle.iloc[-2]
            prev_lower = lower.iloc[-2]
            
            if any(pd.isna([current_upper, current_middle, current_lower])):
                return None
            
            # 计算价格位置（相对于布林带）
            band_position = (current_price - current_lower) / (current_upper - current_lower) * 100
            
            # 计算止盈止损价格
            # 买入后止盈：价格达到中轨或上轨
            tp_price_buy = current_middle  # 止盈到中轨
            sl_price_buy = current_price * (1 - self.sl)
            
            # 计算止盈比例（价格到中轨的距离）
            tp_pct_buy = (current_middle - current_price) / current_price * 100
            
            indicators = {
                'upper': round(current_upper, 2),
                'middle': round(current_middle, 2),
                'lower': round(current_lower, 2),
                'band_position': round(band_position, 2),
                'bandwidth': round((current_upper - current_lower) / current_middle * 100, 2),
                'tp_price': round(tp_price_buy, 2),
                'sl_price': round(sl_price_buy, 2),
                'tp_pct': round(tp_pct_buy, 2),
                'sl_pct': self.sl * 100
            }
            
            # 买入信号：价格触及或跌破下轨后反弹
            # 条件：价格从下轨附近反弹，或者价格突破中轨
            touch_lower = current_price <= current_lower
            bounce_from_lower = prev_price <= prev_lower and current_price > current_lower
            
            # 价格突破中轨（从下往上）
            cross_middle_up = prev_price <= prev_middle and current_price > current_middle
            
            # 卖出信号：价格触及上轨
            touch_upper = current_price >= current_upper
            
            # 价格跌破中轨（从上往下）
            cross_middle_down = prev_price >= prev_middle and current_price < current_middle
            
            if bounce_from_lower or (touch_lower and cross_middle_up):
                # 买入信号强度
                strength = 50
                if band_position < 10:  # 极度超卖
                    strength += 30
                elif band_position < 20:
                    strength += 20
                
                if cross_middle_up:  # 突破中轨确认
                    strength += 15
                
                strength = min(100, strength)
                
                reason = f"价格{'触及' if touch_lower else '反弹自'}布林下轨({current_lower:.2f})"
                if cross_middle_up:
                    reason += ", 突破中轨"
                reason += f", 止盈{tp_price_buy:.2f}, 止损{sl_price_buy:.2f}"
                
                return Signal(
                    code=code,
                    name=name,
                    action="buy",
                    price=round(current_price, 2),
                    strength=strength,
                    reason=reason,
                    timestamp=datetime.now().isoformat(),
                    indicators=indicators
                )
            
            elif touch_upper or cross_middle_down:
                # 卖出信号
                strength = 50
                if band_position > 90:  # 极度超买
                    strength += 30
                elif band_position > 80:
                    strength += 20
                
                if cross_middle_down:
                    strength += 15
                
                strength = min(100, strength)
                
                reason = f"价格{'触及' if touch_upper else '跌破'}布林上轨({current_upper:.2f})"
                if cross_middle_down:
                    reason += ", 跌破中轨"
                
                return Signal(
                    code=code,
                    name=name,
                    action="sell",
                    price=round(current_price, 2),
                    strength=strength,
                    reason=reason,
                    timestamp=datetime.now().isoformat(),
                    indicators=indicators
                )
            
            else:
                # 持有/观望
                position_desc = "超卖" if band_position < 30 else "超买" if band_position > 70 else "中性"
                
                return Signal(
                    code=code,
                    name=name,
                    action="hold",
                    price=round(current_price, 2),
                    strength=50,
                    reason=f"布林带{position_desc}(位置{band_position:.1f}%), 在区间内波动",
                    timestamp=datetime.now().isoformat(),
                    indicators=indicators
                )
                
        except Exception as e:
            logger.error(f"布林带策略错误 {code}: {e}")
            return None
    
    def get_params(self) -> dict:
        """获取策略参数"""
        return {
            'period': self.period,
            'std_dev': self.std_dev,
            'tp': self.tp,
            'sl': self.sl
        }
    
    def set_params(self, **kwargs):
        """设置策略参数"""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)


# 策略工厂函数
def create_bollinger_strategy(**params) -> BollingerStrategy:
    """创建布林带策略"""
    default_params = {
        'period': 20,
        'std_dev': 2.0,
        'tp': 0.10,
        'sl': 0.05
    }
    default_params.update(params)
    return BollingerStrategy(**default_params)


if __name__ == "__main__":
    # 测试
    import random
    import math
    
    print("=== 布林带策略测试 ===")
    
    # 模拟震荡市数据：价格在区间内波动
    base_price = 100
    prices = []
    for i in range(50):
        # 在100附近波动
        prices.append(base_price + math.sin(i / 5) * 10 + random.uniform(-2, 2))
    
    df = pd.DataFrame({
        'Open': prices,
        'High': [p + 3 for p in prices],
        'Low': [p - 3 for p in prices],
        'Close': prices,
        'Volume': [1000000] * len(prices)
    })
    
    strategy = BollingerStrategy()
    signal = strategy.generate(df, "000001", "测试股票")
    
    if signal:
        print(f"信号: {signal.action}")
        print(f"价格: {signal.price}")
        print(f"强度: {signal.strength}")
        print(f"原因: {signal.reason}")
        print(f"指标: {signal.indicators}")
