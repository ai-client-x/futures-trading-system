#!/usr/bin/env python3
"""
动量反转策略 (Momentum Reversal)
适用于熊市环境

策略逻辑：
- 价格持续下跌后出现反弹信号
- 短期RSI超卖 + 价格触及近期新低后反弹
- 结合成交量确认
"""

import logging
from datetime import datetime
from typing import Optional, List
import pandas as pd
import numpy as np

from ..models import Signal
from .base import Strategy

logger = logging.getLogger(__name__)


class MomentumReversalStrategy(Strategy):
    """
    动量反转策略
    
    适用于熊市环境的超跌反弹策略
    
    参数:
    - rsi_period: RSI周期 (默认14)
    - rsi_oversold: RSI超卖阈值 (默认25)
    - lookback: 查找近期低点的周期 (默认20)
    - volume_ma_period: 成交量均线周期 (默认20)
    - tp: 止盈比例 (默认0.15)
    - sl: 止损比例 (默认0.08)
    """
    
    def __init__(self, 
                 rsi_period: int = 14,
                 rsi_oversold: float = 25,
                 lookback: int = 20,
                 volume_ma_period: int = 20,
                 tp: float = 0.15,
                 sl: float = 0.08):
        super().__init__("MomentumReversal")
        self.rsi_period = rsi_period
        self.rsi_oversold = rsi_oversold
        self.lookback = lookback
        self.volume_ma_period = volume_ma_period
        self.tp = tp
        self.sl = sl
    
    def calculate_rsi(self, close: pd.Series) -> pd.Series:
        """计算RSI"""
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(window=self.rsi_period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def find_recent_low(self, close: pd.Series, lookback: int = 20) -> tuple:
        """
        查找近期低点
        
        Returns:
            (最低价位置, 最低价)
        """
        if len(close) < lookback:
            return None, None
        
        recent = close.iloc[-lookback:]
        min_idx = recent.idxmin()
        min_price = close.loc[min_idx]
        
        return min_idx, min_price
    
    def generate_signals(self, df: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """生成交易信号（实现基类抽象方法）"""
        return self.generate(df, code, name)
    
    def generate(self, df: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """生成交易信号"""
        if df is None or len(df) < max(self.lookback, self.rsi_period, self.volume_ma_period) + 5:
            return None
        
        try:
            close = df['Close']
            volume = df['Volume']
            
            # 计算RSI
            rsi = self.calculate_rsi(close)
            current_rsi = rsi.iloc[-1]
            prev_rsi = rsi.iloc[-2]
            
            # 计算成交量均线
            vol_ma = volume.rolling(window=self.volume_ma_period).mean()
            current_vol = volume.iloc[-1]
            avg_vol = vol_ma.iloc[-1]
            
            # 查找近期低点
            _, recent_low = self.find_recent_low(close, self.lookback)
            
            current_price = close.iloc[-1]
            prev_price = close.iloc[-2]
            
            # 计算价格变化
            price_change = (current_price - prev_price) / prev_price * 100
            
            # 计算止盈止损价格
            tp_price = current_price * (1 + self.tp)
            sl_price = current_price * (1 - self.sl)
            
            # 买入信号条件：
            # 1. RSI超卖后回升（从<25回升到>25）
            # 2. 价格从近期低点反弹
            # 3. 成交量放大
            rsi_buy_condition = prev_rsi <= self.rsi_oversold and current_rsi > self.rsi_oversold
            
            # 价格从低点反弹
            if recent_low is not None:
                price_rebound = (current_price - recent_low) / recent_low * 100
                rebound_condition = price_rebound > 0 and price_rebound < 10  # 反弹幅度0-10%
            else:
                rebound_condition = False
            
            # 成交量放大
            volume_condition = current_vol > avg_vol * 1.2 if not pd.isna(avg_vol) else False
            
            # 卖出信号：价格涨幅过大或RSI超买
            rsi_sell_condition = current_rsi > 70
            
            indicators = {
                'rsi': round(current_rsi, 2) if not pd.isna(current_rsi) else None,
                'rsi_period': self.rsi_period,
                'recent_low': round(recent_low, 2) if recent_low else None,
                'price_rebound_pct': round(price_rebound, 2) if recent_low else None,
                'volume_ratio': round(current_vol / avg_vol, 2) if avg_vol and avg_vol > 0 else None,
                'tp_price': round(tp_price, 2),
                'sl_price': round(sl_price, 2),
                'tp_pct': self.tp * 100,
                'sl_pct': self.sl * 100
            }
            
            # 综合判断买入信号
            if rsi_buy_condition and (rebound_condition or price_change > 0):
                # 计算信号强度
                strength = 50
                if rebound_condition:
                    strength += 20
                if volume_condition:
                    strength += 15
                if current_rsi < 20:  # 极度超卖
                    strength += 15
                
                strength = min(100, strength)
                
                reason_parts = [f"RSI超卖回升({current_rsi:.1f})"]
                if rebound_condition:
                    reason_parts.append(f"从低点反弹{price_rebound:.1f}%")
                if volume_condition:
                    reason_parts.append("成交量放大")
                reason_parts.append(f"止盈{tp_price:.2f}, 止损{sl_price:.2f}")
                
                return Signal(
                    code=code,
                    name=name,
                    action="buy",
                    price=round(current_price, 2),
                    strength=strength,
                    reason=", ".join(reason_parts),
                    timestamp=datetime.now().isoformat(),
                    indicators=indicators
                )
            
            # 卖出信号
            elif rsi_sell_condition:
                return Signal(
                    code=code,
                    name=name,
                    action="sell",
                    price=round(current_price, 2),
                    strength=min(100, 30 + (current_rsi - 70) * 2),
                    reason=f"RSI超买({current_rsi:.1f})",
                    timestamp=datetime.now().isoformat(),
                    indicators=indicators
                )
            
            # 持有/观望
            else:
                rsi_status = "超卖" if current_rsi < 30 else "超买" if current_rsi > 70 else "中性"
                return Signal(
                    code=code,
                    name=name,
                    action="hold",
                    price=round(current_price, 2),
                    strength=50,
                    reason=f"RSI {rsi_status}({current_rsi:.1f}), 等待信号",
                    timestamp=datetime.now().isoformat(),
                    indicators=indicators
                )
                
        except Exception as e:
            logger.error(f"动量反转策略错误 {code}: {e}")
            return None
    
    def get_params(self) -> dict:
        """获取策略参数"""
        return {
            'rsi_period': self.rsi_period,
            'rsi_oversold': self.rsi_oversold,
            'lookback': self.lookback,
            'volume_ma_period': self.volume_ma_period,
            'tp': self.tp,
            'sl': self.sl
        }
    
    def set_params(self, **kwargs):
        """设置策略参数"""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)


# 策略工厂函数
def create_momentum_reversal_strategy(**params) -> MomentumReversalStrategy:
    """创建动量反转策略"""
    default_params = {
        'rsi_period': 14,
        'rsi_oversold': 25,
        'lookback': 20,
        'volume_ma_period': 20,
        'tp': 0.15,
        'sl': 0.08
    }
    default_params.update(params)
    return MomentumReversalStrategy(**default_params)


if __name__ == "__main__":
    # 测试
    import random
    
    print("=== 动量反转策略测试 ===")
    
    # 模拟数据：先下跌后反弹
    prices = [100]
    for i in range(25):
        prices.append(prices[-1] - 3)  # 下跌到75
    
    # 反弹
    prices.append(78)
    prices.append(82)
    
    df = pd.DataFrame({
        'Open': prices,
        'High': [p + 2 for p in prices],
        'Low': [p - 2 for p in prices],
        'Close': prices,
        'Volume': [1000000] * len(prices)
    })
    
    strategy = MomentumReversalStrategy()
    signal = strategy.generate(df, "000001", "测试股票")
    
    if signal:
        print(f"信号: {signal.action}")
        print(f"价格: {signal.price}")
        print(f"强度: {signal.strength}")
        print(f"原因: {signal.reason}")
        print(f"指标: {signal.indicators}")
