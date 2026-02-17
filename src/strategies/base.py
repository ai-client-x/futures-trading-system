#!/usr/bin/env python3
"""
策略基类
Strategy Base Classes
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from datetime import datetime

import pandas as pd

from ..models import Signal

logger = logging.getLogger(__name__)


class Strategy(ABC):
    """
    策略基类
    所有策略必须继承此类并实现必要方法
    """
    
    def __init__(self, name: str = "Strategy"):
        self.name = name
        self.positions: Dict[str, dict] = {}
        self.signals: List[Signal] = []
        self.params: Dict = {}
    
    @abstractmethod
    def generate_signals(self, data: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """
        生成交易信号
        
        Args:
            data: 股票数据
            code: 股票代码
            name: 股票名称
        
        Returns:
            Signal 或 None
        """
        pass
    
    def on_bar(self, bar: dict):
        """
        每根K线回调
        可用于实时交易逻辑
        """
        pass
    
    def on_trade(self, trade: dict):
        """
        成交回调
        可用于成交后的处理
        """
        logger.info(f"成交: {trade}")
    
    def on_tick(self, tick: dict):
        """
        行情回调
        可用于高频策略
        """
        pass
    
    def set_params(self, **params):
        """设置策略参数"""
        self.params.update(params)
    
    def get_params(self) -> Dict:
        """获取策略参数"""
        return self.params.copy()


class TrendStrategy(Strategy):
    """趋势跟踪策略"""
    
    def __init__(self, name: str = "TrendStrategy", ma_short: int = 5, ma_long: int = 20):
        super().__init__(name)
        self.ma_short = ma_short
        self.ma_long = ma_long
    
    def generate_signals(self, data: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """基于均线交叉的趋势策略"""
        if len(data) < self.ma_long + 1:
            return None
        
        close = data['Close']
        ma_short = close.rolling(window=self.ma_short).mean()
        ma_long = close.rolling(window=self.ma_long).mean()
        
        latest = data.iloc[-1]
        current_price = latest['Close']
        
        # 金叉买入
        if ma_short.iloc[-1] > ma_long.iloc[-1] and ma_short.iloc[-2] <= ma_long.iloc[-2]:
            return Signal(
                code=code,
                name=name,
                action="buy",
                strength=75,
                reason=f"MA{self.ma_short}上穿MA{self.ma_long}金叉",
                price=current_price,
                timestamp=datetime.now().isoformat()
            )
        
        # 死叉卖出
        if ma_short.iloc[-1] < ma_long.iloc[-1] and ma_short.iloc[-2] >= ma_long.iloc[-2]:
            return Signal(
                code=code,
                name=name,
                action="sell",
                strength=75,
                reason=f"MA{self.ma_short}下穿MA{self.ma_long}死叉",
                price=current_price,
                timestamp=datetime.now().isoformat()
            )
        
        return None


class MeanReversionStrategy(Strategy):
    """均值回归策略"""
    
    def __init__(self, name: str = "MeanReversion", period: int = 20, std_dev: float = 2):
        super().__init__(name)
        self.period = period
        self.std_dev = std_dev
    
    def generate_signals(self, data: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """基于布林带的均值回归策略"""
        if len(data) < self.period + 1:
            return None
        
        close = data['Close']
        ma = close.rolling(window=self.period).mean()
        std = close.rolling(window=self.period).std()
        
        upper = ma + std * self.std_dev
        lower = ma - std * self.std_dev
        
        latest = data.iloc[-1]
        current_price = latest['Close']
        
        # 触及下轨买入
        if current_price <= lower.iloc[-1]:
            return Signal(
                code=code,
                name=name,
                action="buy",
                strength=70,
                reason=f"价格触及布林下轨",
                price=current_price,
                timestamp=datetime.now().isoformat()
            )
        
        # 触及上轨卖出
        if current_price >= upper.iloc[-1]:
            return Signal(
                code=code,
                name=name,
                action="sell",
                strength=70,
                reason=f"价格触及布林上轨",
                price=current_price,
                timestamp=datetime.now().isoformat()
            )
        
        return None


class BreakoutStrategy(Strategy):
    """突破策略"""
    
    def __init__(self, name: str = "Breakout", lookback: int = 20):
        super().__init__(name)
        self.lookback = lookback
    
    def generate_signals(self, data: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """基于突破高/低点的策略"""
        if len(data) < self.lookback + 1:
            return None
        
        latest = data.iloc[-1]
        current_price = latest['Close']
        high = data['High']
        low = data['Low']
        
        # 最近N天最高价
        highest = high.rolling(window=self.lookback).max().iloc[-1]
        lowest = low.rolling(window=self.lookback).min().iloc[-1]
        
        # 突破买入
        if current_price > highest:
            return Signal(
                code=code,
                name=name,
                action="buy",
                strength=80,
                reason=f"突破{self.lookback}日高点",
                price=current_price,
                timestamp=datetime.now().isoformat()
            )
        
        # 跌破卖出
        if current_price < lowest:
            return Signal(
                code=code,
                name=name,
                action="sell",
                strength=80,
                reason=f"跌破{self.lookback}日低点",
                price=current_price,
                timestamp=datetime.now().isoformat()
            )
        
        return None
