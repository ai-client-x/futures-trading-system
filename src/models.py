#!/usr/bin/env python3
"""
数据类定义
Data Classes
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, List
from enum import Enum


class ActionType(Enum):
    """交易动作"""
    BUY = "buy"
    SELL = "sell"
    OPEN = "open"
    CLOSE = "close"


class DirectionType(Enum):
    """交易方向"""
    LONG = "long"
    SHORT = "short"


@dataclass
class FuturesTrade:
    """期货交易记录"""
    date: str
    code: str
    name: str
    action: str      # open/close
    direction: str  # long/short
    price: float
    quantity: int    # 手数
    commission: float
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class FuturesPosition:
    """期货持仓"""
    code: str
    name: str
    direction: str   # long/short
    quantity: int     # 手数
    avg_price: float
    current_price: float = 0
    entry_date: str = None
    contracts: dict = None  # 合约配置注入
    
    def __post_init__(self):
        if self.entry_date is None:
            self.entry_date = datetime.now().strftime("%Y-%m-%d")
        if self.contracts is None:
            from ..config import config
            self.contracts = config.contracts
    
    @property
    def margin(self) -> float:
        """保证金"""
        contract = self.contracts.get(self.code, {})
        return contract.get("margin", 5000) * self.quantity
    
    @property
    def market_value(self) -> float:
        """合约价值"""
        contract = self.contracts.get(self.code, {})
        multiplier = contract.get("multiplier", 10)
        return self.current_price * multiplier * self.quantity
    
    @property
    def profit(self) -> float:
        """盈亏"""
        contract = self.contracts.get(self.code, {})
        multiplier = contract.get("multiplier", 10)
        if self.direction == "long":
            return (self.current_price - self.avg_price) * multiplier * self.quantity
        else:
            return (self.avg_price - self.current_price) * multiplier * self.quantity
    
    @property
    def profit_pct(self) -> float:
        """盈亏比例"""
        if self.avg_price == 0:
            return 0
        return (self.current_price - self.avg_price) / self.avg_price * 100
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class FuturesPortfolio:
    """期货投资组合"""
    cash: float
    positions: Dict[str, 'FuturesPosition']
    trades: List['FuturesTrade']
    frozen_margin: float = 0
    initial_capital: float = 500000
    
    @property
    def total_assets(self) -> float:
        pos_profit = sum(p.profit for p in self.positions.values())
        return self.cash + self.frozen_margin + pos_profit
    
    @property
    def available_margin(self) -> float:
        """可用保证金"""
        return self.cash
    
    @property
    def total_profit(self) -> float:
        return sum(p.profit for p in self.positions.values())
    
    @property
    def profit_pct(self) -> float:
        return (self.total_assets - self.initial_capital) / self.initial_capital * 100
    
    def to_dict(self) -> dict:
        return {
            "cash": self.cash,
            "total_assets": self.total_assets,
            "available_margin": self.available_margin,
            "frozen_margin": self.frozen_margin,
            "total_profit": self.total_profit,
            "profit_pct": self.profit_pct,
            "positions": {k: v.to_dict() for k, v in self.positions.items()},
            "trades": [t.to_dict() for t in self.trades]
        }


@dataclass
class Signal:
    """
    交易信号 - 符合最佳实践规范
    必须包含: code/action/strength/reason/timestamp
    """
    code: str
    action: str      # buy/sell/open/close
    direction: str   # long/short
    strength: float  # 0-100
    reason: str
    price: float = 0
    name: str = ""
    timestamp: str = None
    indicators: dict = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
        if self.indicators is None:
            self.indicators = {}
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @property
    def is_buy(self) -> bool:
        return self.action in ["buy", "open"]
    
    @property
    def is_sell(self) -> bool:
        return self.action in ["sell", "close"]


@dataclass
class DailyRecord:
    """每日记录"""
    date: str
    total_assets: float
    cash: float
    frozen_margin: float
    profit: float
    profit_pct: float
    trade_count: int
    
    def to_dict(self) -> dict:
        return asdict(self)
