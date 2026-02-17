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
    HOLD = "hold"


@dataclass
class Trade:
    """交易记录"""
    date: str
    code: str
    name: str
    action: str  # buy/sell
    price: float
    quantity: int
    commission: float
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Position:
    """持仓"""
    code: str
    name: str
    quantity: int
    avg_cost: float
    current_price: float = 0
    entry_date: str = None
    
    def __post_init__(self):
        if self.entry_date is None:
            self.entry_date = datetime.now().strftime("%Y-%m-%d")
    
    @property
    def market_value(self) -> float:
        return self.quantity * self.current_price
    
    @property
    def cost_basis(self) -> float:
        return self.quantity * self.avg_cost
    
    @property
    def profit(self) -> float:
        return (self.current_price - self.avg_cost) * self.quantity
    
    @property
    def profit_pct(self) -> float:
        if self.avg_cost == 0:
            return 0
        return (self.current_price - self.avg_cost) / self.avg_cost * 100
    
    @property
    def unrealized_pnl(self) -> float:
        """未实现盈亏"""
        return self.profit
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Signal:
    """
    交易信号 - 符合最佳实践规范
    必须包含: code/action/strength/reason/timestamp
    """
    code: str
    action: str  # buy/sell/hold
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
        return self.action == "buy"
    
    @property
    def is_sell(self) -> bool:
        return self.action == "sell"


@dataclass
class Order:
    """订单"""
    order_id: str
    code: str
    name: str
    action: str
    price: float
    quantity: int
    status: str = "pending"  # pending/submitted/partial_filled/filled/cancelled/rejected
    filled_quantity: int = 0
    create_time: str = None
    update_time: str = None
    
    def __post_init__(self):
        if self.create_time is None:
            self.create_time = datetime.now().isoformat()
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Portfolio:
    """投资组合"""
    cash: float
    positions: Dict[str, Position]
    trades: List[Trade]
    initial_capital: float = 1000000
    
    @property
    def total_assets(self) -> float:
        pos_value = sum(p.market_value for p in self.positions.values())
        return self.cash + pos_value
    
    @property
    def total_profit(self) -> float:
        return sum(p.profit for p in self.positions.values())
    
    @property
    def profit_pct(self) -> float:
        return (self.total_assets - self.initial_capital) / self.initial_capital * 100
    
    @property
    def position_ratio(self) -> float:
        """持仓比例"""
        if self.total_assets == 0:
            return 0
        return (self.total_assets - self.cash) / self.total_assets
    
    def to_dict(self) -> dict:
        pos_value = sum(p.market_value for p in self.positions.values())
        return {
            "cash": self.cash,
            "total_assets": self.total_assets,
            "total_profit": self.total_profit,
            "profit_pct": self.profit_pct,
            "position_ratio": self.position_ratio,
            "position_value": pos_value,
            "positions": {k: v.to_dict() for k, v in self.positions.items()},
            "trades": [t.to_dict() for t in self.trades]
        }


@dataclass
class DailyRecord:
    """每日记录"""
    date: str
    total_assets: float
    cash: float
    position_value: float
    profit: float
    profit_pct: float
    trade_count: int
    
    def to_dict(self) -> dict:
        return asdict(self)
