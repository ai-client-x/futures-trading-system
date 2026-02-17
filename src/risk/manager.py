#!/usr/bin/env python3
"""
风控模块
Risk Management
"""

import logging
from datetime import datetime
from typing import Dict, Optional, List
from dataclasses import dataclass

from ..models import Position, Signal
from ..config import config

logger = logging.getLogger(__name__)


@dataclass
class RiskCheckResult:
    """风控检查结果"""
    passed: bool
    reason: str = ""
    action: str = "allow"  # allow/reduce/close


class RiskManager:
    """
    风控管理器
    负责: 仓位管理、止损止盈、风险监控
    """
    
    def __init__(self):
        self.config = config
        self.daily_loss = 0  # 当日亏损
        self.daily_trades = 0  # 当日交易次数
        self.last_trade_date = None
        self.max_daily_loss = self.initial_capital * self.config.max_loss_per_day
        
        # 熔断标志
        self.circuit_broken = False
        self.circuit_reason = ""
    
    @property
    def initial_capital(self) -> float:
        return self.config.initial_capital
    
    def check_buy(self, engine, signal: Signal) -> RiskCheckResult:
        """
        买入风控检查
        """
        # 检查熔断
        if self.circuit_broken:
            return RiskCheckResult(False, f"熔断中: {self.circuit_reason}", "reject")
        
        # 检查当日亏损
        if self.daily_loss <= -self.initial_capital * self.config.max_loss_per_day:
            self.circuit_broken = True
            self.circuit_reason = "单日亏损达到上限"
            return RiskCheckResult(False, self.circuit_reason, "reject")
        
        # 检查仓位
        position_ratio = engine.portfolio.position_ratio
        if position_ratio >= self.config.max_position:
            return RiskCheckResult(False, f"仓位已满 ({position_ratio*100:.1f}%)", "reject")
        
        return RiskCheckResult(True, "通过")
    
    def check_sell(self, engine, code: str, price: float) -> RiskCheckResult:
        """
        卖出风控检查
        """
        # 检查熔断
        if self.circuit_broken:
            return RiskCheckResult(False, f"熔断中: {self.circuit_reason}", "reject")
        
        pos = engine.get_position(code)
        if not pos:
            return RiskCheckResult(False, "无持仓", "reject")
        
        # 止损检查
        stop_loss_price = pos.avg_cost * (1 - self.config.stop_loss_pct)
        if price <= stop_loss_price:
            logger.warning(f"触发止损: {code} @ ¥{price} (止损价: ¥{stop_loss_price:.2f})")
            return RiskCheckResult(True, "触发止损", "allow")
        
        # 止盈检查
        take_profit_price = pos.avg_cost * (1 + self.config.take_profit_pct)
        if price >= take_profit_price:
            logger.info(f"触发止盈: {code} @ ¥{price} (止盈价: ¥{take_profit_price:.2f})")
            return RiskCheckResult(True, "触发止盈", "allow")
        
        return RiskCheckResult(True, "通过")
    
    def calculate_position_size(self, engine, price: float, risk_ratio: float = None) -> int:
        """
        根据风险比例计算仓位
        
        Args:
            engine: 交易引擎
            price: 股票价格
            risk_ratio: 风险比例 (默认配置文件中的单笔最大亏损)
        
        Returns:
            买入数量（手）
        """
        risk_ratio = risk_ratio or self.config.max_loss_per_trade
        
        # 计算最大亏损金额
        max_loss = engine.portfolio.total_assets * risk_ratio
        
        # 计算买入数量（整手）
        quantity = int(max_loss / price / 100) * 100
        
        # 确保不超过可用资金
        max_affordable_quantity = int(engine.portfolio.cash / price / 100) * 100
        quantity = min(quantity, max_affordable_quantity)
        
        return max(quantity, 0)
    
    def check_stop_loss(self, engine, code: str, current_price: float) -> List[str]:
        """
        检查是否触发止损
        
        Returns:
            需要卖出的股票代码列表
        """
        to_sell = []
        
        pos = engine.get_position(code)
        if not pos:
            return to_sell
        
        # 计算止损价格
        stop_loss_price = pos.avg_cost * (1 - self.config.stop_loss_pct)
        
        if current_price <= stop_loss_price:
            logger.warning(f"触发止损: {code} 当前价¥{current_price} <= 止损价¥{stop_loss_price:.2f}")
            to_sell.append(code)
        
        return to_sell
    
    def check_take_profit(self, engine, code: str, current_price: float) -> List[str]:
        """
        检查是否触发止盈
        
        Returns:
            需要卖出的股票代码列表
        """
        to_sell = []
        
        pos = engine.get_position(code)
        if not pos:
            return to_sell
        
        # 计算止盈价格
        take_profit_price = pos.avg_cost * (1 + self.config.take_profit_pct)
        
        if current_price >= take_profit_price:
            logger.info(f"触发止盈: {code} 当前价¥{current_price} >= 止盈价¥{take_profit_price:.2f}")
            to_sell.append(code)
        
        return to_sell
    
    def update_daily_stats(self, engine, date: str):
        """更新每日统计"""
        if self.last_trade_date != date:
            self.daily_loss = 0
            self.daily_trades = 0
            self.last_trade_date = date
        
        # 计算当日亏损
        current_assets = engine.portfolio.total_assets
        self.daily_loss = current_assets - self.initial_capital
    
    def reset_daily(self, date: str):
        """重置每日统计"""
        self.daily_loss = 0
        self.daily_trades = 0
        self.last_trade_date = date
        self.circuit_broken = False
        self.circuit_reason = ""
        
        logger.info(f"风控统计已重置: {date}")
    
    def get_risk_status(self) -> dict:
        """获取风控状态"""
        return {
            "daily_loss": self.daily_loss,
            "daily_trades": self.daily_trades,
            "circuit_broken": self.circuit_broken,
            "circuit_reason": self.circuit_reason,
            "max_daily_loss": self.max_daily_loss
        }
