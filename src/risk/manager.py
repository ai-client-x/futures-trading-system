#!/usr/bin/env python3
"""
风控模块
Risk Management for Futures
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple
from dataclasses import dataclass

from .models import FuturesPosition, Signal
from .config import config

logger = logging.getLogger(__name__)


@dataclass
class RiskCheckResult:
    """风控检查结果"""
    passed: bool
    reason: str = ""
    action: str = "allow"  # allow/reduce/close


class FuturesRiskManager:
    """
    期货风控管理器
    负责: 保证金管理、仓位管理、止损止盈、风险监控
    """
    
    def __init__(self):
        self.config = config
        self.daily_loss = 0
        self.last_trade_date = None
        
        # 熔断标志
        self.circuit_broken = False
        self.circuit_reason = ""
        
        # 历史最高资金（用于计算回撤）
        self.peak_assets = 0
    
    @property
    def initial_capital(self) -> float:
        return self.config.initial_capital
    
    def check_open(self, engine, code: str, direction: str, 
                   price: float, quantity: int) -> RiskCheckResult:
        """
        开仓风控检查
        """
        # 检查熔断
        if self.circuit_broken:
            return RiskCheckResult(False, f"熔断中: {self.circuit_reason}", "reject")
        
        # 检查当日亏损
        current_assets = engine.portfolio.total_assets
        daily_loss = (self.initial_capital - current_assets) / self.initial_capital
        
        if daily_loss >= self.config.get('max_loss_per_day', 0.05):
            self.circuit_broken = True
            self.circuit_reason = "单日亏损达到上限"
            return RiskCheckResult(False, self.circuit_reason, "reject")
        
        # 检查持仓数量
        if len(engine.portfolio.positions) >= self.config.max_positions:
            return RiskCheckResult(False, f"已达最大持仓数 {self.config.max_positions}", "reject")
        
        # 检查保证金比例
        total_margin = engine.portfolio.frozen_margin + self._calculate_margin(engine, code, quantity)
        margin_ratio = total_margin / current_assets
        
        if margin_ratio > self.config.max_position:
            return RiskCheckResult(False, f"保证金比例过高 {margin_ratio*100:.1f}%", "reduce")
        
        return RiskCheckResult(True, "通过")
    
    def check_close(self, engine, code: str, direction: str, 
                    price: float) -> RiskCheckResult:
        """
        平仓风控检查
        """
        # 检查熔断
        if self.circuit_broken:
            return RiskCheckResult(False, f"熔断中: {self.circuit_reason}", "reject")
        
        return RiskCheckResult(True, "通过")
    
    def _calculate_margin(self, engine, code: str, quantity: int) -> float:
        """计算保证金"""
        contract = self.config.contracts.get(code, {})
        return contract.get("margin", 5000) * quantity
    
    def calculate_position_size(self, engine, code: str, price: float, 
                                risk_ratio: float = None) -> int:
        """
        根据风险比例计算开仓手数
        
        Args:
            engine: 交易引擎
            code: 合约代码
            price: 价格
            risk_ratio: 风险比例 (默认单笔最大亏损)
        
        Returns:
            开仓手数
        """
        risk_ratio = risk_ratio or self.config.max_loss_per_trade
        
        # 计算最大亏损金额
        max_loss = engine.portfolio.total_assets * risk_ratio
        
        # 计算每手亏损
        contract = self.config.contracts.get(code, {})
        multiplier = contract.get("multiplier", 10)
        
        # 根据价格波动计算手数
        quantity = int(max_loss / (price * multiplier * 0.02))  # 假设2%波动
        
        # 确保不超过可用保证金
        available_margin = engine.portfolio.cash
        max_qty_by_margin = int(available_margin / contract.get("margin", 5000))
        
        quantity = min(quantity, max_qty_by_margin)
        
        return max(quantity, 0)
    
    def check_stop_loss(self, engine) -> List[dict]:
        """
        检查所有持仓是否触发止损
        
        Returns:
            需要平仓的持仓列表
        """
        to_close = []
        
        for pos_key, pos in engine.portfolio.positions.items():
            # 止损检查
            if pos.profit_pct <= -self.config.stop_loss_pct * 100:
                logger.warning(f"触发止损: {pos.code} {pos.direction} {pos.quantity}手, 亏损{pos.profit_pct:.1f}%")
                to_close.append({
                    "code": pos.code,
                    "direction": pos.direction,
                    "quantity": pos.quantity,
                    "reason": "止损",
                    "profit_pct": pos.profit_pct
                })
            
            # 止盈检查
            elif pos.profit_pct >= self.config.take_profit_pct * 100:
                logger.info(f"触发止盈: {pos.code} {pos.direction} {pos.quantity}手, 盈利{pos.profit_pct:.1f}%")
                to_close.append({
                    "code": pos.code,
                    "direction": pos.direction,
                    "quantity": pos.quantity,
                    "reason": "止盈",
                    "profit_pct": pos.profit_pct
                })
        
        return to_close
    
    def check_margin_call(self, engine) -> List[dict]:
        """
        检查是否触发追保
        
        Returns:
            需要追保的持仓列表
        """
        margin_calls = []
        current_assets = engine.portfolio.total_assets
        
        # 假设维持保证金比例为初始保证金的80%
        maintenance_ratio = 0.8
        
        for pos_key, pos in engine.portfolio.positions.items():
            contract = self.config.contracts.get(pos.code, {})
            initial_margin = contract.get("margin", 5000) * pos.quantity
            maintenance_margin = initial_margin * maintenance_ratio
            
            # 当前持仓盈亏
            if pos.profit < -initial_margin * (1 - maintenance_ratio):
                margin_calls.append({
                    "code": pos.code,
                    "direction": pos.direction,
                    "quantity": pos.quantity,
                    "required_margin": maintenance_margin,
                    "current_profit": pos.profit
                })
        
        return margin_calls
    
    def update_peak(self, engine):
        """更新历史最高资金"""
        current_assets = engine.portfolio.total_assets
        if current_assets > self.peak_assets:
            self.peak_assets = current_assets
    
    def get_max_drawdown(self, engine) -> float:
        """计算当前回撤"""
        if self.peak_assets == 0:
            return 0
        
        current_assets = engine.portfolio.total_assets
        drawdown = (self.peak_assets - current_assets) / self.peak_assets * 100
        return drawdown
    
    def reset_daily(self, date: str):
        """重置每日统计"""
        self.daily_loss = 0
        self.last_trade_date = date
        self.circuit_broken = False
        self.circuit_reason = ""
        
        logger.info(f"风控统计已重置: {date}")
    
    def get_risk_status(self, engine) -> dict:
        """获取风控状态"""
        return {
            "daily_loss": self.daily_loss,
            "peak_assets": self.peak_assets,
            "current_drawdown": self.get_max_drawdown(engine),
            "circuit_broken": self.circuit_broken,
            "circuit_reason": self.circuit_reason,
            "positions_count": len(engine.portfolio.positions),
            "available_margin": engine.portfolio.available_margin
        }
