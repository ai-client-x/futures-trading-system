#!/usr/bin/env python3
"""
交易引擎
Trading Engine
"""

import logging
from datetime import datetime
from typing import Dict, Optional, List

from ..models import Trade, Position, Portfolio, Order
from ..config import config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TradingEngine:
    """
    交易引擎
    负责: 订单执行、持仓管理、资金管理
    """
    
    def __init__(self, initial_capital: float = None):
        self.config = config
        self.initial_capital = initial_capital or self.config.initial_capital
        
        self.portfolio = Portfolio(
            cash=self.initial_capital,
            positions={},
            trades=[],
            initial_capital=self.initial_capital
        )
        
        self.orders: Dict[str, Order] = {}  # order_id -> Order
        self.order_counter = 0
        
        logger.info(f"交易引擎初始化完成，初始资金: ¥{self.initial_capital:,.2f}")
    
    def calculate_commission(self, amount: float, action: str) -> float:
        """计算交易佣金"""
        commission = amount * self.config.commission_rate
        # 佣金最低5元
        commission = max(commission, 5)
        
        # 卖出时收取印花税
        if action == "sell":
            commission += amount * self.config.stamp_tax
        
        # 滑点成本
        commission += amount * self.config.slippage
        
        return commission
    
    def can_buy(self, code: str, price: float, quantity: int) -> tuple:
        """
        检查是否可以买入
        返回: (can_buy: bool, reason: str)
        """
        # 检查资金
        cost = price * quantity
        commission = self.calculate_commission(cost, "buy")
        total_cost = cost + commission
        
        if total_cost > self.portfolio.cash:
            return False, "资金不足"
        
        # 检查仓位上限
        max_position_value = self.initial_capital * self.config.max_position
        current_position_value = self.portfolio.total_assets - self.portfolio.cash
        
        if code not in self.portfolio.positions:
            if current_position_value + cost > self.portfolio.total_assets * self.config.max_position:
                return False, f"超过最大仓位 {self.config.max_position * 100}%"
        
        return True, ""
    
    def can_sell(self, code: str, quantity: int) -> tuple:
        """
        检查是否可以卖出
        返回: (can_sell: bool, reason: str)
        """
        if code not in self.portfolio.positions:
            return False, f"没有持仓 {code}"
        
        pos = self.portfolio.positions[code]
        if pos.quantity < quantity:
            return False, f"持仓不足 (持有: {pos.quantity}, 卖出: {quantity})"
        
        return True, ""
    
    def buy(self, code: str, name: str, price: float, quantity: int, 
            date: str = None, check_risk: bool = True) -> bool:
        """
        买入股票
        
        Args:
            code: 股票代码
            name: 股票名称
            price: 价格
            quantity: 数量（手）
            date: 交易日期
            check_risk: 是否进行风控检查
        
        Returns:
            bool: 是否买入成功
        """
        date = date or datetime.now().strftime("%Y-%m-%d")
        
        # 风控检查
        if check_risk:
            can_buy, reason = self.can_buy(code, price, quantity)
            if not can_buy:
                logger.warning(f"风控拦截买入 {code}: {reason}")
                return False
        
        # 计算成本
        cost = price * quantity
        commission = self.calculate_commission(cost, "buy")
        total_cost = cost + commission
        
        # 扣除资金
        self.portfolio.cash -= total_cost
        
        # 更新持仓
        if code in self.portfolio.positions:
            pos = self.portfolio.positions[code]
            total_qty = pos.quantity + quantity
            total_cost_basis = pos.avg_cost * pos.quantity + cost
            pos.avg_cost = total_cost_basis / total_qty
            pos.quantity = total_qty
        else:
            self.portfolio.positions[code] = Position(
                code=code,
                name=name,
                quantity=quantity,
                avg_cost=price,
                entry_date=date
            )
        
        # 记录交易
        trade = Trade(
            date=date,
            code=code,
            name=name,
            action="buy",
            price=price,
            quantity=quantity,
            commission=commission,
            timestamp=datetime.now().isoformat()
        )
        self.portfolio.trades.append(trade)
        
        logger.info(f"[买入] {code} {name} @ ¥{price} x {quantity}, 手续费: ¥{commission:.2f}")
        return True
    
    def sell(self, code: str, price: float, quantity: int, 
             date: str = None, check_risk: bool = True) -> bool:
        """
        卖出股票
        
        Args:
            code: 股票代码
            price: 价格
            quantity: 数量
            date: 交易日期
            check_risk: 是否进行风控检查
        
        Returns:
            bool: 是否卖出成功
        """
        date = date or datetime.now().strftime("%Y-%m-%d")
        
        # 风控检查
        if check_risk:
            can_sell, reason = self.can_sell(code, quantity)
            if not can_sell:
                logger.warning(f"风控拦截卖出 {code}: {reason}")
                return False
        
        if code not in self.portfolio.positions:
            logger.warning(f"没有持仓 {code}")
            return False
        
        pos = self.portfolio.positions[code]
        
        # 计算收入
        revenue = price * quantity
        commission = self.calculate_commission(revenue, "sell")
        net_revenue = revenue - commission
        
        # 收回资金
        self.portfolio.cash += net_revenue
        
        # 更新持仓
        pos.quantity -= quantity
        if pos.quantity == 0:
            del self.portfolio.positions[code]
        
        # 记录交易
        trade = Trade(
            date=date,
            code=code,
            name=pos.name if hasattr(pos, 'name') else code,
            action="sell",
            price=price,
            quantity=quantity,
            commission=commission,
            timestamp=datetime.now().isoformat()
        )
        self.portfolio.trades.append(trade)
        
        logger.info(f"[卖出] {code} @ ¥{price} x {quantity}, 手续费: ¥{commission:.2f}")
        return True
    
    def close_position(self, code: str, price: float, date: str = None) -> bool:
        """清仓"""
        if code in self.portfolio.positions:
            pos = self.portfolio.positions[code]
            return self.sell(code, price, pos.quantity, date)
        return False
    
    def update_prices(self, prices: Dict[str, float]):
        """更新持仓价格"""
        for code, price in prices.items():
            if code in self.portfolio.positions:
                self.portfolio.positions[code].current_price = price
    
    def get_position(self, code: str) -> Optional[Position]:
        """获取持仓"""
        return self.portfolio.positions.get(code)
    
    def get_status(self) -> dict:
        """获取账户状态"""
        return self.portfolio.to_dict()
    
    def print_status(self, date: str = None):
        """打印账户状态"""
        status = self.get_status()
        date = date or datetime.now().strftime("%Y-%m-%d")
        
        print("\n" + "="*60)
        print(f"📊 账户状态 - {date}")
        print("="*60)
        print(f"总资产:   ¥{status['total_assets']:>12,.2f}")
        print(f"现金:     ¥{status['cash']:>12,.2f}")
        print(f"持仓市值: ¥{status['position_value']:>12,.2f}")
        print(f"总盈亏:   ¥{status['total_profit']:>12,.2f} ({status['profit_pct']:+.2f}%)")
        print(f"持仓比例: {status['position_ratio']*100:>11.1f}%")
        print("-"*60)
        print("持仓明细:")
        
        if status['positions']:
            for code, pos in status['positions'].items():
                profit = (pos['current_price'] - pos['avg_cost']) * pos['quantity']
                profit_pct = (pos['current_price'] - pos['avg_cost']) / pos['avg_cost'] * 100
                print(f"  {code} {pos['name']}: {pos['quantity']}股")
                print(f"    成本: ¥{pos['avg_cost']:.2f} → 现价: ¥{pos['current_price']:.2f}")
                print(f"    盈亏: ¥{profit:+.2f} ({profit_pct:+.2f}%)")
        else:
            print("  (空仓)")
        
        print("="*60 + "\n")
    
    def reset(self):
        """重置账户"""
        self.portfolio = Portfolio(
            cash=self.initial_capital,
            positions={},
            trades=[],
            initial_capital=self.initial_capital
        )
        logger.info("账户已重置")
