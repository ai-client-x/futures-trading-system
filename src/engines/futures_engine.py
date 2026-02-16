#!/usr/bin/env python3
"""
期货交易引擎
Futures Trading Engine
"""

import logging
from datetime import datetime
from typing import Dict, Optional, List, Tuple

from .models import FuturesTrade, FuturesPosition, FuturesPortfolio, Signal
from .config import config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FuturesEngine:
    """
    期货交易引擎
    负责: 订单执行、持仓管理、保证金管理
    """
    
    def __init__(self, initial_capital: float = None):
        self.config = config
        self.initial_capital = initial_capital or self.config.initial_capital
        
        self.portfolio = FuturesPortfolio(
            cash=self.initial_capital,
            positions={},
            trades=[],
            frozen_margin=0,
            initial_capital=self.initial_capital
        )
        
        logger.info(f"期货交易引擎初始化完成，初始资金: ¥{self.initial_capital:,.2f}")
    
    def calculate_commission(self, code: str, price: float, quantity: int, action: str) -> float:
        """计算手续费"""
        contract = self.config.contracts.get(code, {})
        multiplier = contract.get("multiplier", 10)
        
        # 手续费 = 价格 * 乘数 * 手数 * 手续费率
        commission = price * multiplier * quantity * self.config.commission_rate
        
        # 滑点成本
        slippage_cost = price * multiplier * quantity * self.config.slippage
        
        return commission + slippage_cost
    
    def calculate_margin(self, code: str, quantity: int) -> float:
        """计算保证金"""
        contract = self.config.contracts.get(code, {})
        return contract.get("margin", 5000) * quantity
    
    def can_open(self, code: str, direction: str, price: float, quantity: int) -> Tuple[bool, str]:
        """
        检查是否可以开仓
        返回: (can_open: bool, reason: str)
        """
        if code not in self.config.contracts:
            return False, f"未知合约: {code}"
        
        # 检查保证金
        margin_required = self.calculate_margin(code, quantity)
        commission = self.calculate_commission(code, price, quantity, "open")
        total_cost = margin_required + commission
        
        if total_cost > self.portfolio.cash:
            return False, f"保证金不足 (需要: {total_cost:.2f}, 可用: {self.portfolio.cash:.2f})"
        
        # 检查持仓数量上限
        if len(self.portfolio.positions) >= self.config.max_positions:
            return False, f"已达到最大持仓数 {self.config.max_positions}"
        
        # 检查是否已有该品种持仓
        pos_key = f"{code}_{direction}"
        if pos_key in self.portfolio.positions:
            return False, f"已有{direction}持仓"
        
        return True, ""
    
    def can_close(self, code: str, direction: str, quantity: int) -> Tuple[bool, str]:
        """检查是否可以平仓"""
        pos_key = f"{code}_{direction}"
        
        if pos_key not in self.portfolio.positions:
            return False, f"没有持仓 {code} {direction}"
        
        pos = self.portfolio.positions[pos_key]
        if pos.quantity < quantity:
            return False, f"持仓不足 (持有: {pos.quantity}, 平仓: {quantity})"
        
        return True, ""
    
    def open_position(self, code: str, direction: str, price: float, 
                      quantity: int, date: str = None, check_risk: bool = True) -> bool:
        """
        开仓
        
        Args:
            code: 合约代码
            direction: 方向 (long/short)
            price: 价格
            quantity: 手数
            date: 交易日期
            check_risk: 是否进行风控检查
        
        Returns:
            bool: 是否开仓成功
        """
        date = date or datetime.now().strftime("%Y-%m-%d")
        
        # 风控检查
        if check_risk:
            can_open, reason = self.can_open(code, direction, price, quantity)
            if not can_open:
                logger.warning(f"风控拦截开仓 {code} {direction}: {reason}")
                return False
        
        contract = self.config.contracts.get(code, {})
        
        # 计算保证金和手续费
        margin_required = self.calculate_margin(code, quantity)
        commission = self.calculate_commission(code, price, quantity, "open")
        
        # 冻结保证金
        self.portfolio.cash -= margin_required
        self.portfolio.frozen_margin += margin_required
        # 扣除手续费
        self.portfolio.cash -= commission
        
        # 更新持仓
        pos_key = f"{code}_{direction}"
        if pos_key in self.portfolio.positions:
            pos = self.portfolio.positions[pos_key]
            total_qty = pos.quantity + quantity
            total_cost = pos.avg_price * pos.quantity + price * quantity
            pos.avg_price = total_cost / total_qty
            pos.quantity = total_qty
        else:
            self.portfolio.positions[pos_key] = FuturesPosition(
                code=code,
                name=contract.get("name", code),
                direction=direction,
                quantity=quantity,
                avg_price=price,
                entry_date=date,
                contracts=self.config.contracts
            )
        
        # 记录交易
        trade = FuturesTrade(
            date=date,
            code=code,
            name=contract.get("name", code),
            action="open",
            direction=direction,
            price=price,
            quantity=quantity,
            commission=commission,
            timestamp=datetime.now().isoformat()
        )
        self.portfolio.trades.append(trade)
        
        action_str = "做多" if direction == "long" else "做空"
        logger.info(f"[开仓] {code} {contract.get('name', '')} {action_str} @ ¥{price} x {quantity}手, 手续费: ¥{commission:.2f}")
        return True
    
    def close_position(self, code: str, direction: str, price: float, 
                       quantity: int, date: str = None, check_risk: bool = True) -> bool:
        """
        平仓
        
        Args:
            code: 合约代码
            direction: 方向 (long/short)
            price: 价格
            quantity: 手数
            date: 交易日期
            check_risk: 是否进行风控检查
        
        Returns:
            bool: 是否平仓成功
        """
        date = date or datetime.now().strftime("%Y-%m-%d")
        
        # 风控检查
        if check_risk:
            can_close, reason = self.can_close(code, direction, quantity)
            if not can_close:
                logger.warning(f"风控拦截平仓 {code} {direction}: {reason}")
                return False
        
        pos_key = f"{code}_{direction}"
        if pos_key not in self.portfolio.positions:
            logger.warning(f"没有持仓 {code} {direction}")
            return False
        
        pos = self.portfolio.positions[pos_key]
        contract = self.config.contracts.get(code, {})
        multiplier = contract.get("multiplier", 10)
        
        # 计算盈亏
        if direction == "long":
            profit = (price - pos.avg_price) * multiplier * quantity
        else:
            profit = (pos.avg_price - price) * multiplier * quantity
        
        # 计算手续费
        commission = self.calculate_commission(code, price, quantity, "close")
        net_profit = profit - commission
        
        # 释放保证金
        margin_return = self.calculate_margin(code, quantity)
        self.portfolio.frozen_margin -= margin_return
        self.portfolio.cash += margin_return
        
        # 扣除手续费
        self.portfolio.cash -= commission
        
        # 加上盈利/扣除亏损
        self.portfolio.cash += net_profit
        
        # 更新持仓
        pos.quantity -= quantity
        if pos.quantity == 0:
            del self.portfolio.positions[pos_key]
        
        # 记录交易
        trade = FuturesTrade(
            date=date,
            code=code,
            name=contract.get("name", code),
            action="close",
            direction=direction,
            price=price,
            quantity=quantity,
            commission=commission,
            timestamp=datetime.now().isoformat()
        )
        self.portfolio.trades.append(trade)
        
        profit_str = f"+{net_profit:.2f}" if net_profit >= 0 else f"{net_profit:.2f}"
        logger.info(f"[平仓] {code} @ ¥{price} x {quantity}手, 盈亏: {profit_str}")
        return True
    
    def close_all(self, code: str, price: float, date: str = None) -> List[bool]:
        """平掉所有该品种持仓"""
        results = []
        date = date or datetime.now().strftime("%Y-%m-%d")
        
        # 找出所有持仓
        to_close = []
        for pos_key, pos in list(self.portfolio.positions.items()):
            if pos.code == code:
                to_close.append((pos.direction, pos.quantity))
        
        # 平仓
        for direction, quantity in to_close:
            result = self.close_position(code, direction, price, quantity, date)
            results.append(result)
        
        return results
    
    def update_prices(self, prices: Dict[str, float]):
        """更新持仓价格"""
        for code, price in prices.items():
            for pos_key, pos in self.portfolio.positions.items():
                if pos.code == code:
                    pos.current_price = price
    
    def get_position(self, code: str = None, direction: str = None) -> Optional[FuturesPosition]:
        """获取持仓"""
        if code and direction:
            return self.portfolio.positions.get(f"{code}_{direction}")
        elif code:
            # 返回该品种所有持仓
            return {k: v for k, v in self.portfolio.positions.items() if v.code == code}
        return None
    
    def get_status(self) -> dict:
        """获取账户状态"""
        return self.portfolio.to_dict()
    
    def print_status(self, date: str = None):
        """打印账户状态"""
        status = self.get_status()
        date = date or datetime.now().strftime("%Y-%m-%d")
        
        print("\n" + "="*60)
        print(f"📊 期货账户状态 - {date}")
        print("="*60)
        print(f"总资产:     ¥{status['total_assets']:>12,.2f}")
        print(f"可用资金:   ¥{status['cash']:>12,.2f}")
        print(f"冻结保证金: ¥{status['frozen_margin']:>12,.2f}")
        print(f"总盈亏:     ¥{status['total_profit']:>12,.2f} ({status['profit_pct']:+.2f}%)")
        print("-"*60)
        print("持仓:")
        
        if status['positions']:
            for key, pos in status['positions'].items():
                direction_str = "多" if pos['direction'] == "long" else "空"
                print(f"  {pos['code']} {pos['name']}: {direction_str} {pos['quantity']}手")
                print(f"    均价: ¥{pos['avg_price']:.2f} → 现价: ¥{pos['current_price']:.2f}")
                print(f"    盈亏: ¥{pos['profit']:+.2f} ({pos['profit_pct']:+.2f}%)")
        else:
            print("  (空仓)")
        
        print("="*60 + "\n")
    
    def reset(self):
        """重置账户"""
        self.portfolio = FuturesPortfolio(
            cash=self.initial_capital,
            positions={},
            trades=[],
            frozen_margin=0,
            initial_capital=self.initial_capital
        )
        logger.info("账户已重置")
