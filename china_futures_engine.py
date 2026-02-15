#!/usr/bin/env python3
"""
中国期货市场 - 量化交易模拟系统
China Futures Market - Quantitative Trading Simulation System
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

# ============ 配置 ============
CONFIG = {
    "market": "china_futures",
    "initial_capital": 500000,  # 初始资金 50万 (期货门槛较低)
    "margin_rate": 0.10,        # 保证金比例 10% (螺纹钢为例)
    "commission_rate": 0.0001,  # 手续费万分之一
    "max_position": 0.5,        # 最大仓位50%
    "max_loss_per_trade": 0.03, # 单笔最大亏损3%
    "stop_loss_pct": 0.02,      # 止损线2%
}

# 期货合约配置 (示例)
FUTURES_CONTRACTS = {
    "rb": {"name": "螺纹钢", "margin": 5000, "tick": 1, "multiplier": 10},
    "i":  {"name": "铁矿石", "margin": 10000, "tick": 0.5, "multiplier": 100},
    "j":  {"name": "焦炭", "margin": 10000, "tick": 0.5, "multiplier": 100},
    "jm": {"name": "焦煤", "margin": 8000, "tick": 0.5, "multiplier": 60},
    "au": {"name": "黄金", "margin": 40000, "tick": 0.05, "multiplier": 1000},
    "ag": {"name": "白银", "margin": 8000, "tick": 1, "multiplier": 15},
    "cu": {"name": "铜", "margin": 20000, "tick": 10, "multiplier": 5},
    "al": {"name": "铝", "margin": 5000, "tick": 5, "multiplier": 5},
}

# ============ 数据类 ============
@dataclass
class FuturesTrade:
    """期货交易记录"""
    date: str
    code: str
    name: str
    action: str      # open/close
    direction: str  # long/short
    price: float
    quantity: int   # 手数
    commission: float

@dataclass
class FuturesPosition:
    """期货持仓"""
    code: str
    name: str
    direction: str  # long/short
    quantity: int   # 手数
    avg_price: float
    current_price: float = 0
    
    @property
    def margin(self) -> float:
        """保证金"""
        contract = FUTURES_CONTRACTS.get(self.code, {})
        return contract.get("margin", 5000) * self.quantity
    
    @property
    def market_value(self) -> float:
        """合约价值"""
        contract = FUTURES_CONTRACTS.get(self.code, {})
        return self.current_price * contract.get("multiplier", 10) * self.quantity
    
    @property
    def profit(self) -> float:
        """盈亏"""
        contract = FUTURES_CONTRACTS.get(self.code, {})
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

@dataclass
class FuturesPortfolio:
    """期货投资组合"""
    cash: float
    positions: Dict[str, FuturesPosition]
    trades: List[FuturesTrade]
    frozen_margin: float = 0  # 冻结保证金
    
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


# ============ 期货交易引擎 ============
class FuturesEngine:
    def __init__(self, initial_capital: float = None):
        self.config = CONFIG.copy()
        if initial_capital:
            self.config["initial_capital"] = initial_capital
            
        self.portfolio = FuturesPortfolio(
            cash=self.config["initial_capital"],
            positions={},
            trades=[]
        )
        
    def open_position(self, code: str, direction: str, price: float, quantity: int, date: str) -> bool:
        """开仓"""
        if code not in FUTURES_CONTRACTS:
            print(f"未知合约: {code}")
            return False
            
        contract = FUTURES_CONTRACTS[code]
        margin_required = contract["margin"] * quantity
        commission = price * contract["multiplier"] * quantity * self.config["commission_rate"]
        total_cost = margin_required + commission
        
        if total_cost > self.portfolio.cash:
            print(f"保证金不足，无法开仓 {code}")
            return False
            
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
            total_cost_basis = pos.avg_price * pos.quantity + price * quantity
            pos.avg_price = total_cost_basis / total_qty
            pos.quantity = total_qty
        else:
            self.portfolio.positions[pos_key] = FuturesPosition(
                code=code,
                name=contract["name"],
                direction=direction,
                quantity=quantity,
                avg_price=price
            )
            
        # 记录交易
        self.portfolio.trades.append(FuturesTrade(
            date=date,
            code=code,
            name=contract["name"],
            action="open",
            direction=direction,
            price=price,
            quantity=quantity,
            commission=commission
        ))
        
        action = "做多" if direction == "long" else "做空"
        print(f"[开仓] {code} {contract['name']} {action} @ {price} x {quantity}手")
        return True
    
    def close_position(self, code: str, direction: str, price: float, quantity: int, date: str) -> bool:
        """平仓"""
        pos_key = f"{code}_{direction}"
        if pos_key not in self.portfolio.positions:
            print(f"没有持仓 {code} {direction}")
            return False
            
        pos = self.portfolio.positions[pos_key]
        if pos.quantity < quantity:
            print(f"持仓不足，无法平仓 {code}")
            return False
            
        contract = FUTURES_CONTRACTS[code]
        
        # 计算盈亏
        if direction == "long":
            profit = (price - pos.avg_price) * contract["multiplier"] * quantity
        else:
            profit = (pos.avg_price - price) * contract["multiplier"] * quantity
            
        commission = price * contract["multiplier"] * quantity * self.config["commission_rate"]
        net_profit = profit - commission
        
        # 释放保证金
        margin_return = contract["margin"] * quantity
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
        self.portfolio.trades.append(FuturesTrade(
            date=date,
            code=code,
            name=contract["name"],
            action="close",
            direction=direction,
            price=price,
            quantity=quantity,
            commission=commission
        ))
        
        profit_str = f"+{net_profit:.2f}" if net_profit >= 0 else f"{net_profit:.2f}"
        print(f"[平仓] {code} @ {price} x {quantity}手, 盈亏: {profit_str}")
        return True
    
    def update_prices(self, prices: Dict[str, float]):
        """更新价格"""
        for code, price in prices.items():
            for pos_key, pos in self.portfolio.positions.items():
                if pos.code == code:
                    pos.current_price = price
    
    def check_stop_loss(self) -> List[dict]:
        """检查止损"""
        stop_signals = []
        for pos_key, pos in self.portfolio.positions.items():
            if pos.profit_pct < -self.config["stop_loss_pct"] * 100:
                stop_signals.append({
                    "code": pos.code,
                    "direction": pos.direction,
                    "quantity": pos.quantity,
                    "profit_pct": pos.profit_pct
                })
        return stop_signals
    
    def get_status(self) -> dict:
        """获取账户状态"""
        positions_data = {}
        for k, v in self.portfolio.positions.items():
            pos_dict = asdict(v)
            pos_dict['profit'] = v.profit  # 添加计算属性
            positions_data[k] = pos_dict
        return {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_assets": self.portfolio.total_assets,
            "cash": self.portfolio.cash,
            "frozen_margin": self.portfolio.frozen_margin,
            "available_margin": self.portfolio.available_margin,
            "total_profit": self.portfolio.total_profit,
            "profit_pct": (self.portfolio.total_assets - self.config["initial_capital"]) / self.config["initial_capital"] * 100,
            "positions": positions_data
        }
    
    def print_status(self):
        """打印账户状态"""
        status = self.get_status()
        print("\n" + "="*60)
        print(f"📊 期货账户状态 - {status['date']}")
        print("="*60)
        print(f"总资产: ¥{status['total_assets']:,.2f}")
        print(f"可用资金: ¥{status['cash']:,.2f}")
        print(f"冻结保证金: ¥{status['frozen_margin']:,.2f}")
        print(f"总盈亏: ¥{status['total_profit']:,.2f} ({status['profit_pct']:.2f}%)")
        print("-"*60)
        print("持仓:")
        for key, pos in status['positions'].items():
            direction_str = "多" if pos['direction'] == "long" else "空"
            print(f"  {pos['code']} {pos['name']}: {direction_str} {pos['quantity']}手 @ {pos['current_price']:.2f} (均价:{pos['avg_price']:.2f}, 盈亏:{pos['profit']:.2f})")
        print("="*60 + "\n")


# ============ 信号生成 ============
def generate_signals(engine: FuturesEngine) -> dict:
    """生成交易信号"""
    signals = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "market": "china_futures",
        "signals": [],
        "positions": [],
        "portfolio": {
            "total_assets": engine.portfolio.total_assets,
            "cash": engine.portfolio.cash,
            "total_profit": engine.portfolio.total_profit,
            "profit_pct": (engine.portfolio.total_assets - engine.config["initial_capital"]) / engine.config["initial_capital"] * 100
        }
    }
    
    # 检查现有持仓
    for pos_key, pos in engine.portfolio.positions.items():
        signals["positions"].append({
            "code": pos.code,
            "name": pos.name,
            "direction": pos.direction,
            "quantity": pos.quantity,
            "avg_price": pos.avg_price,
            "current_price": pos.current_price,
            "profit": pos.profit,
            "profit_pct": pos.profit_pct
        })
        
        # 止损检查
        if pos.profit_pct < -2.0:
            signals["signals"].append({
                "type": "stop_loss",
                "code": pos.code,
                "direction": pos.direction,
                "action": "close",
                "reason": f"止损触发 - 亏损{pos.profit_pct:.2f}%"
            })
    
    # 生成开仓信号 (示例策略)
    # 基于模拟价格生成信号
    if engine.portfolio.available_margin > 100000:
        # 建议做多螺纹钢
        signals["signals"].append({
            "type": "long",
            "code": "rb",
            "name": "螺纹钢",
            "action": "open",
            "direction": "long",
            "suggested_price": 4050,
            "quantity": 3,
            "reason": "突破关键阻力位，建议做多"
        })
    
    return signals


# ============ 主函数 ============
def main():
    """主函数 - 演示"""
    engine = FuturesEngine(initial_capital=500000)
    
    # 模拟开仓
    engine.open_position("rb", "long", 4000, 5, "2026-02-15")  # 螺纹钢做多
    engine.open_position("i", "short", 800, 2, "2026-02-15")   # 铁矿石做空
    
    # 模拟更新价格
    engine.update_prices({
        "rb": 4050,
        "i": 780
    })
    
    # 打印状态
    engine.print_status()
    
    # 检查止损
    stops = engine.check_stop_loss()
    if stops:
        print(f"⚠️ 触发止损: {stops}")
    
    # 生成交易信号
    signals = generate_signals(engine)
    
    # 保存信号到文件
    output_path = "signals.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(signals, f, indent=2, ensure_ascii=False)
    
    print(f"✅ 信号已生成并保存到 {output_path}")
    print(f"\n📈 信号内容:")
    print(json.dumps(signals, indent=2, ensure_ascii=False))
    
    # 保存状态
    os.makedirs("data", exist_ok=True)
    with open("data/futures_portfolio.json", "w") as f:
        json.dump(engine.get_status(), f, indent=2, ensure_ascii=False)
    
    print("✅ 期货交易系统初始化完成")


if __name__ == "__main__":
    main()
