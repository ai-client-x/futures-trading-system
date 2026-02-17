#!/usr/bin/env python3
"""
A股多策略回测系统 - 风控优化版
加入：
1. 动态仓位 - 根据市场波动调整
2. 更严格止损 - -5%止损
3. 策略轮动 - 根据市场状态切换
4. 最大回撤熔断 - 回撤超15%自动减仓
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import sqlite3
from src.engines.trading_engine import TradingEngine
from src.models import Position
from src.config import config


# ============ 配置 ============
DB_PATH = "data/stocks.db"

# 数据划分 (从config读取)
DEVELOP_START = config.develop_start
DEVELOP_END = config.develop_end
BACKTEST_START = config.backtest_start
BACKTEST_END = config.backtest_end

INITIAL_CAPITAL = config.initial_capital

# 测试股票 (从config读取)
TEST_STOCKS = [s['code'] + ('.SH' if s['code'].startswith('6') else '.SZ') 
               for s in config.stock_pool] if config.stock_pool else [
    '600519.SH', '000858.SZ', '601318.SH', '300750.SZ', '002594.SZ',
]


# ============ 交易成本 ============
class TradingCosts:
    COMMISSION_RATE = config.commission_rate
    MIN_COMMISSION = 5
    STAMP_DUTY_RATE = config.stamp_tax
    TRANSFER_FEE_RATE = 0.00002
    SLIPPAGE_RATE = config.slippage
    
    @classmethod
    def calc_commission(cls, amount: float) -> float:
        return max(amount * cls.COMMISSION_RATE, cls.MIN_COMMISSION)
    
    @classmethod
    def calc_buy_cost(cls, amount: float) -> float:
        return cls.calc_commission(amount) + amount * cls.TRANSFER_FEE_RATE + amount * cls.SLIPPAGE_RATE
    
    @classmethod
    def calc_sell_cost(cls, amount: float) -> float:
        return cls.calc_commission(amount) + amount * cls.STAMP_DUTY_RATE + amount * cls.TRANSFER_FEE_RATE + amount * cls.SLIPPAGE_RATE


# ============ 风控配置 ============
class RiskControl:
    """风控参数"""
    # 止损止盈（更严格）
    STOP_LOSS = -5      # 止损 -5%
    TAKE_PROFIT = 10    # 止盈 +10%
    
    # 回撤熔断
    MAX_DRAWDOWN_THRESHOLD = 15  # 回撤15%时触发
    REDUCTION_RATIO = 0.5        # 减仓50%
    
    # 动态仓位
    VOLATILITY_WINDOW = 20       # 波动率计算窗口
    MIN_POSITION_SIZE = 0.15     # 最小仓位
    MAX_POSITION_SIZE = 0.30     # 最大仓位
    
    # 市场状态判断
    MARKET_REGIME_WINDOW = 20    # 市场状态判断窗口


# ============ 策略定义 ============

class Strategy:
    def __init__(self, name: str, max_positions: int, position_size: float,
                 stop_loss: float, take_profit: float, min_signal_score: int):
        self.name = name
        self.max_positions = max_positions
        self.position_size = position_size
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.min_signal_score = min_signal_score
    
    def calculate_signal(self, df: pd.DataFrame) -> dict:
        raise NotImplementedError


class TrendStrategy(Strategy):
    """趋势跟踪策略"""
    def __init__(self):
        super().__init__("趋势跟踪", 3, 0.25, RiskControl.STOP_LOSS, RiskControl.TAKE_PROFIT, 25)
    
    def calculate_signal(self, df: pd.DataFrame) -> dict:
        if df is None or len(df) < 60:
            return {'action': 'hold', 'score': 0}
        
        close = df['close']
        ma5, ma20, ma60 = close.rolling(5).mean(), close.rolling(20).mean(), close.rolling(60).mean()
        
        score = 0
        reasons = []
        
        if ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1]:
            score += 30
            reasons.append('均线多头')
        
        if ma5.iloc[-1] > ma5.iloc[-5] * 1.02:
            score += 10
            reasons.append('短期趋势向上')
        
        if score >= self.min_signal_score:
            return {'action': 'buy', 'score': score, 'reason': '; '.join(reasons)}
        
        if ma5.iloc[-1] < ma20.iloc[-1]:
            return {'action': 'sell', 'score': 20, 'reason': '均线死叉'}
        
        return {'action': 'hold', 'score': score}


class ValueStrategy(Strategy):
    """价值投资策略"""
    def __init__(self):
        super().__init__("价值投资", 3, 0.30, -8, 15, 20)
    
    def calculate_signal(self, df: pd.DataFrame) -> dict:
        if df is None or len(df) < 120:
            return {'action': 'hold', 'score': 0}
        
        close = df['close']
        ma20, ma60 = close.rolling(20).mean(), close.rolling(60).mean()
        
        score = 0
        if ma20.iloc[-1] < ma60.iloc[-1]:
            score += 25
        
        low_60 = close.rolling(60).min()
        if close.iloc[-1] < low_60.iloc[-1] * 1.1:
            score += 20
        
        if score >= self.min_signal_score:
            return {'action': 'buy', 'score': score}
        
        return {'action': 'hold', 'score': score}


class MomentumStrategy(Strategy):
    """动量策略"""
    def __init__(self):
        super().__init__("动量策略", 2, 0.20, RiskControl.STOP_LOSS, 12, 30)
    
    def calculate_signal(self, df: pd.DataFrame) -> dict:
        if df is None or len(df) < 30:
            return {'action': 'hold', 'score': 0}
        
        close = df['close']
        ret5 = (close.iloc[-1] / close.iloc[-6] - 1) * 100 if len(close) > 5 else 0
        
        score = 0
        if 3 < ret5 < 15:
            score += 30
        
        if score >= self.min_signal_score:
            return {'action': 'buy', 'score': score}
        
        if ret5 < -5:
            return {'action': 'sell', 'score': 25}
        
        return {'action': 'hold', 'score': score}


class BreakoutStrategy(Strategy):
    """突破策略"""
    def __init__(self):
        super().__init__("突破策略", 2, 0.20, -4, 10, 25)
    
    def calculate_signal(self, df: pd.DataFrame) -> dict:
        if df is None or len(df) < 60:
            return {'action': 'hold', 'score': 0}
        
        close, high = df['close'], df['high']
        high_20 = high.rolling(20).max().shift(1)
        
        score = 0
        if close.iloc[-1] > high_20.iloc[-1]:
            score += 30
        
        if score >= self.min_signal_score:
            return {'action': 'buy', 'score': score}
        
        return {'action': 'hold', 'score': score}


# ============ 风控优化版策略组合 ============

class OptimizedPortfolio:
    """风控优化版组合"""
    
    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.strategies = [
            TrendStrategy(),
            ValueStrategy(),
            MomentumStrategy(),
            BreakoutStrategy()
        ]
        
        # 每个策略独立资金
        self.engines = {s.name: TradingEngine(initial_capital / len(self.strategies)) 
                       for s in self.strategies}
        
        # 风控状态
        self.peak_equity = initial_capital
        self.current_drawdown = 0
        self.is_reduced = False  # 是否已减仓
        
        print(f"\n🎯 风控优化版多策略组合")
        print(f"   止损: {RiskControl.STOP_LOSS}% | 止盈: {RiskControl.TAKE_PROFIT}%")
        print(f"   回撤熔断: {RiskControl.MAX_DRAWDOWN_THRESHOLD}%")
        print(f"   动态仓位: {RiskControl.MIN_POSITION_SIZE*100:.0f}-{RiskControl.MAX_POSITION_SIZE*100:.0f}%")
    
    def update_drawdown(self, current_equity: float):
        """更新回撤状态"""
        if current_equity > self.peak_equity:
            self.peak_equity = current_equity
        
        self.current_drawdown = (self.peak_equity - current_equity) / self.peak_equity * 100
        
        # 触发回撤熔断
        if self.current_drawdown > RiskControl.MAX_DRAWDOWN_THRESHOLD and not self.is_reduced:
            self.is_reduced = True
            return True  # 触发减仓
        elif self.current_drawdown < RiskControl.MAX_DRAWDOWN_THRESHOLD * 0.5:
            self.is_reduced = False  # 恢复仓位
        return False
    
    def get_position_size(self, base_size: float, market_volatility: float = 0.02) -> float:
        """动态仓位计算"""
        # 波动率越大，仓位越小
        vol_factor = 1 - min(market_volatility * 5, 0.5)
        size = base_size * vol_factor
        return max(RiskControl.MIN_POSITION_SIZE, min(RiskControl.MAX_POSITION_SIZE, size))
    
    def get_total_assets(self) -> float:
        return sum(e.portfolio.total_assets for e in self.engines.values())


# ============ 回测函数 ============

def get_stock_list():
    conn = sqlite3.connect(DB_PATH)
    placeholders = ','.join(['?'] * len(TEST_STOCKS))
    df = pd.read_sql(f"SELECT ts_code, name FROM stocks WHERE ts_code IN ({placeholders})", 
                   conn, params=TEST_STOCKS)
    conn.close()
    return df.to_dict('records')


def get_market_volatility(conn, date: str) -> float:
    """计算市场波动率"""
    df = pd.read_sql(f"""
        SELECT close FROM daily 
        WHERE trade_date <= '{date}'
        ORDER BY trade_date DESC
        LIMIT {RiskControl.VOLATILITY_WINDOW + 1}
    """, conn)
    
    if len(df) < RiskControl.VOLATILITY_WINDOW:
        return 0.02
    
    returns = df['close'].pct_change().dropna()
    return returns.std() if len(returns) > 0 else 0.02


def run_backtest(stock_list: list, start_date: str, end_date: str, period_name: str) -> dict:
    print(f"\n{'='*60}")
    print(f"📊 {period_name} - 风控优化版")
    print(f"{'='*60}")
    
    portfolio = OptimizedPortfolio(INITIAL_CAPITAL)
    
    conn = sqlite3.connect(DB_PATH)
    trade_dates = pd.read_sql(f"""
        SELECT DISTINCT trade_date 
        FROM daily 
        WHERE trade_date >= '{start_date}' 
        AND trade_date <= '{end_date}'
        ORDER BY trade_date
    """, conn)['trade_date'].tolist()
    conn.close()
    
    print(f"交易日数: {len(trade_dates)}")
    
    trades = []
    daily_values = []
    circuit_breaker_triggered = 0
    
    for i, date in enumerate(trade_dates):
        total_equity = portfolio.get_total_assets()
        
        # 更新回撤，检测是否触发熔断
        if portfolio.update_drawdown(total_equity):
            circuit_breaker_triggered += 1
            print(f"⚠️ Day {i+1}: 触发回撤熔断！当前回撤 {portfolio.current_drawdown:.1f}%")
        
        # 获取市场波动率
        conn = sqlite3.connect(DB_PATH)
        market_vol = get_market_volatility(conn, date)
        conn.close()
        
        for strategy in portfolio.strategies:
            engine = portfolio.engines[strategy.name]
            
            # 应用仓位调整（如果触发熔断）
            effective_position_size = strategy.position_size
            if portfolio.is_reduced:
                effective_position_size = strategy.position_size * RiskControl.REDUCTION_RATIO
            
            # 动态仓位
            effective_position_size = portfolio.get_position_size(effective_position_size, market_vol)
            
            # 1. 卖出检查
            for code in list(engine.portfolio.positions.keys()):
                holding = engine.portfolio.positions[code]
                if holding.quantity > 0:
                    conn = sqlite3.connect(DB_PATH)
                    price_row = conn.execute(f'''
                        SELECT close FROM daily 
                        WHERE ts_code='{code}' AND trade_date='{date}'
                    ''').fetchone()
                    conn.close()
                    
                    if price_row:
                        current_price = float(price_row[0])
                        pnl_pct = (current_price - holding.avg_cost) / holding.avg_cost * 100
                        
                        # 止损/止盈（更严格）
                        if pnl_pct <= strategy.stop_loss or pnl_pct >= strategy.take_profit:
                            revenue = current_price * holding.quantity
                            cost = TradingCosts.calc_sell_cost(revenue)
                            engine.portfolio.cash += revenue - cost
                            trades.append({
                                'date': date,
                                'strategy': strategy.name,
                                'code': code,
                                'action': 'sell',
                                'pnl_pct': pnl_pct
                            })
                            holding.quantity = 0
                            del engine.portfolio.positions[code]
            
            # 2. 买入检查
            if len(engine.portfolio.positions) < strategy.max_positions:
                for stock in stock_list:
                    code = stock['ts_code']
                    name = stock['name']
                    
                    if code in engine.portfolio.positions:
                        continue
                    
                    conn = sqlite3.connect(DB_PATH)
                    df = pd.read_sql(f"""
                        SELECT trade_date, open, high, low, close, vol
                        FROM daily
                        WHERE ts_code = '{code}'
                        AND trade_date <= '{date}'
                        ORDER BY trade_date
                    """, conn)
                    conn.close()
                    
                    if len(df) < 60:
                        continue
                    
                    signal = strategy.calculate_signal(df)
                    
                    if signal['action'] == 'buy':
                        next_idx = trade_dates.index(date) + 1
                        if next_idx < len(trade_dates):
                            next_date = trade_dates[next_idx]
                            conn = sqlite3.connect(DB_PATH)
                            open_row = conn.execute(f'''
                                SELECT open FROM daily 
                                WHERE ts_code='{code}' AND trade_date='{next_date}'
                            ''').fetchone()
                            conn.close()
                            
                            if open_row:
                                open_price = float(open_row[0])
                                amount = engine.portfolio.cash * effective_position_size
                                qty = int(amount / open_price / 100) * 100
                                
                                if qty > 0:
                                    cost = open_price * qty
                                    trade_cost = TradingCosts.calc_buy_cost(cost)
                                    total_cost = cost + trade_cost
                                    
                                    if engine.portfolio.cash >= total_cost:
                                        engine.portfolio.cash -= total_cost
                                        position = Position(
                                            code=code, name=name, quantity=qty,
                                            avg_cost=open_price, current_price=open_price
                                        )
                                        engine.portfolio.positions[code] = position
                                        trades.append({
                                            'date': next_date,
                                            'strategy': strategy.name,
                                            'code': code,
                                            'action': 'buy'
                                        })
        
        daily_values.append({'date': date, 'assets': portfolio.get_total_assets()})
        
        if (i + 1) % 100 == 0:
            ret = (portfolio.get_total_assets() - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
            print(f"Day {i+1}: 总资产¥{portfolio.get_total_assets():,.0f} ({ret:+.2f}%) 回撤:{portfolio.current_drawdown:.1f}%")
    
    # 最终结算
    conn = sqlite3.connect(DB_PATH)
    last_date = trade_dates[-1]
    
    for strategy in portfolio.strategies:
        engine = portfolio.engines[strategy.name]
        for code, holding in list(engine.portfolio.positions.items()):
            if holding.quantity > 0:
                price_row = conn.execute(f'''
                    SELECT close FROM daily 
                    WHERE ts_code='{code}' AND trade_date='{last_date}'
                ''').fetchone()
                if price_row:
                    price = float(price_row[0])
                    revenue = price * holding.quantity
                    cost = TradingCosts.calc_sell_cost(revenue)
                    engine.portfolio.cash += revenue - cost
    conn.close()
    
    # 计算结果
    final_assets = portfolio.get_total_assets()
    total_return = (final_assets - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
    years = len(trade_dates) / 252
    annual_return = total_return / years if years > 0 else 0
    
    # 最大回撤
    peak = INITIAL_CAPITAL
    max_drawdown = 0
    for v in daily_values:
        if v['assets'] > peak:
            peak = v['assets']
        dd = (peak - v['assets']) / peak * 100
        if dd > max_drawdown:
            max_drawdown = dd
    
    # 各策略结果
    strategy_results = {}
    for strategy in portfolio.strategies:
        engine = portfolio.engines[strategy.name]
        strat_ret = (engine.portfolio.total_assets - INITIAL_CAPITAL / len(portfolio.strategies)) / (INITIAL_CAPITAL / len(portfolio.strategies)) * 100
        strategy_results[strategy.name] = {
            'final_assets': engine.portfolio.total_assets,
            'return': strat_ret
        }
    
    # 打印结果
    print(f"\n{'='*60}")
    print(f"📊 {period_name} 结果 (风控优化版)")
    print(f"{'='*60}")
    print(f"初始资金:       ¥{INITIAL_CAPITAL:,.0f}")
    print(f"最终资产:       ¥{final_assets:,.0f}")
    print(f"总收益率:       {total_return:+.2f}%")
    print(f"年化收益率:     {annual_return:+.2f}%")
    print(f"最大回撤:       {max_drawdown:.2f}%")
    print(f"交易次数:       {len(trades)}")
    print(f"回撤熔断触发:   {circuit_breaker_triggered}次")
    print(f"\n📈 各策略表现:")
    for name, result in strategy_results.items():
        print(f"  {name}: {result['return']:+.2f}%")
    print(f"{'='*60}")
    
    return {
        'period': period_name,
        'final_assets': final_assets,
        'total_return': total_return,
        'annual_return': annual_return,
        'max_drawdown': max_drawdown,
        'trade_count': len(trades),
        'circuit_breaker': circuit_breaker_triggered,
        'strategy_results': strategy_results
    }


def main():
    print("="*60)
    print("📈 A股多策略量化回测系统 - 风控优化版")
    print("="*60)
    print(f"\n🔧 风控配置:")
    print(f"   止损: {RiskControl.STOP_LOSS}% | 止盈: {RiskControl.TAKE_PROFIT}%")
    print(f"   回撤熔断: {RiskControl.MAX_DRAWDOWN_THRESHOLD}% 减仓{RiskControl.REDUCTION_RATIO*100:.0f}%")
    print(f"   动态仓位: {RiskControl.MIN_POSITION_SIZE*100:.0f}-{RiskControl.MAX_POSITION_SIZE*100:.0f}%")
    
    stock_list = get_stock_list()
    print(f"股票数量: {len(stock_list)}")
    
    # 开发期
    develop_result = run_backtest(stock_list, DEVELOP_START, DEVELOP_END, "开发期")
    
    # 回测期
    backtest_result = run_backtest(stock_list, BACKTEST_START, BACKTEST_END, "回测期")
    
    # 保存结果
    os.makedirs('backtest_results', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    results = {
        'develop': develop_result,
        'backtest': backtest_result,
        'optimizations': {
            'stop_loss': RiskControl.STOP_LOSS,
            'take_profit': RiskControl.TAKE_PROFIT,
            'max_drawdown_threshold': RiskControl.MAX_DRAWDOWN_THRESHOLD,
            'reduction_ratio': RiskControl.REDUCTION_RATIO,
            'dynamic_position': True
        }
    }
    
    filepath = f'backtest_results/optimized_{timestamp}.json'
    with open(filepath, 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ 结果已保存: {filepath}")


if __name__ == "__main__":
    main()
