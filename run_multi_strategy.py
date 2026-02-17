#!/usr/bin/env python3
"""
A股多策略回测系统
同时运行多个策略，分散风险
"""

import os
import sys
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import sqlite3
from src.engines.trading_engine import TradingEngine


# ============ 配置 ============
DB_PATH = "data/stocks.db"

# 数据划分
DEVELOP_START = "20200101"
DEVELOP_END = "20221231"
BACKTEST_START = "20230101"
BACKTEST_END = "20241231"

INITIAL_CAPITAL = 1000000

# 测试股票
TEST_STOCKS = [
    '600519.SH', '000858.SZ', '601318.SH', '300750.SZ', '002594.SZ',
    '600036.SH', '600900.SH', '601888.SH', '600276.SH', '000001.SZ'
]

# ============ 交易成本设置 ============
class TradingCosts:
    """交易成本模拟"""
    
    # 手续费（券商佣金，默认万1.5，有最低5元）
    COMMISSION_RATE = 0.00015  # 万1.5
    MIN_COMMISSION = 5  # 最低5元
    
    # 印花税（卖出时收取，千1）
    STAMP_DUTY_RATE = 0.001  # 千1
    
    # 过户费（万0.2）
    TRANSFER_FEE_RATE = 0.00002  # 万0.2
    
    # 滑点（万5=0.05%）
    SLIPPAGE_RATE = 0.0005  # 万5
    
    @classmethod
    def calc_commission(cls, amount: float) -> float:
        """计算佣金"""
        comm = amount * cls.COMMISSION_RATE
        return max(comm, cls.MIN_COMMISSION)
    
    @classmethod
    def calc_buy_cost(cls, amount: float) -> float:
        """计算买入总成本（佣金+过户费+滑点）"""
        commission = cls.calc_commission(amount)
        transfer_fee = amount * cls.TRANSFER_FEE_RATE
        slippage = amount * cls.SLIPPAGE_RATE
        return commission + transfer_fee + slippage
    
    @classmethod
    def calc_sell_cost(cls, amount: float) -> float:
        """计算卖出总成本（佣金+印花税+过户费+滑点）"""
        commission = cls.calc_commission(amount)
        stamp_duty = amount * cls.STAMP_DUTY_RATE
        transfer_fee = amount * cls.TRANSFER_FEE_RATE
        slippage = amount * cls.SLIPPAGE_RATE
        return commission + stamp_duty + transfer_fee + slippage


# 打印成本信息
print("="*50)
print("📊 交易成本设置（模拟实盘）")
print("="*50)
print(f"券商佣金: {TradingCosts.COMMISSION_RATE*10000:.1f}‰ (最低¥{TradingCosts.MIN_COMMISSION})")
print(f"印花税:   {TradingCosts.STAMP_DUTY_RATE*1000:.1f}‰ (仅卖出)")
print(f"过户费:   {TradingCosts.TRANSFER_FEE_RATE*10000:.2f}‰")
print(f"滑点:     {TradingCosts.SLIPPAGE_RATE*10000:.1f}‰")
print(f"单笔往返成本: ~0.24%")
print("="*50)


# ============ 策略定义 ============

class Strategy:
    """策略基类"""
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
    """趋势跟踪策略 - 均线多头排列"""
    def __init__(self):
        super().__init__(
            name="趋势跟踪",
            max_positions=3,
            position_size=0.25,
            stop_loss=-7,
            take_profit=15,
            min_signal_score=25
        )
    
    def calculate_signal(self, df: pd.DataFrame) -> dict:
        if df is None or len(df) < 60:
            return {'action': 'hold', 'score': 0, 'reason': '数据不足'}
        
        close = df['close']
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        
        score = 0
        reasons = []
        
        # 均线多头排列
        if ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1]:
            score += 30
            reasons.append('均线多头排列')
        
        # 趋势强度
        if ma5.iloc[-1] > ma5.iloc[-5] * 1.02:
            score += 10
            reasons.append('短期趋势向上')
        
        # 成交量放大
        if len(df) >= 20:
            vol_ma = df['vol'].rolling(20).mean()
            if df['vol'].iloc[-1] > vol_ma.iloc[-1] * 1.5:
                score += 10
                reasons.append('成交量放大')
        
        if score >= self.min_signal_score:
            return {'action': 'buy', 'score': score, 'reason': '; '.join(reasons)}
        
        # 卖出信号
        if ma5.iloc[-1] < ma20.iloc[-1]:
            return {'action': 'sell', 'score': 20, 'reason': '均线死叉'}
        
        return {'action': 'hold', 'score': score, 'reason': '观望'}


class ValueStrategy(Strategy):
    """价值投资策略 - 低估值"""
    def __init__(self):
        super().__init__(
            name="价值投资",
            max_positions=3,
            position_size=0.3,
            stop_loss=-10,
            take_profit=20,
            min_signal_score=20
        )
    
    def calculate_signal(self, df: pd.DataFrame) -> dict:
        if df is None or len(df) < 120:
            return {'action': 'hold', 'score': 0, 'reason': '数据不足'}
        
        close = df['close']
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        ma120 = close.rolling(120).mean()
        
        score = 0
        reasons = []
        
        # 价格低于长期均线（低估）
        if ma20.iloc[-1] < ma60.iloc[-1] < ma120.iloc[-1]:
            score += 25
            reasons.append('价格低于长期均线')
        
        # 接近阶段性低点
        low_60 = close.rolling(60).min()
        if close.iloc[-1] < low_60.iloc[-1] * 1.1:
            score += 20
            reasons.append('接近60日低点')
        
        # RSI超卖
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))
        
        if rsi.iloc[-1] < 35:
            score += 15
            reasons.append(f'RSI超卖({rsi.iloc[-1]:.0f})')
        
        if score >= self.min_signal_score:
            return {'action': 'buy', 'score': score, 'reason': '; '.join(reasons)}
        
        # 卖出：价格远离均线
        if close.iloc[-1] > ma20.iloc[-1] * 1.2:
            return {'action': 'sell', 'score': 20, 'reason': '价格远离均线'}
        
        return {'action': 'hold', 'score': score, 'reason': '观望'}


class MomentumStrategy(Strategy):
    """动量策略 - 追强势股"""
    def __init__(self):
        super().__init__(
            name="动量策略",
            max_positions=2,
            position_size=0.2,
            stop_loss=-5,
            take_profit=12,
            min_signal_score=30
        )
    
    def calculate_signal(self, df: pd.DataFrame) -> dict:
        if df is None or len(df) < 30:
            return {'action': 'hold', 'score': 0, 'reason': '数据不足'}
        
        close = df['close']
        
        # 短期涨幅
        ret_5d = (close.iloc[-1] / close.iloc[-6] - 1) * 100 if len(close) > 5 else 0
        ret_20d = (close.iloc[-1] / close.iloc[-21] - 1) * 100 if len(close) > 20 else 0
        
        score = 0
        reasons = []
        
        # 短期强势但不过热
        if 3 < ret_5d < 15:
            score += 20
            reasons.append(f'5日涨幅{ret_5d:.1f}%')
        
        # 中期趋势向上
        if ret_20d > 5:
            score += 15
            reasons.append(f'20日涨幅{ret_20d:.1f}%')
        
        # 成交量配合
        if len(df) >= 10:
            vol_ma = df['vol'].rolling(10).mean()
            if df['vol'].iloc[-1] > vol_ma.iloc[-1] * 1.2:
                score += 10
                reasons.append('成交量放大')
        
        if score >= self.min_signal_score:
            return {'action': 'buy', 'score': score, 'reason': '; '.join(reasons)}
        
        # 卖出：动量反转
        if ret_5d < -5:
            return {'action': 'sell', 'score': 25, 'reason': '短期大跌'}
        
        return {'action': 'hold', 'score': score, 'reason': '观望'}


class BreakoutStrategy(Strategy):
    """突破策略 - 突破关键点位"""
    def __init__(self):
        super().__init__(
            name="突破策略",
            max_positions=2,
            position_size=0.2,
            stop_loss=-4,
            take_profit=10,
            min_signal_score=25
        )
    
    def calculate_signal(self, df: pd.DataFrame) -> dict:
        if df is None or len(df) < 60:
            return {'action': 'hold', 'score': 0, 'reason': '数据不足'}
        
        close = df['close']
        high = df['high']
        
        # 20日高点
        high_20 = high.rolling(20).max().shift(1)
        
        score = 0
        reasons = []
        
        # 突破20日高点
        if close.iloc[-1] > high_20.iloc[-1]:
            score += 30
            reasons.append('突破20日高点')
        
        # 放量突破
        if len(df) >= 20:
            vol_ma = df['vol'].rolling(20).mean()
            if df['vol'].iloc[-1] > vol_ma.iloc[-1] * 1.5 and close.iloc[-1] > high_20.iloc[-1]:
                score += 15
                reasons.append('放量突破')
        
        # 突破后回踩不破
        if len(df) >= 5:
            if close.iloc[-1] > close.iloc[-5] * 0.98:
                score += 10
                reasons.append('回踩支撑有效')
        
        if score >= self.min_signal_score:
            return {'action': 'buy', 'score': score, 'reason': '; '.join(reasons)}
        
        # 卖出：跌破均线
        ma10 = close.rolling(10).mean()
        if close.iloc[-1] < ma10.iloc[-1]:
            return {'action': 'sell', 'score': 20, 'reason': '跌破10日线'}
        
        return {'action': 'hold', 'score': score, 'reason': '观望'}


# ============ 多策略组合 ============

class MultiStrategyPortfolio:
    """多策略组合"""
    
    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.strategies = [
            TrendStrategy(),
            ValueStrategy(),
            MomentumStrategy(),
            BreakoutStrategy()
        ]
        
        # 每个策略独立的交易引擎
        self.engines = {s.name: TradingEngine(initial_capital / len(self.strategies)) 
                       for s in self.strategies}
        
        # 持仓统计
        self.positions = {}  # {strategy_name: {code: holding}}
        
        print(f"\n🎯 多策略组合初始化完成")
        print(f"   策略数量: {len(self.strategies)}")
        print(f"   每策略资金: ¥{initial_capital / len(self.strategies):,.0f}")
        for s in self.strategies:
            print(f"   - {s.name}")
    
    def get_total_assets(self) -> float:
        return sum(e.portfolio.total_assets for e in self.engines.values())
    
    def get_all_positions(self) -> dict:
        """获取所有策略的持仓"""
        all_positions = {}
        for name, engine in self.engines.items():
            for code, holding in engine.portfolio.positions.items():
                if holding.quantity > 0:
                    all_positions[f"{name}_{code}"] = {
                        'strategy': name,
                        'code': code,
                        'name': holding.name,
                        'quantity': holding.quantity,
                        'avg_cost': holding.avg_cost
                    }
        return all_positions


# ============ 回测函数 ============

def get_stock_list():
    """获取股票列表"""
    conn = sqlite3.connect(DB_PATH)
    placeholders = ','.join(['?'] * len(TEST_STOCKS))
    df = pd.read_sql(f"SELECT ts_code, name, market FROM stocks WHERE ts_code IN ({placeholders})", 
                   conn, params=TEST_STOCKS)
    conn.close()
    return df.to_dict('records')


def run_backtest(stock_list: list, start_date: str, end_date: str, period_name: str) -> dict:
    """运行多策略回测"""
    
    print(f"\n{'='*60}")
    print(f"📊 {period_name} - {start_date} ~ {end_date}")
    print(f"{'='*60}")
    
    # 初始化多策略组合
    portfolio = MultiStrategyPortfolio(INITIAL_CAPITAL)
    
    # 获取交易日
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
    
    for i, date in enumerate(trade_dates):
        # 遍历每个策略
        for strategy in portfolio.strategies:
            engine = portfolio.engines[strategy.name]
            
            # 1. 卖出检查（止盈止损）
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
                        
                        # 止损/止盈
                        if pnl_pct <= strategy.stop_loss or pnl_pct >= strategy.take_profit:
                            revenue = current_price * holding.quantity
                            # 使用新的交易成本计算
                            cost = TradingCosts.calc_sell_cost(revenue)
                            engine.portfolio.cash += revenue - cost
                            trades.append({
                                'date': date,
                                'strategy': strategy.name,
                                'code': code,
                                'action': 'sell',
                                'price': current_price,
                                'pnl_pct': pnl_pct
                            })
                            holding.quantity = 0
                            del engine.portfolio.positions[code]
            
            # 2. 买入检查
            if len(engine.portfolio.positions) < strategy.max_positions:
                # 遍历股票找信号
                for stock in stock_list:
                    code = stock['ts_code']
                    name = stock['name']
                    
                    if code in engine.portfolio.positions:
                        continue
                    
                    # 获取历史数据
                    conn = sqlite3.connect(DB_PATH)
                    df = pd.read_sql(f"""
                        SELECT trade_date, open, high, low, close, vol, amount
                        FROM daily
                        WHERE ts_code = '{code}'
                        AND trade_date <= '{date}'
                        ORDER BY trade_date
                    """, conn)
                    conn.close()
                    
                    if len(df) < 60:
                        continue
                    
                    # 计算信号
                    signal = strategy.calculate_signal(df)
                    
                    if signal['action'] == 'buy':
                        # 次日开盘买入
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
                                amount = engine.portfolio.cash * strategy.position_size
                                qty = int(amount / open_price / 100) * 100
                                
                                if qty > 0:
                                    # 买入 - 使用Position类
                                    from src.models import Position
                                    cost = open_price * qty
                                    # 使用新的交易成本计算
                                    trade_cost = TradingCosts.calc_buy_cost(cost)
                                    total_cost = cost + trade_cost
                                    if engine.portfolio.cash >= total_cost:
                                        engine.portfolio.cash -= total_cost
                                        
                                        position = Position(
                                            code=code,
                                            name=name,
                                            quantity=qty,
                                            avg_cost=open_price,
                                            current_price=open_price
                                        )
                                        engine.portfolio.positions[code] = position
                                        
                                        trades.append({
                                            'date': next_date,
                                            'strategy': strategy.name,
                                            'code': code,
                                            'action': 'buy',
                                            'price': open_price,
                                            'qty': qty
                                        })
        
        # 进度
        if (i + 1) % 100 == 0:
            total = portfolio.get_total_assets()
            ret = (total - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
            print(f"Day {i+1}: 总资产¥{total:,.0f} ({ret:+.2f}%)")
    
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
                    # 使用新的交易成本计算
                    cost = TradingCosts.calc_sell_cost(revenue)
                    engine.portfolio.cash += revenue - cost
    conn.close()
    
    # 计算结果
    final_assets = portfolio.get_total_assets()
    total_return = (final_assets - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
    years = len(trade_dates) / 252
    annual_return = total_return / years if years > 0 else 0
    
    # 各策略结果
    strategy_results = {}
    for strategy in portfolio.strategies:
        engine = portfolio.engines[strategy.name]
        strat_ret = (engine.portfolio.total_assets - INITIAL_CAPITAL / len(portfolio.strategies)) / (INITIAL_CAPITAL / len(portfolio.strategies)) * 100
        strategy_results[strategy.name] = {
            'final_assets': engine.portfolio.total_assets,
            'return': strat_ret,
            'positions': len([h for h in engine.portfolio.positions.values() if h.quantity > 0])
        }
    
    # 打印结果
    print(f"\n{'='*60}")
    print(f"📊 {period_name} 结果")
    print(f"{'='*60}")
    print(f"初始资金:   ¥{INITIAL_CAPITAL:,.0f}")
    print(f"最终资产:   ¥{final_assets:,.0f}")
    print(f"总收益率:   {total_return:+.2f}%")
    print(f"年化收益:   {annual_return:+.2f}%")
    print(f"交易次数:   {len(trades)}")
    print(f"\n📈 各策略表现:")
    for name, result in strategy_results.items():
        print(f"  {name}: {result['return']:+.2f}% (¥{result['final_assets']:,.0f})")
    print(f"{'='*60}")
    
    return {
        'period': period_name,
        'start_date': start_date,
        'end_date': end_date,
        'initial_capital': INITIAL_CAPITAL,
        'final_assets': final_assets,
        'total_return': total_return,
        'annual_return': annual_return,
        'trade_count': len(trades),
        'strategy_results': strategy_results
    }


def main():
    print("="*60)
    print("📈 A股多策略量化回测系统")
    print("="*60)
    
    # 获取股票列表
    stock_list = get_stock_list()
    print(f"股票数量: {len(stock_list)}")
    
    # 开发期回测
    develop_result = run_backtest(stock_list, DEVELOP_START, DEVELOP_END, "开发期")
    
    # 回测期
    backtest_result = run_backtest(stock_list, BACKTEST_START, BACKTEST_END, "回测期")
    
    # 保存结果
    os.makedirs('backtest_results', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    results = {
        'develop': develop_result,
        'backtest': backtest_result,
        'strategies': [s.name for s in [
            TrendStrategy(), ValueStrategy(), MomentumStrategy(), BreakoutStrategy()
        ]]
    }
    
    filepath = f'backtest_results/multi_strategy_{timestamp}.json'
    with open(filepath, 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ 结果已保存: {filepath}")


if __name__ == "__main__":
    main()
