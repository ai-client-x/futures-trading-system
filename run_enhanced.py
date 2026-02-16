#!/usr/bin/env python3
"""
A股多策略交易系统 - 完整版
包含：
1. 多维度信号评分
2. 股票筛选与排序
3. 仓位分配
4. 风控管理
"""

import os
import sys
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import sqlite3
from src.engines.trading_engine import TradingEngine
from src.models import Position


# ============ 配置 ============
DB_PATH = "data/stocks.db"

# 测试时间
START_DATE = "20200101"
END_DATE = "20241231"

INITIAL_CAPITAL = 1000000

# 选股参数
MAX_POSITIONS = 5           # 最大持仓数
MAX_POSITION_PCT = 0.25      # 单只最大仓位比例
MIN_TURNOVER = 10000000     # 最小日成交额(1000万)
MIN_MARKET_CAP = 50e8        # 最小市值(50亿)

# 风控参数
STOP_LOSS = -5               # 止损
TAKE_PROFIT = 10             # 止盈


# ============ 交易成本 ============
class TradingCosts:
    COMMISSION_RATE = 0.00015
    MIN_COMMISSION = 5
    STAMP_DUTY_RATE = 0.001
    TRANSFER_FEE_RATE = 0.00002
    SLIPPAGE_RATE = 0.0005
    
    @classmethod
    def calc_buy_cost(cls, amount: float) -> float:
        return max(amount * cls.COMMISSION_RATE, cls.MIN_COMMISSION) + amount * (cls.TRANSFER_FEE_RATE + cls.SLIPPAGE_RATE)
    
    @classmethod
    def calc_sell_cost(cls, amount: float) -> float:
        return max(amount * cls.COMMISSION_RATE, cls.MIN_COMMISSION) + amount * (cls.STAMP_DUTY_RATE + cls.TRANSFER_FEE_RATE + cls.SLIPPAGE_RATE)


# ============ 信号评分系统 ============
class SignalScorer:
    """多维度信号评分"""
    
    # 权重配置
    WEIGHTS = {
        'trend': 0.30,      # 趋势
        'momentum': 0.25,   # 动量
        'value': 0.25,      # 价值
        'liquidity': 0.20   # 流动性
    }
    
    @classmethod
    def calculate_score(cls, df: pd.DataFrame, turnover: float = 0) -> dict:
        """
        计算综合信号分数
        返回: {'score': 0-100, 'details': {...}}
        """
        if df is None or len(df) < 60:
            return {'score': 0, 'action': 'hold', 'details': {}}
        
        close = df['close']
        
        # 1. 趋势信号 (30%)
        trend_score = cls._calc_trend_score(close)
        
        # 2. 动量信号 (25%)
        momentum_score = cls._calc_momentum_score(close)
        
        # 3. 价值信号 (25%)
        value_score = cls._calc_value_score(close)
        
        # 4. 流动性信号 (20%)
        liquidity_score = cls._calc_liquidity_score(turnover)
        
        # 综合分数
        total_score = (
            trend_score * cls.WEIGHTS['trend'] +
            momentum_score * cls.WEIGHTS['momentum'] +
            value_score * cls.WEIGHTS['value'] +
            liquidity_score * cls.WEIGHTS['liquidity']
        )
        
        # 判定动作
        if total_score >= 60:
            action = 'buy'
        elif total_score < 30:
            action = 'sell'
        else:
            action = 'hold'
        
        return {
            'score': total_score,
            'action': action,
            'details': {
                'trend': trend_score,
                'momentum': momentum_score,
                'value': value_score,
                'liquidity': liquidity_score
            }
        }
    
    @classmethod
    def _calc_trend_score(cls, close: pd.Series) -> float:
        """趋势信号：均线多头排列"""
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        
        score = 0
        if ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1]:
            score = 100
        elif ma5.iloc[-1] > ma20.iloc[-1]:
            score = 60
        elif ma5.iloc[-1] < ma20.iloc[-1] < ma60.iloc[-1]:
            score = 0
        elif ma5.iloc[-1] < ma20.iloc[-1]:
            score = 30
        
        return score
    
    @classmethod
    def _calc_momentum_score(cls, close: pd.Series) -> float:
        """动量信号：短期涨幅"""
        if len(close) < 20:
            return 50
        
        ret_5d = (close.iloc[-1] / close.iloc[-6] - 1) * 100 if len(close) > 5 else 0
        ret_20d = (close.iloc[-1] / close.iloc[-21] - 1) * 100 if len(close) > 20 else 0
        
        # 适度涨幅最好(3-15%)
        if 3 <= ret_5d <= 15 and ret_20d > 0:
            return 100
        elif ret_5d > 20 or ret_5d < -5:
            return 30
        else:
            return 60
    
    @classmethod
    def _calc_value_score(cls, close: pd.Series) -> float:
        """价值信号：价格接近均线（不过高不过低）"""
        ma20 = close.rolling(20).mean()
        
        if len(ma20) < 1 or pd.isna(ma20.iloc[-1]):
            return 50
        
        price_to_ma = close.iloc[-1] / ma20.iloc[-1]
        
        # 价格接近20日均线最好
        if 0.9 <= price_to_ma <= 1.1:
            return 100
        elif price_to_ma < 0.85:
            return 80  # 低估
        elif price_to_ma > 1.2:
            return 40  # 高估
        else:
            return 60
    
    @classmethod
    def _calc_liquidity_score(cls, turnover: float) -> float:
        """流动性信号"""
        if turnover >= MIN_TURNOVER * 5:
            return 100
        elif turnover >= MIN_TURNOVER:
            return 70
        elif turnover >= MIN_TURNOVER * 0.5:
            return 40
        else:
            return 0


# ============ 选股与仓位管理 ============
class StockSelector:
    """股票选择器"""
    
    def __init__(self, max_positions: int = 5, max_position_pct: float = 0.25):
        self.max_positions = max_positions
        self.max_position_pct = max_position_pct
    
    def select_stocks(self, candidates: list, current_positions: set, capital: float) -> list:
        """
        从候选股票中选择最优的N只
        返回: [(code, name, score, position_pct), ...]
        """
        # 过滤已持仓
        available = [c for c in candidates if c['code'] not in current_positions]
        
        if not available:
            return []
        
        # 按分数排序
        available.sort(key=lambda x: x['score'], reverse=True)
        
        # 取top N
        selected = available[:self.max_positions]
        
        # 仓位分配（等权重或按分数加权）
        positions = []
        total_score = sum(s['score'] for s in selected)
        
        for s in selected:
            # 按分数比例分配，但不超过最大仓位
            if total_score > 0:
                base_pct = (s['score'] / total_score) * self.max_positions * self.max_position_pct
            else:
                base_pct = self.max_position_pct / len(selected)
            
            # 限制最大仓位
            position_pct = min(base_pct, self.max_position_pct)
            
            positions.append({
                'code': s['code'],
                'name': s['name'],
                'score': s['score'],
                'position_pct': position_pct
            })
        
        return positions


# ============ 主回测函数 ============
def get_stock_pool():
    """获取候选股票池（从数据库读取）"""
    conn = sqlite3.connect(DB_PATH)
    
    # 简化：使用固定的热门股票池
    stocks = [
        ('600519.SH', '贵州茅台'),
        ('000858.SZ', '五粮液'),
        ('601318.SH', '中国平安'),
        ('300750.SZ', '宁德时代'),
        ('002594.SZ', '比亚迪'),
        ('600036.SH', '招商银行'),
        ('600900.SH', '长江电力'),
        ('601888.SH', '中国中免'),
        ('600276.SH', '恒瑞医药'),
        ('000001.SZ', '平安银行'),
        ('000333.SZ', '美的集团'),
        ('600030.SH', '中信证券'),
        ('601012.SH', '隆基绿能'),
        ('600690.SH', '海尔智家'),
        ('000002.SZ', '万科A'),
        ('600028.SH', '中国石化'),
        ('600887.SH', '伊利股份'),
        ('601398.SH', '工商银行'),
        ('600036.SH', '招商银行'),
        ('000651.SZ', '格力电器'),
    ]
    
    conn.close()
    return [{'code': s[0], 'name': s[1]} for s in stocks]


def run_backtest():
    """运行回测"""
    
    print("="*70)
    print("📈 A股多策略交易系统 - 完整版")
    print("="*70)
    print(f"最大持仓: {MAX_POSITIONS}")
    print(f"单只仓位: ≤{MAX_POSITION_PCT*100}%")
    print(f"止损: {STOP_LOSS}% | 止盈: {TAKE_PROFIT}%")
    print("="*70)
    
    # 初始化
    stock_pool = get_stock_pool()
    engine = TradingEngine(INITIAL_CAPITAL)
    selector = StockSelector(MAX_POSITIONS, MAX_POSITION_PCT)
    scorer = SignalScorer()
    
    # 获取交易日
    conn = sqlite3.connect(DB_PATH)
    dates_df = pd.read_sql(f'''
        SELECT DISTINCT trade_date 
        FROM daily 
        WHERE ts_code = '600519.SH'
        AND trade_date >= '{START_DATE}'
        AND trade_date <= '{END_DATE}'
        ORDER BY trade_date
    ''', conn)
    trade_dates = dates_df['trade_date'].tolist()
    conn.close()
    
    print(f"交易日数: {len(trade_dates)}")
    print(f"股票池: {len(stock_pool)}只")
    print()
    
    trades = []
    daily_values = []
    
    for i, date in enumerate(trade_dates):
        
        # ========== 1. 卖出检查 ==========
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
                    if pnl_pct <= STOP_LOSS or pnl_pct >= TAKE_PROFIT:
                        revenue = current_price * holding.quantity
                        cost = TradingCosts.calc_sell_cost(revenue)
                        engine.portfolio.cash += revenue - cost
                        trades.append({
                            'date': date,
                            'code': code,
                            'action': 'sell',
                            'price': current_price,
                            'pnl_pct': pnl_pct
                        })
                        holding.quantity = 0
                        del engine.portfolio.positions[code]
        
        # ========== 2. 买入决策 ==========
        current_positions = set(engine.portfolio.positions.keys())
        
        if len(current_positions) < MAX_POSITIONS:
            # 获取所有候选股票的信号
            candidates = []
            
            for stock in stock_pool:
                code = stock['code']
                name = stock['name']
                
                # 跳过已持仓
                if code in current_positions:
                    continue
                
                # 获取历史数据
                conn = sqlite3.connect(DB_PATH)
                df = pd.read_sql(f'''
                    SELECT trade_date, open, high, low, close, vol, amount
                    FROM daily
                    WHERE ts_code = '{code}'
                    AND trade_date <= '{date}'
                    ORDER BY trade_date
                ''', conn)
                
                # 获取当日成交额
                turnover_row = conn.execute(f'''
                    SELECT amount FROM daily 
                    WHERE ts_code='{code}' AND trade_date='{date}'
                ''').fetchone()
                turnover = float(turnover_row[0]) if turnover_row else 0
                conn.close()
                
                if len(df) < 60:
                    continue
                
                # 计算信号
                result = scorer.calculate_score(df, turnover)
                
                if result['action'] == 'buy':
                    candidates.append({
                        'code': code,
                        'name': name,
                        'score': result['score'],
                        'details': result['details']
                    })
            
            # 选择最优股票
            if candidates:
                selected = selector.select_stocks(
                    candidates, 
                    current_positions, 
                    engine.portfolio.cash
                )
                
                # 执行买入
                next_idx = trade_dates.index(date) + 1
                if next_idx < len(trade_dates):
                    next_date = trade_dates[next_idx]
                    
                    for s in selected:
                        code = s['code']
                        name = s['name']
                        position_pct = s['position_pct']
                        
                        # 获取次日开盘价
                        conn = sqlite3.connect(DB_PATH)
                        open_row = conn.execute(f'''
                            SELECT open FROM daily 
                            WHERE ts_code='{code}' AND trade_date='{next_date}'
                        ''').fetchone()
                        conn.close()
                        
                        if open_row:
                            open_price = float(open_row[0])
                            amount = engine.portfolio.cash * position_pct
                            qty = int(amount / open_price / 100) * 100  # 整手
                            
                            if qty > 0:
                                cost = open_price * qty
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
                                        'code': code,
                                        'action': 'buy',
                                        'price': open_price,
                                        'qty': qty,
                                        'score': s['score']
                                    })
        
        # 记录每日资产
        daily_values.append({
            'date': date,
            'assets': engine.portfolio.total_assets
        })
        
        # 进度
        if (i + 1) % 100 == 0:
            ret = (engine.portfolio.total_assets - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
            print(f"Day {i+1}: 资产¥{engine.portfolio.total_assets:,.0f} ({ret:+.2f}%)")
    
    # ========== 最终结算 ==========
    conn = sqlite3.connect(DB_PATH)
    last_date = trade_dates[-1]
    
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
    
    # ========== 计算结果 ==========
    final_assets = engine.portfolio.total_assets
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
    
    # 胜率
    sell_trades = [t for t in trades if t['action'] == 'sell']
    wins = sum(1 for t in sell_trades if t.get('pnl_pct', 0) > 0)
    win_rate = wins / len(sell_trades) * 100 if sell_trades else 0
    
    # 打印结果
    print()
    print("="*70)
    print("📊 回测结果")
    print("="*70)
    print(f"初始资金:     ¥{INITIAL_CAPITAL:,.0f}")
    print(f"最终资产:     ¥{final_assets:,.0f}")
    print(f"总收益率:     {total_return:+.2f}%")
    print(f"年化收益:     {annual_return:+.2f}%")
    print(f"最大回撤:     {max_drawdown:.2f}%")
    print(f"交易次数:     {len(trades)}")
    print(f"胜率:         {win_rate:.1f}%")
    print("="*70)
    
    # 评分
    score_annual = min(annual_return, 50) / 50 * 40
    score_dd = (1 - min(max_drawdown, 50) / 50) * 25
    score_win = win_rate / 100 * 20
    score_trade = min(len(trades), 100) / 100 * 15
    total_score = score_annual + score_dd + score_win + score_trade
    
    print(f"\n📈 策略评分: {total_score:.1f}/100")
    level = "⭐优秀" if total_score >= 60 else "✅良好" if total_score >= 40 else "⚠️一般"
    print(f"   评价: {level}")
    
    return {
        'final_assets': final_assets,
        'total_return': total_return,
        'annual_return': annual_return,
        'max_drawdown': max_drawdown,
        'trade_count': len(trades),
        'win_rate': win_rate,
        'score': total_score
    }


if __name__ == "__main__":
    result = run_backtest()
    
    # 保存结果
    os.makedirs('backtest_results', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = f'backtest_results/enhanced_{timestamp}.json'
    with open(filepath, 'w') as f:
        json.dump(result, f, indent=2)
    print(f"\n✅ 结果已保存: {filepath}")
