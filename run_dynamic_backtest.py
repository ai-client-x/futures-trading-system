#!/usr/bin/env python3
"""
多策略对比回测 - 动态选股池版
模拟实盘：每日/每周检查选股池，动态更新
"""

import os
import sys
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 添加项目路径并导入config
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src.config import config

DB_PATH = "data/stocks.db"
INITIAL_CAPITAL = config.initial_capital

# 回测期间
BACKTEST_START = config.backtest_start
BACKTEST_END = config.backtest_end



def calculate_max_positions(capital: float) -> int:
    """根据资金量动态计算持仓数量
    
    规则:
    - 10万以下: 2-3只
    - 10-50万: 3-5只
    - 50-100万: 5-8只
    - 100万+: 8-15只
    """
    if capital < 100000:
        return 2
    elif capital < 500000:
        return 3
    elif capital < 1000000:
        return 5
    elif capital < 2000000:
        return 8
    else:
        return 10


class TradingCosts:
    """交易成本"""
    COMMISSION = config.commission_rate
    MIN_COMM = 5
    STAMP = config.stamp_tax
    TRANSFER = 0.00002
    SLIPPAGE = config.slippage
    
    @classmethod
    def buy_cost(cls, amount):
        comm = max(amount * cls.COMMISSION, cls.MIN_COMM)
        return comm + amount * cls.TRANSFER + amount * cls.SLIPPAGE
    
    @classmethod
    def sell_cost(cls, amount):
        comm = max(amount * cls.COMMISSION, cls.MIN_COMM)
        return comm + amount * cls.STAMP + amount * cls.TRANSFER + amount * cls.SLIPPAGE


class DynamicBacktest:
    """动态选股池回测"""
    
    def __init__(self):
        self.db_path = DB_PATH
        self.initial_capital = INITIAL_CAPITAL
        
        # 基本面筛选条件
        self.conditions = {
            'max_pe': config.max_pe,
            'min_roe': config.min_roe,
            'min_dv_ratio': config.min_dv_ratio,
            'max_debt': config.max_debt,
            'min_market_cap': config.min_market_cap
        }
        
        logger.info(f"基本面筛选条件: {self.conditions}")
    
    def _conn(self):
        return sqlite3.connect(self.db_path)
    
    def get_stock_pool(self, date: str) -> List[Dict]:
        """
        获取指定日期的选股池
        从数据库动态读取
        """
        conn = self._conn()
        
        query = f"""
            SELECT ts_code, name, close, pe, roe, dv_ratio, 
                   debt_to_assets, market_cap
            FROM fundamentals
            WHERE pe > 0 AND pe <= {self.conditions['max_pe']}
              AND roe >= {self.conditions['min_roe']}
              AND dv_ratio >= {self.conditions['min_dv_ratio']}
              AND debt_to_assets <= {self.conditions['max_debt']}
              AND market_cap >= {self.conditions['min_market_cap']}
            ORDER BY roe DESC
            LIMIT 50
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df is None or len(df) == 0:
            return []
        
        return df.to_dict('records')
    
    def get_price_series(self, code: str, start: str, end: str) -> Optional[pd.DataFrame]:
        """获取价格序列"""
        conn = self._conn()
        
        df = pd.read_sql(f"""
            SELECT trade_date, open, high, low, close, vol
            FROM daily
            WHERE ts_code = '{code}'
              AND trade_date >= '{start}'
              AND trade_date <= '{end}'
            ORDER BY trade_date
        """, conn)
        
        conn.close()
        
        if df is not None and len(df) > 0:
            df = df.rename(columns={'vol': 'Volume'})
        
        return df
    
    def get_latest_price(self, code: str, date: str) -> Optional[float]:
        """获取指定日期收盘价"""
        conn = self._conn()
        
        df = pd.read_sql(f"""
            SELECT close FROM daily
            WHERE ts_code = '{code}' AND trade_date <= '{date}'
            ORDER BY trade_date DESC LIMIT 1
        """, conn)
        
        conn.close()
        
        return df.iloc[0]['close'] if df is not None and len(df) > 0 else None
    
    # ============ 策略信号 ============
    def check_ma_signal(self, df: pd.DataFrame) -> str:
        """均线策略信号"""
        if df is None or len(df) < 60:
            return 'hold'
        
        close = df['close']
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        
        if len(df) < 2:
            return 'hold'
        
        if ma5.iloc[-1] > ma20.iloc[-1] and ma5.iloc[-2] <= ma20.iloc[-2]:
            return 'buy'
        if ma5.iloc[-1] < ma20.iloc[-1] and ma5.iloc[-2] >= ma20.iloc[-2]:
            return 'sell'
        return 'hold'
    
    def check_macd_signal(self, df: pd.DataFrame) -> str:
        """MACD策略信号"""
        if df is None or len(df) < 60:
            return 'hold'
        
        close = df['close']
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9).mean()
        
        if len(df) < 2:
            return 'hold'
        
        if macd.iloc[-1] > signal.iloc[-1] and macd.iloc[-2] <= signal.iloc[-2]:
            return 'buy'
        if macd.iloc[-1] < signal.iloc[-1] and macd.iloc[-2] >= signal.iloc[-2]:
            return 'sell'
        return 'hold'
    
    def check_momentum_signal(self, df: pd.DataFrame) -> str:
        """动量策略信号"""
        if df is None or len(df) < 30:
            return 'hold'
        
        close = df['close']
        mom = close / close.shift(20) - 1
        
        if mom.iloc[-1] > 0.1:
            return 'buy'
        if mom.iloc[-1] < -0.05:
            return 'sell'
        return 'hold'
    
    def check_breakout_signal(self, df: pd.DataFrame) -> str:
        """突破策略信号 - 均线交叉"""
        if df is None or len(df) < 20:
            return 'hold'
        
        close = df['close']
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        
        if ma5.iloc[-1] > ma20.iloc[-1]:
            return 'buy'
        if ma5.iloc[-1] < ma20.iloc[-1]:
            return 'sell'
        return 'hold'
    
    def run(self, strategy_name: str, signal_func, start_date: str, end_date: str) -> Dict:
        """运行回测 - 支持多持仓"""
        logger.info(f"运行策略: {strategy_name}")
        
        capital = self.initial_capital
        positions = []  # 多持仓列表
        max_positions = calculate_max_positions(capital)  # 动态持仓数
        trades = []
        current_pool = []
        last_pool_date = None
        
        # 最大回撤
        peak_value = self.initial_capital
        max_drawdown = 0
        
        # 获取所有交易日
        conn = self._conn()
        trade_dates = pd.read_sql(f"""
            SELECT DISTINCT trade_date FROM daily
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            ORDER BY trade_date
        """, conn)['trade_date'].tolist()
        conn.close()
        
        for i, date in enumerate(trade_dates):
            month = date[:6]
            if month != last_pool_date:
                current_pool = self.get_stock_pool(date)
                last_pool_date = month
            
            if not current_pool:
                continue
            
            # === 卖出检查 ===
            to_sell = []
            for pos in list(positions):
                df = self.get_price_series(pos['code'], start_date, date)
                if df is not None and len(df) >= 20:
                    signal = signal_func(df)
                    price = df.iloc[-1]['close']
                    if (price <= pos['cost'] * (1 - config.stop_loss_pct) or
                        price >= pos['cost'] * (1 + config.take_profit_pct) or
                        signal == 'sell'):
                        to_sell.append((pos, price))
            
            for pos, price in to_sell:
                revenue = price * pos['qty'] - TradingCosts.sell_cost(price * pos['qty'])
                capital += revenue
                trades.append({'date': date, 'action': 'sell', 'price': price, 'code': pos['code'], 'reason': '风控'})
                positions = [p for p in positions if p['code'] != pos['code']]
            
            # === 买入检查 ===
            if len(positions) < max_positions and capital > 0:
                alloc = capital / (max_positions - len(positions))
                for stock in current_pool[:15]:
                    if len(positions) >= max_positions:
                        break
                    code = stock['ts_code']
                    if any(p['code'] == code for p in positions):
                        continue
                    df = self.get_price_series(code, start_date, date)
                    if df is not None and len(df) >= 60:
                        signal = signal_func(df)
                        if signal == 'buy':
                            price = df.iloc[-1]['close']
                            qty = int(alloc / price / 100) * 100
                            if qty > 0:
                                cost = price * qty + TradingCosts.buy_cost(price * qty)
                                if cost <= capital:
                                    capital -= cost
                                    positions.append({
                                        'code': code,
                                        'qty': qty,
                                        'cost': price,
                                        'name': stock.get('name', '')
                                    })
                                    trades.append({
                                        'date': date, 'action': 'buy', 'price': price,
                                        'code': code, 'name': stock.get('name', '')
                                    })
            
            # === 市值计算 ===
            total_value = capital
            for pos in positions:
                price = self.get_latest_price(pos['code'], date)
                if price:
                    total_value += price * pos['qty']
            
            if total_value > peak_value:
                peak_value = total_value
            drawdown = (peak_value - total_value) / peak_value if peak_value > 0 else 0
            if drawdown > max_drawdown:
                max_drawdown = drawdown
            
            if (i + 1) % 60 == 0:
                logger.info(f"{date}: 现金={capital:,.0f}, 持仓={len(positions)}只, 总值={total_value:,.0f}, 回撤={drawdown*100:.1f}%")
        
        # 最终清仓
        for pos in list(positions):
            df = self.get_price_series(pos['code'], start_date, end_date)
            if df is not None and len(df) > 0:
                price = df.iloc[-1]['close']
                capital += price * pos['qty'] - TradingCosts.sell_cost(price * pos['qty'])
        
        total_return = (capital - self.initial_capital) / self.initial_capital
        years = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(start_date, '%Y%m%d')).days / 365
        
        return {
            'strategy': strategy_name,
            'initial_capital': self.initial_capital,
            'final_assets': capital,
            'total_return': total_return * 100,
            'annual_return': ((1 + total_return) ** (1/years) - 1) * 100 if years > 0 else 0,
            'max_drawdown': max_drawdown * 100,
            'trade_count': len(trades)
        }


def main():
    print("="*70)
    print("📊 多策略回测 (动态选股池)")
    print("="*70)
    print(f"回测期间: {BACKTEST_START} ~ {BACKTEST_END}")
    print(f"初始资金: ¥{INITIAL_CAPITAL:,}")
    print(f"基本面条件: PE≤{config.max_pe}, ROE≥{config.min_roe}%, 股息≥{config.min_dv_ratio}%, 负债≤{config.max_debt}%, 市值≥{config.min_market_cap}亿")
    print(f"风控: 止损{config.stop_loss_pct*100}%, 止盈{config.take_profit_pct*100}%")
    print("="*70)
    
    engine = DynamicBacktest()
    
    # 测试各个策略
    strategies = [
        ("均线策略", engine.check_ma_signal),
        ("MACD策略", engine.check_macd_signal),
        ("动量策略", engine.check_momentum_signal),
        ("突破策略", engine.check_breakout_signal),
    ]
    
    results = {'backtest': {}}
    
    for name, func in strategies:
        result = engine.run(name, func, BACKTEST_START, BACKTEST_END)
        results['backtest'][name] = result
    
    # 打印结果
    print("\n" + "="*70)
    print("📈 回测结果")
    print("="*70)
    
    total_return = 0
    for name, result in results['backtest'].items():
        print(f"  {result['strategy']:10s}: 收益={result['total_return']:>7.2f}%, 年化={result['annual_return']:>7.2f}%, 回撤={result['max_drawdown']:.2f}%, 交易={result['trade_count']}")
        total_return += result['total_return']
    
    avg_return = total_return / len(results['backtest'])
    print(f"  {'综合策略':10s}: 收益={avg_return:>7.2f}%")
    
    print("\n" + "="*70)
    
    # 保存
    filepath = f"backtest_results/dynamic_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    os.makedirs('backtest_results', exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"已保存: {filepath}")


if __name__ == "__main__":
    main()
