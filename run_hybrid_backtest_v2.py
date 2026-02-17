#!/usr/bin/env python3
"""
基本面混合策略回测
与实盘保持一致：基本面筛选 + 技术面择时 + 风险平价仓位
"""

import os
import sys
import json
import sqlite3
import logging
from datetime import datetime
from typing import List, Dict

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============ 配置 ============
DB_PATH = "data/stocks.db"

# 回测期间
DEVELOP_START = "20200101"
DEVELOP_END = "20221231"
BACKTEST_START = "20230101"
BACKTEST_END = "20241231"

INITIAL_CAPITAL = 1000000


# ============ 交易成本 ============
class TradingCosts:
    """交易成本"""
    COMMISSION_RATE = 0.00015  # 万1.5
    MIN_COMMISSION = 5
    STAMP_DUTY_RATE = 0.001  # 千1
    TRANSFER_FEE_RATE = 0.00002  # 万0.2
    SLIPPAGE_RATE = 0.0005  # 万5
    
    @classmethod
    def calc_buy_cost(cls, amount: float) -> float:
        comm = max(amount * cls.COMMISSION_RATE, cls.MIN_COMMISSION)
        return comm + amount * cls.TRANSFER_FEE_RATE + amount * cls.SLIPPAGE_RATE
    
    @classmethod
    def calc_sell_cost(cls, amount: float) -> float:
        comm = max(amount * cls.COMMISSION_RATE, cls.MIN_COMMISSION)
        return comm + amount * cls.STAMP_DUTY_RATE + amount * cls.TRANSFER_FEE_RATE + amount * cls.SLIPPAGE_RATE


class HybridBacktestEngine:
    """基本面混合策略回测引擎"""
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.initial_capital = INITIAL_CAPITAL
        
        # 基本面条件（与模拟盘一致）
        self.conditions = {
            'max_pe': 25,
            'min_roe': 10,
            'min_dv_ratio': 1,
            'max_debt': 70,
            'min_market_cap': 30
        }
    
    def _get_conn(self):
        return sqlite3.connect(self.db_path)
    
    def get_fundamental_candidates(self, date: str) -> List[Dict]:
        """获取基本面候选股票（指定日期的快照）"""
        # 简化：使用最新基本面数据，实际应使用历史快照
        conn = self._get_conn()
        
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
        
        return df.to_dict('records') if df is not None else []
    
    def get_stock_price(self, ts_code: str, date: str) -> float:
        """获取指定日期的收盘价"""
        conn = self._get_conn()
        
        df = pd.read_sql(f"""
            SELECT close FROM daily
            WHERE ts_code = '{ts_code}' AND trade_date <= '{date}'
            ORDER BY trade_date DESC LIMIT 1
        """, conn)
        
        conn.close()
        
        return df.iloc[0]['close'] if df is not None and len(df) > 0 else None
    
    def get_price_series(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取价格序列"""
        conn = self._get_conn()
        
        df = pd.read_sql(f"""
            SELECT trade_date, open, high, low, close, vol
            FROM daily
            WHERE ts_code = '{ts_code}'
              AND trade_date >= '{start_date}'
              AND trade_date <= '{end_date}'
            ORDER BY trade_date
        """, conn)
        
        conn.close()
        
        if df is not None and len(df) > 0:
            df = df.rename(columns={
                'vol': 'Volume', 'open': 'Open', 'high': 'High',
                'low': 'Low', 'close': 'Close'
            })
        
        return df
    
    def check_technical_signal(self, df: pd.DataFrame) -> str:
        """检查技术面信号"""
        if df is None or len(df) < 60:
            return 'hold'
        
        close = df['Close']
        
        # 均线
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        
        # MACD
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        
        if len(df) < 2:
            return 'hold'
        
        # 买入: 均线多头 + MACD金叉
        if (ma5.iloc[-1] > ma20.iloc[-1] and
            macd.iloc[-1] > signal.iloc[-1] and
            macd.iloc[-2] <= signal.iloc[-2]):
            return 'buy'
        
        # 卖出: 均线死叉
        if (ma5.iloc[-1] < ma20.iloc[-1] or
            macd.iloc[-1] < signal.iloc[-1]):
            return 'sell'
        
        return 'hold'
    
    def run(self, start_date: str, end_date: str) -> Dict:
        """运行回测"""
        logger.info(f"回测期间: {start_date} - {end_date}")
        
        capital = self.initial_capital
        holdings = {}  # {code: {'qty': x, 'cost': y}}
        trades = []
        
        # 按月调仓
        dates = pd.date_range(start=start_date, end=end_date, freq='MS')
        
        for i, month_start in enumerate(dates):
            month_str = month_start.strftime('%Y%m%d')
            
            # 每月获取候选
            candidates = self.get_fundamental_candidates(month_str)
            
            # 技术面筛选
            buy_signals = []
            for c in candidates[:30]:
                data = self.get_price_series(c['ts_code'], 
                    month_start.strftime('%Y%m%d'), 
                    (month_start + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime('%Y%m%d'))
                
                if data is not None and len(data) >= 20:
                    signal = self.check_technical_signal(data)
                    if signal == 'buy':
                        buy_signals.append(c)
            
            # 卖出风控
            to_sell = []
            for code in list(holdings.keys()):
                data = self.get_price_series(code,
                    month_start.strftime('%Y%m%d'),
                    (month_start + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime('%Y%m%d'))
                
                if data is not None and len(data) > 0:
                    signal = self.check_technical_signal(data)
                    price = data.iloc[-1]['Close']
                    
                    # 止盈止损
                    cost = holdings[code]['cost']
                    if price >= cost * 1.05:  # 5%止盈
                        to_sell.append((code, price, '止盈'))
                    elif price <= cost * 0.97:  # 3%止损
                        to_sell.append((code, price, '止损'))
                    elif signal == 'sell':
                        to_sell.append((code, price, '技术卖出'))
            
            # 执行卖出
            for code, price, reason in to_sell:
                qty = holdings[code]['qty']
                revenue = price * qty - TradingCosts.calc_sell_cost(price * qty)
                capital += revenue
                trades.append({'date': month_str, 'code': code, 'action': 'sell', 'price': price, 'qty': qty, 'reason': reason})
                del holdings[code]
            
            # 买入
            if buy_signals and capital > 0:
                # 风险平价分配
                n = min(len(buy_signals), 5)
                per_stock = capital / n * 0.3  # 30%仓位
                
                for c in buy_signals[:n]:
                    if capital < per_stock * 1.1:
                        break
                    
                    price = self.get_stock_price(c['ts_code'], month_str)
                    if price:
                        qty = int(per_stock / price / 100) * 100
                        if qty > 0:
                            cost = price * qty + TradingCosts.calc_buy_cost(price * qty)
                            if cost <= capital:
                                capital -= cost
                                holdings[c['ts_code']] = {'qty': qty, 'cost': price, 'name': c['name']}
                                trades.append({'date': month_str, 'code': c['ts_code'], 'action': 'buy', 'price': price, 'qty': qty, 'name': c['name']})
            
            # 日志
            if i % 6 == 0:
                total_value = capital + sum(
                    self.get_stock_price(code, month_str) * h['qty'] 
                    for code, h in holdings.items() 
                    if self.get_stock_price(code, month_str)
                )
                logger.info(f"{month_str}: 现金={capital:,.0f}, 持仓={len(holdings)}, 总值={total_value:,.0f}")
        
        # 最终计算
        final_value = capital
        for code, h in holdings.items():
            price = self.get_stock_price(code, end_date)
            if price:
                final_value += price * h['qty']
        
        total_return = (final_value - self.initial_capital) / self.initial_capital
        years = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(start_date, '%Y%m%d')).days / 365
        annual_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
        
        return {
            'period': f'{start_date}-{end_date}',
            'initial_capital': self.initial_capital,
            'final_assets': final_value,
            'total_return': total_return * 100,
            'annual_return': annual_return * 100,
            'trade_count': len(trades),
            'final_holdings': len(holdings)
        }


def main():
    print("="*70)
    print("📊 基本面混合策略回测")
    print("="*70)
    print(f"初始资金: ¥{INITIAL_CAPITAL:,}")
    print(f"基本面条件: PE≤25, ROE≥10%, 股息≥1%, 负债≤70%, 市值≥30亿")
    print(f"技术面: 均线多头 + MACD金叉买入, 死叉/止损卖出")
    print(f"仓位: 风险平价, 最多5只, 30%仓位")
    print("="*70)
    
    engine = HybridBacktestEngine()
    
    # 开发期
    logger.info("运行开发期回测...")
    develop_result = engine.run(DEVELOP_START, DEVELOP_END)
    
    # 回测期
    logger.info("运行回测期回测...")
    backtest_result = engine.run(BACKTEST_START, BACKTEST_END)
    
    # 打印结果
    print("\n" + "="*70)
    print("📈 回测结果")
    print("="*70)
    
    for name, result in [('开发期 (2020-2022)', develop_result), ('回测期 (2023-2024)', backtest_result)]:
        print(f"\n【{name}】")
        print(f"  初始资金: ¥{result['initial_capital']:>12,.0f}")
        print(f"  最终资产: ¥{result['final_assets']:>12,.0f}")
        print(f"  总收益:   {result['total_return']:>11.2f}%")
        print(f"  年化收益: {result['annual_return']:>11.2f}%")
        print(f"  交易次数: {result['trade_count']:>12}")
        print(f"  最终持仓: {result['final_holdings']:>12}只")
    
    print("\n" + "="*70)
    
    # 保存结果
    result = {
        'develop': develop_result,
        'backtest': backtest_result,
        'conditions': engine.conditions
    }
    
    filepath = f"backtest_results/hybrid_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filepath, 'w') as f:
        json.dump(result, f, indent=2, default=str)
    
    logger.info(f"结果已保存: {filepath}")


if __name__ == "__main__":
    main()
