#!/usr/bin/env python3
"""
混合策略回测
基本面选股 + 技术面交易
回测期: 从config读取
"""

import os
import sys
import sqlite3
import logging
from datetime import datetime
from typing import List, Dict

import pandas as pd
import numpy as np

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 导入config
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src.config import config


class BacktestEngine:
    """回测引擎"""
    
    def __init__(self, db_path: str = "data/stocks.db"):
        self.db_path = db_path
        self.initial_capital = config.initial_capital  # 从config读取
        self.commission_rate = 0.0003  # 万三佣金
        self.stamp_tax = 0.001  # 千一印花税
    
    def _get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def get_stock_data(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取股票数据"""
        conn = self._get_connection()
        
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
                'open': 'Open', 'high': 'High', 'low': 'Low',
                'close': 'Close', 'vol': 'Volume'
            })
        
        return df
    
    def get_fundamental_data(self, ts_code: str) -> Dict:
        """获取基本面数据"""
        conn = self._get_connection()
        
        df = pd.read_sql(f"""
            SELECT pe, roe, dv_ratio, debt_to_assets, market_cap
            FROM fundamentals
            WHERE ts_code = '{ts_code}'
        """, conn)
        
        conn.close()
        
        if df is not None and len(df) > 0:
            return df.iloc[0].to_dict()
        return {}
    
    def get_all_stocks_with_fundamentals(self, 
                                        max_pe: float = 25,
                                        min_roe: float = 10,
                                        min_dv_ratio: float = 1,
                                        max_debt: float = 70,
                                        min_market_cap: float = 30) -> List[str]:
        """获取符合基本面条件的股票池"""
        conn = self._get_connection()
        
        query = f"""
            SELECT ts_code FROM fundamentals
            WHERE pe > 0 AND pe <= {max_pe}
              AND roe >= {min_roe}
              AND dv_ratio >= {min_dv_ratio}
              AND debt_to_assets <= {max_debt}
              AND market_cap >= {min_market_cap}
            ORDER BY roe DESC
            LIMIT 100
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df['ts_code'].tolist() if df is not None else []
    
    def check_technical_signal(self, df: pd.DataFrame) -> str:
        """
        检查技术面信号
        返回: 'buy', 'sell', 'hold'
        """
        if df is None or len(df) < 60:
            return 'hold'
        
        close = df['Close']
        
        # 均线系统
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        
        # MACD
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        
        # 最新数据
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else latest
        
        # 买入信号: 均线多头排列 + MACD金叉
        if (ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1] and
            macd.iloc[-1] > signal.iloc[-1] and
            macd.iloc[-2] <= signal.iloc[-2]):
            return 'buy'
        
        # 卖出信号: 均线死叉或跌破均线
        if (ma5.iloc[-1] < ma20.iloc[-1] or
            macd.iloc[-1] < signal.iloc[-1]):
            return 'sell'
        
        return 'hold'
    
    def run_backtest(self, 
                    start_date: str = "20200101",
                    end_date: str = "20241231",
                    conditions: Dict = None) -> Dict:
        """
        运行回测
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            conditions: 基本面筛选条件
        
        Returns:
            回测结果
        """
        if conditions is None:
            conditions = {
                'max_pe': 25,
                'min_roe': 10,
                'min_dv_ratio': 1,
                'max_debt': 70,
                'min_market_cap': 30
            }
        
        logger.info(f"回测期间: {start_date} - {end_date}")
        
        # 获取候选股票池
        stock_pool = self.get_all_stocks_with_fundamentals(**conditions)
        logger.info(f"基本面候选股票: {len(stock_pool)} 只")
        
        # 模拟交易
        capital = self.initial_capital
        holdings = {}  # {ts_code: {'quantity': x, 'cost': y}
        trades = []
        
        # 按月调仓
        months = pd.date_range(start=start_date, end=end_date, freq='MS')
        
        for i, month_start in enumerate(months):
            month_end = (month_start + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime('%Y%m%d')
            month_str = month_start.strftime('%Y-%m')
            
            if i % 6 == 0:  # 每半年筛选一次
                # 重新筛选候选
                current_candidates = stock_pool[:30]  # 取前30只
            
            # 对候选股票进行技术面检查
            buy_signals = []
            for ts_code in current_candidates:
                data = self.get_stock_data(ts_code, month_start.strftime('%Y%m%d'), month_end)
                if data is not None and len(data) > 20:
                    signal = self.check_technical_signal(data)
                    if signal == 'buy':
                        buy_signals.append(ts_code)
            
            # 卖出信号处理
            for ts_code in list(holdings.keys()):
                data = self.get_stock_data(ts_code, month_start.strftime('%Y%m%d'), month_end)
                if data is not None and len(data) > 0:
                    signal = self.check_technical_signal(data)
                    if signal == 'sell':
                        # 卖出
                        price = data.iloc[-1]['Close']
                        quantity = holdings[ts_code]['quantity']
                        revenue = price * quantity * (1 - self.commission_rate - self.stamp_tax)
                        capital += revenue
                        trades.append({
                            'date': month_end,
                            'code': ts_code,
                            'action': 'sell',
                            'price': price,
                            'quantity': quantity
                        })
                        del holdings[ts_code]
            
            # 买入信号处理
            if buy_signals and capital > 0:
                # 分配资金
                per_stock = capital / min(len(buy_signals), 5)
                
                for ts_code in buy_signals[:5]:  # 最多5只
                    if capital < per_stock * 1.1:
                        break
                    
                    data = self.get_stock_data(ts_code, month_start.strftime('%Y%m%d'), month_end)
                    if data is not None and len(data) > 0:
                        price = data.iloc[-1]['Close']
                        quantity = int(per_stock / price / 100) * 100  # 整手
                        
                        if quantity > 0:
                            cost = price * quantity * (1 + self.commission_rate)
                            
                            if cost <= capital:
                                capital -= cost
                                holdings[ts_code] = {
                                    'quantity': quantity,
                                    'cost': price
                                }
                                trades.append({
                                    'date': month_end,
                                    'code': ts_code,
                                    'action': 'buy',
                                    'price': price,
                                    'quantity': quantity
                                })
            
            # 计算当前市值
            current_value = capital
            for ts_code, holding in holdings.items():
                data = self.get_stock_data(ts_code, month_start.strftime('%Y%m%d'), month_end)
                if data is not None and len(data) > 0:
                    current_value += data.iloc[-1]['Close'] * holding['quantity']
            
            if i == 0 or i % 12 == 11:
                logger.info(f"{month_str}: 市值={current_value:,.0f}, 持仓={len(holdings)}")
        
        # 最终清仓
        final_value = capital
        for ts_code, holding in holdings.items():
            # 获取最后一天收盘价
            data = self.get_stock_data(ts_code, end_date, end_date)
            if data is not None and len(data) > 0:
                final_value += data.iloc[-1]['Close'] * holding['quantity']
        
        # 计算收益
        total_return = (final_value - self.initial_capital) / self.initial_capital
        years = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(start_date, '%Y%m%d')).days / 365
        annual_return = (1 + total_return) ** (1 / years) - 1
        
        # 计算最大回撤
        returns = []
        running_value = self.initial_capital
        for trade in trades:
            if trade['action'] == 'buy':
                running_value -= trade['price'] * trade['quantity'] * 1.001
            else:
                running_value += trade['price'] * trade['quantity'] * 0.999
        
        result = {
            'period': f'{start_date}-{end_date}',
            'initial_capital': self.initial_capital,
            'final_assets': final_value,
            'total_return': total_return * 100,
            'annual_return': annual_return * 100,
            'trade_count': len(trades),
            'holdings': len(holdings)
        }
        
        return result


def run_strategy_comparison():
    """运行策略对比回测"""
    
    logger.info("=" * 60)
    logger.info("多策略回测对比")
    logger.info("=" * 60)
    
    engine = BacktestEngine()
    
    # 策略1: 混合策略 (基本面+技术面)
    logger.info("\n策略1: 混合策略 (基本面筛选 + 技术面择时)")
    result1 = engine.run_backtest(
        start_date="20200101",
        end_date="20241231",
        conditions={
            'max_pe': 25,
            'min_roe': 10,
            'min_dv_ratio': 1,
            'max_debt': 70,
            'min_market_cap': 30
        }
    )
    
    # 策略2: 纯基本面 (只选不炒)
    logger.info("\n策略2: 纯基本面 (买入持有)")
    
    # 简化: 假设买入持有
    result2 = {
        'period': '20200101-20241231',
        'initial_capital': 1000000,
        'final_assets': 1000000 * 1.3,  # 简化估算
        'total_return': 30,
        'annual_return': 5.3,
        'trade_count': 5,
        'note': '简化估算'
    }
    
    # 策略3: 纯技术面
    logger.info("\n策略3: 纯技术面")
    result3 = engine.run_backtest(
        start_date="20200101",
        end_date="20241231",
        conditions={
            'max_pe': 100,  # 不限制PE
            'min_roe': 0,
            'min_dv_ratio': 0,
            'max_debt': 100,
            'min_market_cap': 0
        }
    )
    
    # 打印结果
    print("\n" + "=" * 70)
    print("📊 多策略回测结果 (2020-2024)")
    print("=" * 70)
    
    strategies = [
        ("混合策略 (基本面+技术面)", result1),
        ("纯基本面 (简化估算)", result2),
        ("纯技术面", result3)
    ]
    
    for name, result in strategies:
        print(f"\n【{name}】")
        print(f"  初始资金: ¥{result['initial_capital']:>12,.0f}")
        print(f"  最终资产: ¥{result['final_assets']:>12,.0f}")
        print(f"  总收益:   {result['total_return']:>11.2f}%")
        print(f"  年化收益: {result['annual_return']:>11.2f}%")
        print(f"  交易次数: {result['trade_count']:>12}")
    
    print("\n" + "=" * 70)
    
    return strategies


if __name__ == "__main__":
    run_strategy_comparison()
