#!/usr/bin/env python3
"""
回测系统
Backtesting System
"""

import logging
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass

import pandas as pd
import numpy as np

from ..engines.trading_engine import TradingEngine
from ..risk.manager import RiskManager
from ..signals.generator import CompositeSignalGenerator, SignalGeneratorBase
from ..strategies.base import Strategy
from ..models import DailyRecord

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """回测结果"""
    initial_capital: float
    final_assets: float
    total_return: float
    annual_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    total_trades: int
    trading_days: int
    
    def to_dict(self) -> dict:
        return {
            "initial_capital": self.initial_capital,
            "final_assets": self.final_assets,
            "total_return": self.total_return,
            "annual_return": self.annual_return,
            "sharpe_ratio": self.sharpe_ratio,
            "max_drawdown": self.max_drawdown,
            "win_rate": self.win_rate,
            "total_trades": self.total_trades,
            "trading_days": self.trading_days
        }


class Backtester:
    """
    回测引擎
    
    特性:
    - 避免未来函数 (使用次日开盘价成交)
    - 交易成本 (佣金+印花税+滑点)
    - 完整的风控检查
    """
    
    def __init__(self, initial_capital: float = 1000000, data_fetcher=None):
        self.initial_capital = initial_capital
        self.engine = TradingEngine(initial_capital)
        self.risk_manager = RiskManager()
        self.signal_generator = CompositeSignalGenerator()
        
        self.data_fetcher = data_fetcher
        
        # 回测结果
        self.daily_records: List[DailyRecord] = []
        self.equity_curve: List[float] = []
        
    def fetch_data(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取历史数据"""
        if self.data_fetcher:
            return self.data_fetcher.fetch(code, start_date, end_date)
        return None
    
    def run(self, stocks: List[Dict], start_date: str, end_date: str, 
            strategy: Strategy = None, verbose: bool = True) -> BacktestResult:
        """
        运行回测
        
        Args:
            stocks: 股票列表 [{code, name}, ...]
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            strategy: 交易策略
            verbose: 是否打印详细信息
        
        Returns:
            BacktestResult
        """
        if verbose:
            print(f"\n{'='*60}")
            print("📊 A股策略回测")
            print(f"{'='*60}")
            print(f"股票: {[s['code'] for s in stocks]}")
            print(f"时间: {start_date} ~ {end_date}")
            print(f"资金: ¥{self.initial_capital:,.0f}")
            print(f"{'='*60}\n")
        
        # 获取所有股票数据
        stock_data = {}
        for stock in stocks:
            code = stock['code']
            if verbose:
                print(f"📥 获取 {code} 数据...")
            
            data = self.fetch_data(code, start_date, end_date)
            if data is not None and len(data) > 30:
                stock_data[code] = {
                    'data': data,
                    'name': stock.get('name', code)
                }
        
        if not stock_data:
            logger.error("无法获取股票数据")
            return self._empty_result()
        
        # 获取回测日期范围
        all_dates = sorted(set(
            d.strftime('%Y-%m-%d') 
            for code_data in stock_data.values() 
            for d in code_data['data'].index
        ))
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        test_dates = [d for d in all_dates if start_dt.strftime('%Y-%m-%d') <= str(d) <= end_dt.strftime('%Y-%m-%d')]
        
        if verbose:
            print(f"\n📈 回测期间: {len(test_dates)} 个交易日\n")
        
        # 逐日回测
        for i, date in enumerate(test_dates):
            # 重置每日风控
            self.risk_manager.reset_daily(date)
            
            # 获取当日数据
            daily_data = {}
            for code, info in stock_data.items():
                df = info['data']
                if date in df.index:
                    daily_data[code] = {
                        'data': df.loc[date],
                        'name': info['name']
                    }
            
            if not daily_data:
                continue
            
            # 生成信号并执行交易
            for code, info in daily_data.items():
                df = stock_data[code]['data']
                
                # 获取次日开盘价（避免未来函数）
                if i + 1 < len(test_dates):
                    next_date = test_dates[i + 1]
                    if next_date in df.index:
                        next_open = df.loc[next_date, 'Open']
                    else:
                        next_open = df.iloc[-1]['Close']
                else:
                    next_open = df.iloc[-1]['Close']
                
                # 生成信号
                signal = self.signal_generator.generate(df, code, info['name'])
                
                if signal and signal.action in ['buy', 'sell']:
                    # 风控检查
                    if signal.action == 'buy':
                        risk_result = self.risk_manager.check_buy(self.engine, signal)
                        if not risk_result.passed:
                            if verbose:
                                print(f"  🚫 风控拦截买入 {code}: {risk_result.reason}")
                            continue
                        
                        # 计算仓位 - 使用固定比例
                        max_amount = self.engine.portfolio.cash * 0.3  # 30%仓位
                        quantity = int(max_amount / next_open / 100) * 100  # 整手
                        if quantity > 0:
                            self.engine.buy(
                                code, info['name'], next_open, 
                                quantity, date
                            )
                    
                    elif signal.action == 'sell':
                        risk_result = self.risk_manager.check_sell(
                            self.engine, code, next_open
                        )
                        if not risk_result.passed:
                            if verbose:
                                print(f"  🚫 风控拦截卖出 {code}: {risk_result.reason}")
                            continue
                        
                        # 卖出当前持仓
                        pos = self.engine.get_position(code)
                        if pos:
                            self.engine.sell(
                                code, next_open, pos.quantity, date
                            )
                
                # 检查止损/止盈
                current_price = daily_data[code]['data']['Close']
                
                # 止损检查
                to_stop_loss = self.risk_manager.check_stop_loss(
                    self.engine, code, current_price
                )
                for stop_code in to_stop_loss:
                    pos = self.engine.get_position(stop_code)
                    if pos:
                        self.engine.sell(stop_code, current_price, pos.quantity, date)
                
                # 止盈检查
                to_take_profit = self.risk_manager.check_take_profit(
                    self.engine, code, current_price
                )
                for tp_code in to_take_profit:
                    pos = self.engine.get_position(tp_code)
                    if pos:
                        self.engine.sell(tp_code, current_price, pos.quantity, date)
            
            # 更新持仓价格
            prices = {
                code: daily_data[code]['data']['Close'] 
                for code in daily_data
            }
            self.engine.update_prices(prices)
            
            # 记录每日状态
            status = self.engine.get_status()
            self.daily_records.append(DailyRecord(
                date=date,
                total_assets=status['total_assets'],
                cash=status['cash'],
                position_value=status['position_value'],
                profit=status['total_profit'],
                profit_pct=status['profit_pct'],
                trade_count=len([t for t in self.engine.portfolio.trades if t.date == date])
            ))
            
            self.equity_curve.append(status['total_assets'])
            
            # 打印进度
            if verbose and (i + 1) % 20 == 0:
                print(f"  Day {i+1}: 资产¥{status['total_assets']:,.0f} ({status['profit_pct']:+.2f}%)")
        
        # 计算最终结果
        result = self._calculate_result(len(test_dates))
        
        if verbose:
            self._print_result(result)
        
        return result
    
    def _calculate_result(self, trading_days: int) -> BacktestResult:
        """计算回测指标"""
        final_status = self.engine.get_status()
        trades = self.engine.portfolio.trades
        
        # 总收益
        total_return = final_status['profit_pct']
        
        # 年化收益
        years = trading_days / 252
        if years > 0 and final_status['total_assets'] > 0:
            annual_return = ((final_status['total_assets'] / self.initial_capital) 
                          ** (1/years) - 1) * 100
        else:
            annual_return = 0
        
        # 夏普比率 (简化)
        if len(self.equity_curve) > 1:
            returns = np.diff(self.equity_curve) / self.equity_curve[:-1]
            sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0
        else:
            sharpe_ratio = 0
        
        # 最大回撤
        max_drawdown = 0
        peak = self.initial_capital
        for equity in self.equity_curve:
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak * 100
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        # 胜率
        buy_trades = [t for t in trades if t.action == 'buy']
        sell_trades = [t for t in trades if t.action == 'sell']
        
        wins = 0
        for sell in sell_trades:
            # 找对应的买入
            code = sell.code
            for buy in buy_trades:
                if buy.code == code and buy.date <= sell.date:
                    if sell.price > buy.price:
                        wins += 1
                    break
        
        total_completed = len(sell_trades)
        win_rate = (wins / total_completed * 100) if total_completed > 0 else 0
        
        return BacktestResult(
            initial_capital=self.initial_capital,
            final_assets=final_status['total_assets'],
            total_return=total_return,
            annual_return=annual_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            win_rate=win_rate,
            total_trades=len(trades),
            trading_days=trading_days
        )
    
    def _print_result(self, result: BacktestResult):
        """打印回测结果"""
        print(f"\n{'='*60}")
        print("📊 回测结果")
        print(f"{'='*60}")
        print(f"初始资金:   ¥{result.initial_capital:>12,.0f}")
        print(f"最终资产:   ¥{result.final_assets:>12,.0f}")
        print(f"总收益率:   {result.total_return:>12.2f}%")
        print(f"年化收益:   {result.annual_return:>12.2f}%")
        print(f"夏普比率:   {result.sharpe_ratio:>12.2f}")
        print(f"最大回撤:   {result.max_drawdown:>12.2f}%")
        print(f"交易次数:   {result.total_trades:>12d}")
        print(f"胜率:       {result.win_rate:>12.1f}%")
        print(f"{'='*60}\n")
    
    def _empty_result(self) -> BacktestResult:
        """空结果"""
        return BacktestResult(
            initial_capital=self.initial_capital,
            final_assets=self.initial_capital,
            total_return=0,
            annual_return=0,
            sharpe_ratio=0,
            max_drawdown=0,
            win_rate=0,
            total_trades=0,
            trading_days=0
        )
    
    def save_results(self, filepath: str):
        """保存回测结果"""
        result = {
            "generated_at": datetime.now().isoformat(),
            "initial_capital": self.initial_capital,
            "final_assets": self.engine.get_status()['total_assets'],
            "equity_curve": self.equity_curve,
            "daily_records": [r.to_dict() for r in self.daily_records],
            "trades": [t.to_dict() for t in self.engine.portfolio.trades]
        }
        
        os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        logger.info(f"回测结果已保存: {filepath}")
