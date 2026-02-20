#!/usr/bin/env python3
"""
自适应策略回测脚本 - 优化版
测试2020-2023年的市场表现
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.strategies import (
    AdaptiveStrategy, 
    WilliamsStrategy, 
    MomentumReversalStrategy, 
    BollingerStrategy
)
from src.market_regime import MarketRegimeDetectorV2

DB_PATH = "data/stocks.db"


def get_stock_data_batch(ts_codes: list, start_date: str, end_date: str) -> dict:
    """批量获取股票数据"""
    conn = sqlite3.connect(DB_PATH)
    
    query = f"""
        SELECT ts_code, trade_date, open, high, low, close, vol
        FROM daily
        WHERE ts_code IN ({','.join([f"'{c}'" for c in ts_codes])})
        AND trade_date >= '{start_date}'
        AND trade_date <= '{end_date}'
        ORDER BY ts_code, trade_date
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # 转换为dict
    result = {}
    for ts_code in ts_codes:
        stock_df = df[df['ts_code'] == ts_code].copy()
        if len(stock_df) > 0:
            stock_df['trade_date'] = pd.to_datetime(stock_df['trade_date'])
            stock_df = stock_df.sort_values('trade_date').reset_index(drop=True)
            stock_df = stock_df.rename(columns={'vol': 'Volume', 'close': 'Close', 'open': 'Open', 'high': 'High', 'low': 'Low'})
            result[ts_code] = stock_df
    
    return result


def get_market_index(start_date: str, end_date: str) -> pd.DataFrame:
    """获取市场指数（使用大盘股模拟）"""
    conn = sqlite3.connect(DB_PATH)
    
    # 获取成交额前10的股票
    query = f"""
        SELECT d.ts_code
        FROM daily d
        WHERE d.trade_date >= '{start_date}'
        AND d.trade_date <= '{end_date}'
        GROUP BY d.ts_code
        ORDER BY SUM(d.amount) DESC
        LIMIT 10
    """
    
    large_stocks = pd.read_sql(query, conn)['ts_code'].tolist()
    conn.close()
    
    print(f"   使用 {len(large_stocks)} 只大盘股模拟市场")
    
    # 获取数据
    stock_data = get_stock_data_batch(large_stocks, start_date, end_date)
    
    # 计算等权平均
    market_data = []
    all_dates = set()
    for df in stock_data.values():
        all_dates.update(df['trade_date'].tolist())
    
    all_dates = sorted(all_dates)
    
    for date in all_dates:
        prices = []
        for df in stock_data.values():
            row = df[df['trade_date'] == date]
            if len(row) > 0:
                prices.append(row.iloc[0]['Close'])
        
        if prices:
            market_data.append({
                'trade_date': date,
                'Close': np.mean(prices),
                'High': np.mean(prices) * 1.01,
                'Low': np.mean(prices) * 0.99,
                'Open': np.mean(prices),
                'Volume': 100000000
            })
    
    return pd.DataFrame(market_data)


def run_backtest(strategy_name: str, stock_data: dict, market_df: pd.DataFrame, initial_capital: float = 1000000):
    """运行回测"""
    print(f"\n   回测: {strategy_name}")
    
    # 创建策略
    if strategy_name == "威廉指标":
        strategy = WilliamsStrategy()
    elif strategy_name == "动量反转":
        strategy = MomentumReversalStrategy()
    elif strategy_name == "布林带":
        strategy = BollingerStrategy()
    elif strategy_name == "自适应策略":
        strategy = AdaptiveStrategy()
        regime_detector = MarketRegimeDetectorV2()
    else:
        return None
    
    # 初始化
    capital = initial_capital
    positions = {}
    trades = []
    equity_curve = []
    
    # 获取交易日期
    trade_dates = sorted(set(market_df['trade_date']) & set(range(1000)))
    market_df = market_df.set_index('trade_date')
    
    # 每日回测
    for i, date in enumerate(market_df.index):
        if i % 50 == 0:
            print(f"   进度: {i}/{len(market_df)}")
        
        # 获取当日市场数据
        idx_data = market_df.loc[:date].tail(150)
        if len(idx_data) < 120:
            equity = capital
            equity_curve.append({'date': date, 'equity': equity})
            continue
        
        # 判断市场状态
        regime = 'consolidation'
        if strategy_name == "自适应策略":
            regime = regime_detector.detect(idx_data)['regime']
        
        # 获取当前指数价格
        current_idx_price = idx_data.iloc[-1]['Close']
        
        # 获取股票价格
        prices = {}
        signals = []
        
        for ts_code, df in stock_data.items():
            if len(df) < 60:
                continue
            
            stock_history = df[df['trade_date'] <= date].tail(150)
            if len(stock_history) < 60:
                continue
            
            current_price = stock_history.iloc[-1]['Close']
            prices[ts_code] = current_price
            
            # 生成信号
            try:
                if strategy_name == "自适应策略":
                    signal = strategy.generate(stock_history, ts_code, ts_code, idx_data)
                else:
                    signal = strategy.generate(stock_history, ts_code, ts_code)
            except:
                signal = None
            
            if signal and signal.strength >= 60:
                signals.append((ts_code, signal, current_price))
        
        # 执行交易
        if signals:
            signals.sort(key=lambda x: x[1].strength, reverse=True)
            
            for ts_code, signal, price in signals[:3]:
                if signal.action == "buy" and (ts_code not in positions or positions[ts_code] == 0):
                    shares = int(capital / price / 3)
                    if shares > 0:
                        cost = price * shares * 1.003
                        capital -= cost
                        positions[ts_code] = {'shares': shares, 'cost': price}
                        trades.append(('BUY', ts_code, price, shares))
                
                elif signal.action == "sell" and ts_code in positions:
                    shares = positions[ts_code]['shares']
                    revenue = price * shares * 0.997
                    capital += revenue
                    trades.append(('SELL', ts_code, price, shares))
                    del positions[ts_code]
        
        # 计算权益
        position_value = sum(p['shares'] * prices.get(ts_code, 0) for ts_code, p in positions.items())
        equity = capital + position_value
        equity_curve.append({'date': date, 'equity': equity})
    
    # 清仓
    for ts_code, pos in positions.items():
        if ts_code in prices:
            revenue = prices[ts_code] * pos['shares'] * 0.997
            capital += revenue
            trades.append(('SELL', ts_code, prices[ts_code], pos['shares']))
    
    final_equity = capital
    
    # 计算指标
    if not equity_curve:
        return None
    
    equity_df = pd.DataFrame(equity_curve)
    equity_df['equity'] = equity_df['equity'].astype(float)
    
    # 收益率
    total_return = (final_equity - initial_capital) / initial_capital * 100
    years = len(equity_df) / 252
    annual_return = ((final_equity / initial_capital) ** (1/years) - 1) * 100 if years > 0 else 0
    
    # 回撤
    equity_df['cummax'] = equity_df['equity'].cummax()
    equity_df['drawdown'] = (equity_df['equity'] - equity_df['cummax']) / equity_df['cummax'] * 100
    max_drawdown = abs(equity_df['drawdown'].min())
    avg_drawdown = equity_df['drawdown'].mean()
    
    # 胜率
    win_trades = 0
    loss_trades = 0
    buy_prices = {}
    for trade in trades:
        if trade[0] == 'BUY':
            buy_prices[trade[1]] = trade[2]
        elif trade[0] == 'SELL' and trade[1] in buy_prices:
            if trade[2] > buy_prices[trade[1]]:
                win_trades += 1
            else:
                loss_trades += 1
    
    win_rate = win_trades / (win_trades + loss_trades) * 100 if (win_trades + loss_trades) > 0 else 0
    
    print(f"   ✅ {strategy_name}: 总收益{total_return:.2f}%, 年化{annual_return:.2f}%, 最大回撤{max_drawdown:.2f}%, 胜率{win_rate:.2f}%")
    
    return {
        'strategy': strategy_name,
        'total_return': round(total_return, 2),
        'annual_return': round(annual_return, 2),
        'max_drawdown': round(max_drawdown, 2),
        'avg_drawdown': round(avg_drawdown, 2),
        'win_rate': round(win_rate, 2),
        'total_trades': len(trades)
    }


def main():
    print("="*60)
    print("📊 自适应策略回测 2020-2023")
    print("="*60)
    
    start_date = "20200101"
    end_date = "20231231"
    initial_capital = 1000000
    
    # 获取市场指数
    print("\n📈 获取市场数据...")
    market_df = get_market_index(start_date, end_date)
    print(f"   市场指数: {len(market_df)} 天")
    
    # 获取测试股票（成交额前20）
    conn = sqlite3.connect(DB_PATH)
    stocks = pd.read_sql(f"""
        SELECT d.ts_code
        FROM daily d
        WHERE d.trade_date >= '{start_date}'
        AND d.trade_date <= '{end_date}'
        GROUP BY d.ts_code
        ORDER BY SUM(d.amount) DESC
        LIMIT 20
    """, conn)
    conn.close()
    
    test_stocks = stocks['ts_code'].tolist()
    print(f"   测试股票: {len(test_stocks)} 只")
    
    # 批量获取股票数据
    print("\n📥 加载股票数据...")
    stock_data = get_stock_data_batch(test_stocks, start_date, end_date)
    print(f"   有效股票: {len(stock_data)} 只")
    
    # 运行回测
    results = []
    
    for name in ["威廉指标", "动量反转", "布林带", "自适应策略"]:
        result = run_backtest(name, stock_data, market_df, initial_capital)
        if result:
            results.append(result)
    
    # 汇总
    print("\n" + "="*60)
    print("📊 回测结果汇总 (2020-01-01 ~ 2023-12-31)")
    print("="*60)
    print(f"\n{'策略':<12} {'总收益':>10} {'年化收益':>10} {'最大回撤':>10} {'平均回撤':>10} {'胜率':>8} {'交易次数':>8}")
    print("-"*70)
    
    for r in results:
        print(f"{r['strategy']:<12} {r['total_return']:>9.2f}% {r['annual_return']:>9.2f}% {r['max_drawdown']:>9.2f}% {r['avg_drawdown']:>9.2f}% {r['win_rate']:>7.2f}% {r['total_trades']:>8}")
    
    print("-"*70)


if __name__ == "__main__":
    main()
