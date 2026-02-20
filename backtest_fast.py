#!/usr/bin/env python3
"""
自适应策略快速回测 - 简化版
2020-2023年回测
"""

import sqlite3
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.strategies import (
    WilliamsStrategy, 
    MomentumReversalStrategy, 
    BollingerStrategy
)
from src.market_regime import MarketRegimeDetectorV2

DB_PATH = "data/stocks.db"


def get_data(ts_codes, start_date, end_date):
    """获取数据"""
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
    
    # 转换列名
    df = df.rename(columns={
        'close': 'Close', 'open': 'Open', 
        'high': 'High', 'low': 'Low', 'vol': 'Volume'
    })
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    return df


def run_backtest(strategy, stock_df, market_df, initial_capital=1000000):
    """快速回测"""
    capital = initial_capital
    positions = {}
    trades = []
    equity = []
    
    trade_dates = sorted(stock_df['trade_date'].unique())
    
    for i, date in enumerate(trade_dates):
        if i % 20 == 0:
            print(f"   进度: {i}/{len(trade_dates)}")
        
        # 当日数据
        day_stocks = stock_df[stock_df['trade_date'] == date]
        day_market = market_df[market_df['trade_date'] == date]
        
        if len(day_market) == 0:
            continue
        
        idx_data = market_df[market_df['trade_date'] <= date].tail(150)
        
        # 判断市场状态
        regime = 'consolidation'
        if hasattr(strategy, 'regime_detector'):
            regime = strategy.regime_detector.detect(idx_data)['regime']
        
        # 获取信号
        signals = []
        for _, row in day_stocks.iterrows():
            ts_code = row['ts_code']
            
            hist = stock_df[(stock_df['ts_code'] == ts_code) & (stock_df['trade_date'] <= date)].tail(150)
            if len(hist) < 60:
                continue
            
            try:
                signal = strategy.generate(hist, ts_code, ts_code, idx_data if hasattr(strategy, 'regime_detector') else None)
            except:
                signal = None
            
            if signal and signal.strength >= 60:
                signals.append((ts_code, signal, row['Close']))
        
        # 交易
        if signals:
            signals.sort(key=lambda x: x[1].strength, reverse=True)
            
            for ts_code, signal, price in signals[:3]:
                if signal.action == "buy" and (ts_code not in positions or positions[ts_code] == 0):
                    shares = int(capital / price / 3)
                    if shares > 0:
                        capital -= price * shares * 1.003
                        positions[ts_code] = {'shares': shares, 'cost': price}
                        trades.append(('BUY', ts_code, price, shares))
                
                elif signal.action == "sell" and ts_code in positions:
                    shares = positions[ts_code]['shares']
                    capital += price * shares * 0.997
                    trades.append(('SELL', ts_code, price, shares))
                    del positions[ts_code]
        
        # 计算权益
        pos_value = sum(p['shares'] * day_stocks[day_stocks['ts_code'] == ts]['Close'].values[0] 
                       for ts, p in positions.items() if len(day_stocks[day_stocks['ts_code'] == ts]) > 0)
        equity.append(capital + pos_value)
    
    # 清仓
    final_prices = stock_df[stock_df['trade_date'] == trade_dates[-1]].set_index('ts_code')['Close'].to_dict()
    for ts, pos in positions.items():
        if ts in final_prices:
            capital += final_prices[ts] * pos['shares'] * 0.997
    
    # 统计
    total_return = (capital - initial_capital) / initial_capital * 100
    years = len(equity) / 252
    annual_return = ((capital / initial_capital) ** (1/years) - 1) * 100 if years > 0 else 0
    
    # 回撤
    equity_s = pd.Series(equity)
    cummax = equity_s.cummax()
    drawdown = (equity_s - cummax) / cummax * 100
    max_dd = abs(drawdown.min())
    avg_dd = drawdown.mean()
    
    # 胜率
    wins, losses = 0, 0
    buy_prices = {}
    for t in trades:
        if t[0] == 'BUY':
            buy_prices[t[1]] = t[2]
        elif t[0] == 'SELL' and t[1] in buy_prices:
            if t[2] > buy_prices[t[1]]:
                wins += 1
            else:
                losses += 1
    
    win_rate = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    
    return {
        'total_return': round(total_return, 2),
        'annual_return': round(annual_return, 2),
        'max_drawdown': round(max_dd, 2),
        'avg_drawdown': round(avg_dd, 2),
        'win_rate': round(win_rate, 2),
        'trades': len(trades)
    }


def main():
    print("="*60)
    print("📊 自适应策略回测 2020-2023")
    print("="*60)
    
    # 获取股票
    conn = sqlite3.connect(DB_PATH)
    stocks = pd.read_sql("""
        SELECT d.ts_code FROM daily d
        WHERE d.trade_date >= '20200101' AND d.trade_date <= '20231231'
        GROUP BY d.ts_code ORDER BY SUM(d.amount) DESC LIMIT 15
    """, conn)['ts_code'].tolist()
    conn.close()
    
    print(f"\n📈 股票: {len(stocks)} 只")
    
    # 获取数据
    print("📥 加载数据...")
    df = get_data(stocks, "20200101", "20231231")
    
    # 市场指数
    market = df.groupby('trade_date').agg({
        'Close': 'mean', 'High': 'mean', 'Low': 'mean', 
        'Open': 'mean', 'Volume': 'sum'
    }).reset_index()
    print(f"   市场: {len(market)} 天")
    
    # 股票数据
    stock_data = df[df['ts_code'].isin(stocks)]
    print(f"   股票数据: {len(stock_data)} 条")
    
    results = []
    
    # 威廉指标
    print("\n🔄 威廉指标策略...")
    strategy = WilliamsStrategy()
    r = run_backtest(strategy, stock_data, market)
    r['strategy'] = '威廉指标'
    results.append(r)
    print(f"   ✅ 收益: {r['annual_return']:.2f}%, 回撤: {r['max_drawdown']:.2f}%")
    
    # 动量反转
    print("\n🔄 动量反转策略...")
    strategy = MomentumReversalStrategy()
    r = run_backtest(strategy, stock_data, market)
    r['strategy'] = '动量反转'
    results.append(r)
    print(f"   ✅ 收益: {r['annual_return']:.2f}%, 回撤: {r['max_drawdown']:.2f}%")
    
    # 布林带
    print("\n🔄 布林带策略...")
    strategy = BollingerStrategy()
    r = run_backtest(strategy, stock_data, market)
    r['strategy'] = '布林带'
    results.append(r)
    print(f"   ✅ 收益: {r['annual_return']:.2f}%, 回撤: {r['max_drawdown']:.2f}%")
    
    # 汇总
    print("\n" + "="*60)
    print("📊 回测结果 (2020-01-01 ~ 2023-12-31)")
    print("="*60)
    print(f"\n{'策略':<10} {'总收益':>10} {'年化收益':>10} {'最大回撤':>10} {'平均回撤':>10} {'胜率':>8} {'交易':>6}")
    print("-"*70)
    
    for r in results:
        print(f"{r['strategy']:<10} {r['total_return']:>9.2f}% {r['annual_return']:>9.2f}% {r['max_drawdown']:>9.2f}% {r['avg_drawdown']:>9.2f}% {r['win_rate']:>7.2f}% {r['trades']:>6}")
    
    print("-"*70)


if __name__ == "__main__":
    main()
