#!/usr/bin/env python3
"""
快速回测 - 精简版
"""

import sqlite3
import pandas as pd
import numpy as np

DB_PATH = "data/stocks.db"


def get_data(ts_codes, start_date, end_date):
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
    
    df = df.rename(columns={'close': 'Close', 'open': 'Open', 'high': 'High', 'low': 'Low', 'vol': 'Volume'})
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    return df


def run_simple_backtest(strategy_name, stock_data, initial_capital=1000000):
    """简化回测"""
    print(f"\n🔄 {strategy_name}...")
    
    capital = initial_capital
    positions = {}
    trades = []
    equity_curve = []
    
    all_dates = sorted(stock_data['trade_date'].unique())
    
    for date in all_dates:
        day_data = stock_data[stock_data['trade_date'] == date]
        
        # 遍历每只股票检查信号
        for _, row in day_data.iterrows():
            ts_code = row['ts_code']
            close = row['Close']
            
            # 获取历史
            hist = stock_data[(stock_data['ts_code'] == ts_code) & (stock_data['trade_date'] <= date)]
            if len(hist) < 30:
                continue
            
            # 计算信号
            if strategy_name == "威廉指标":
                # WR
                highest = hist['High'].rolling(14).max().iloc[-1]
                lowest = hist['Low'].rolling(14).min().iloc[-1]
                curr_close = hist['Close'].iloc[-1]
                if pd.isna(highest) or pd.isna(lowest):
                    continue
                wr = ((highest - curr_close) / (highest - lowest)) * -100
                
                prev_highest = hist['High'].rolling(14).max().iloc[-2]
                prev_lowest = hist['Low'].rolling(14).min().iloc[-2]
                prev_close = hist['Close'].iloc[-2]
                if pd.isna(prev_highest) or pd.isna(prev_lowest):
                    continue
                prev_wr = ((prev_highest - prev_close) / (prev_highest - prev_lowest)) * -100
                
                if pd.isna(wr) or pd.isna(prev_wr):
                    continue
                
                # 买入
                if prev_wr <= -90 and wr > -90:
                    if ts_code not in positions or positions[ts_code] == 0:
                        shares = int(capital / close / 3)
                        if shares > 0:
                            capital -= close * shares * 1.003
                            positions[ts_code] = {'shares': shares, 'cost': close}
                            trades.append(('BUY', ts_code, close))
                # 卖出
                elif wr > -10 and ts_code in positions:
                    pos = positions[ts_code]
                    capital += close * pos['shares'] * 0.997
                    trades.append(('SELL', ts_code, close))
                    del positions[ts_code]
            
            elif strategy_name == "动量反转":
                # RSI
                delta = hist['Close'].diff()
                gain = delta.where(delta > 0, 0).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                
                curr_rsi = rsi.iloc[-1]
                prev_rsi = rsi.iloc[-2]
                
                if pd.isna(curr_rsi) or pd.isna(prev_rsi):
                    continue
                
                if prev_rsi <= 25 and curr_rsi > 25:
                    if ts_code not in positions or positions[ts_code] == 0:
                        shares = int(capital / close / 3)
                        if shares > 0:
                            capital -= close * shares * 1.003
                            positions[ts_code] = {'shares': shares, 'cost': close}
                            trades.append(('BUY', ts_code, close))
                elif curr_rsi > 70 and ts_code in positions:
                    pos = positions[ts_code]
                    capital += close * pos['shares'] * 0.997
                    trades.append(('SELL', ts_code, close))
                    del positions[ts_code]
            
            elif strategy_name == "布林带":
                # BB
                ma = hist['Close'].rolling(20).mean()
                std = hist['Close'].rolling(20).std()
                upper = ma + 2 * std
                lower = ma - 2 * std
                
                curr_close = hist['Close'].iloc[-1]
                prev_close = hist['Close'].iloc[-2]
                curr_lower = lower.iloc[-1]
                prev_lower = lower.iloc[-2]
                
                if pd.isna(curr_lower):
                    continue
                
                if prev_close <= prev_lower and curr_close > curr_lower:
                    if ts_code not in positions or positions[ts_code] == 0:
                        shares = int(capital / close / 3)
                        if shares > 0:
                            capital -= close * shares * 1.003
                            positions[ts_code] = {'shares': shares, 'cost': close}
                            trades.append(('BUY', ts_code, close))
                elif curr_close >= upper.iloc[-1] and ts_code in positions:
                    pos = positions[ts_code]
                    capital += close * pos['shares'] * 0.997
                    trades.append(('SELL', ts_code, close))
                    del positions[ts_code]
        
        # 计算权益
        total = capital
        for ts, pos in positions.items():
            price = day_data[day_data['ts_code'] == ts]['Close'].values
            if len(price) > 0:
                total += price[0] * pos['shares']
        equity_curve.append(total)
    
    # 清仓
    final_date = all_dates[-1]
    final_data = stock_data[stock_data['trade_date'] == final_date]
    for ts, pos in positions.items():
        price = final_data[final_data['ts_code'] == ts]['Close'].values
        if len(price) > 0:
            capital += price[0] * pos['shares'] * 0.997
    
    # 统计
    total_return = (capital - initial_capital) / initial_capital * 100
    years = len(all_dates) / 252
    annual_return = ((capital / initial_capital) ** (1/years) - 1) * 100 if years > 0 else 0
    
    equity_s = pd.Series(equity_curve)
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
    
    print(f"   ✅ 收益: {annual_return:.2f}%, 回撤: {max_dd:.2f}%, 交易: {len(trades)}")
    
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
    print("📊 策略回测 2020-2023")
    print("="*60)
    
    # 获取股票 - 减少到10只
    conn = sqlite3.connect(DB_PATH)
    stocks = pd.read_sql("""
        SELECT d.ts_code FROM daily d
        WHERE d.trade_date >= '20200101' AND d.trade_date <= '20231231'
        GROUP BY d.ts_code ORDER BY SUM(d.amount) DESC LIMIT 10
    """, conn)['ts_code'].tolist()
    conn.close()
    
    print(f"\n📈 股票: {len(stocks)} 只")
    
    # 获取数据
    print("📥 加载数据...")
    df = get_data(stocks, "20200101", "20231231")
    print(f"   数据: {len(df)} 条")
    
    results = []
    
    for name in ["威廉指标", "动量反转", "布林带"]:
        r = run_simple_backtest(name, df)
        r['strategy'] = name
        results.append(r)
    
    # 汇总
    print("\n" + "="*60)
    print("📊 回测结果 (2020-01-01 ~ 2023-12-31)")
    print("="*60)
    print(f"\n{'策略':<10} {'总收益':>10} {'年化收益':>10} {'最大回撤':>10} {'胜率':>8} {'交易':>6}")
    print("-"*60)
    
    for r in results:
        print(f"{r['strategy']:<10} {r['total_return']:>9.2f}% {r['annual_return']:>9.2f}% {r['max_drawdown']:>9.2f}% {r['win_rate']:>7.2f}% {r['trades']:>6}")
    
    print("-"*60)


if __name__ == "__main__":
    main()
