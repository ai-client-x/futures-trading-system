#!/usr/bin/env python3
"""
自适应策略回测 - 修复版
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


def calculate_wr(high, low, close, period=14):
    """威廉指标"""
    highest = high.rolling(window=period).max()
    lowest = low.rolling(window=period).min()
    wr = ((highest - close) / (highest - lowest)) * -100
    return wr


def calculate_rsi(close, period=14):
    """RSI"""
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def calculate_bollinger(close, period=20):
    """布林带"""
    middle = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    upper = middle + 2 * std
    lower = middle - 2 * std
    return upper, middle, lower


def williams_signal(df):
    """威廉指标策略信号"""
    if len(df) < 20:
        return None
    
    close = df['Close']
    high = df['High']
    low = df['Low']
    
    wr = calculate_wr(high, low, close)
    
    if len(wr) < 2:
        return None
    
    curr_wr = wr.iloc[-1]
    prev_wr = wr.iloc[-2]
    
    if pd.isna(curr_wr) or pd.isna(prev_wr):
        return None
    
    # 买入信号：WR从超卖区域回升
    if prev_wr <= -90 and curr_wr > -90:
        return {'action': 'buy', 'strength': 70, 'reason': f'WR超卖回升({curr_wr:.1f})'}
    # 卖出信号
    elif curr_wr > -10:
        return {'action': 'sell', 'strength': 70, 'reason': f'WR超买({curr_wr:.1f})'}
    
    return None


def momentum_signal(df):
    """动量反转策略信号"""
    if len(df) < 20:
        return None
    
    close = df['Close']
    
    rsi = calculate_rsi(close)
    
    if len(rsi) < 2:
        return None
    
    curr_rsi = rsi.iloc[-1]
    prev_rsi = rsi.iloc[-2]
    
    if pd.isna(curr_rsi) or pd.isna(prev_rsi):
        return None
    
    # 买入信号：RSI从超卖回升
    if prev_rsi <= 25 and curr_rsi > 25:
        return {'action': 'buy', 'strength': 70, 'reason': f'RSI超卖回升({curr_rsi:.1f})'}
    # 卖出信号
    elif curr_rsi > 70:
        return {'action': 'sell', 'strength': 70, 'reason': f'RSI超买({curr_rsi:.1f})'}
    
    return None


def bollinger_signal(df):
    """布林带策略信号"""
    if len(df) < 25:
        return None
    
    close = df['Close']
    
    upper, middle, lower = calculate_bollinger(close)
    
    if len(upper) < 2:
        return None
    
    curr_price = close.iloc[-1]
    prev_price = close.iloc[-2]
    
    curr_lower = lower.iloc[-1]
    prev_lower = lower.iloc[-2]
    curr_middle = middle.iloc[-1]
    prev_middle = middle.iloc[-2]
    
    if pd.isna(curr_lower) or pd.isna(curr_middle):
        return None
    
    # 买入信号：价格触及下轨或反弹
    if prev_price <= prev_lower and curr_price > curr_lower:
        return {'action': 'buy', 'strength': 70, 'reason': '触及布林下轨反弹'}
    # 价格突破中轨
    elif prev_price <= prev_middle and curr_price > curr_middle:
        return {'action': 'buy', 'strength': 65, 'reason': '突破布林中轨'}
    # 卖出信号
    elif curr_price >= upper.iloc[-1]:
        return {'action': 'sell', 'strength': 70, 'reason': '触及布林上轨'}
    
    return None


def run_backtest(strategy_name, stock_data, initial_capital=1000000):
    """运行回测"""
    print(f"\n🔄 {strategy_name}...")
    
    # 选择信号函数
    if strategy_name == "威廉指标":
        signal_func = williams_signal
    elif strategy_name == "动量反转":
        signal_func = momentum_signal
    else:
        signal_func = bollinger_signal
    
    capital = initial_capital
    positions = {}
    trades = []
    equity = []
    
    # 获取所有交易日
    all_dates = sorted(stock_data['trade_date'].unique())
    
    for i, date in enumerate(all_dates):
        if i % 100 == 0:
            print(f"   进度: {i}/{len(all_dates)}")
        
        # 当日所有股票
        day_data = stock_data[stock_data['trade_date'] == date]
        
        # 获取信号
        signals = []
        for _, row in day_data.iterrows():
            ts_code = row['ts_code']
            
            # 获取历史数据
            hist = stock_data[(stock_data['ts_code'] == ts_code) & (stock_data['trade_date'] <= date)]
            if len(hist) < 30:
                continue
            
            signal = signal_func(hist)
            if signal:
                signals.append((ts_code, signal, row['Close']))
        
        # 执行交易
        if signals:
            # 按强度排序
            signals.sort(key=lambda x: x[1]['strength'], reverse=True)
            
            for ts_code, sig, price in signals[:3]:
                if sig['action'] == 'buy' and (ts_code not in positions or positions[ts_code] == 0):
                    # 买入
                    shares = int(capital / price / 3)  # 最多1/3仓位
                    if shares > 0:
                        cost = price * shares * 1.003
                        capital -= cost
                        positions[ts_code] = {'shares': shares, 'cost': price}
                        trades.append(('BUY', ts_code, price, shares, date))
                
                elif sig['action'] == 'sell' and ts_code in positions:
                    # 卖出
                    pos = positions[ts_code]
                    revenue = price * pos['shares'] * 0.997
                    capital += revenue
                    trades.append(('SELL', ts_code, price, pos['shares'], date))
                    del positions[ts_code]
        
        # 计算权益
        total_value = capital
        for ts_code, pos in positions.items():
            price_row = day_data[day_data['ts_code'] == ts_code]
            if len(price_row) > 0:
                total_value += price_row.iloc[0]['Close'] * pos['shares']
        
        equity.append({'date': date, 'equity': total_value})
    
    # 清仓
    if len(all_dates) > 0:
        final_date = all_dates[-1]
        final_data = stock_data[stock_data['trade_date'] == final_date]
        final_prices = final_data.set_index('ts_code')['Close'].to_dict()
        
        for ts_code, pos in positions.items():
            if ts_code in final_prices:
                capital += final_prices[ts_code] * pos['shares'] * 0.997
    
    # 计算指标
    if not equity:
        return None
    
    equity_df = pd.DataFrame(equity)
    equity_df['equity'] = equity_df['equity'].astype(float)
    
    total_return = (capital - initial_capital) / initial_capital * 100
    years = len(equity) / 252
    annual_return = ((capital / initial_capital) ** (1/years) - 1) * 100 if years > 0 else 0
    
    # 回撤
    equity_df['cummax'] = equity_df['equity'].cummax()
    equity_df['drawdown'] = (equity_df['equity'] - equity_df['cummax']) / equity_df['cummax'] * 100
    max_dd = abs(equity_df['drawdown'].min())
    avg_dd = equity_df['drawdown'].mean()
    
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
    print(f"   数据: {len(df)} 条, {df['trade_date'].nunique()} 天")
    
    results = []
    
    # 威廉指标
    r = run_backtest("威廉指标", df)
    if r:
        r['strategy'] = '威廉指标'
        results.append(r)
    
    # 动量反转
    r = run_backtest("动量反转", df)
    if r:
        r['strategy'] = '动量反转'
        results.append(r)
    
    # 布林带
    r = run_backtest("布林带", df)
    if r:
        r['strategy'] = '布林带'
        results.append(r)
    
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
