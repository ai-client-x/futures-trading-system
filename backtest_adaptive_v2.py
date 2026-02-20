#!/usr/bin/env python3
"""
自适应策略回测 - 包含市场判断
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


def get_market_regime(market_data, date):
    """判断市场状态"""
    hist = market_data[market_data['trade_date'] <= date].tail(120)
    if len(hist) < 60:
        return 'consolidation'
    
    close = hist['Close']
    
    # 均线
    ma20 = close.rolling(20).mean().iloc[-1]
    ma60 = close.rolling(60).mean().iloc[-1]
    ma120 = close.rolling(120).mean().iloc[-1]
    
    if pd.isna(ma20) or pd.isna(ma60) or pd.isna(ma120):
        return 'consolidation'
    
    # 20日涨跌
    change = (close.iloc[-1] - close.iloc[-20]) / close.iloc[-20] * 100 if len(close) >= 20 else 0
    
    # 牛市：均线多头 + 上涨
    if ma20 > ma60 > ma120 and change > 5:
        return 'bull'
    # 熊市：均线空头 + 下跌
    elif ma20 < ma60 < ma120 and change < -5:
        return 'bear'
    else:
        return 'consolidation'


def run_adaptive_backtest(stock_data, market_data, initial_capital=1000000):
    """自适应策略回测"""
    print(f"\n🔄 自适应策略...")
    
    capital = initial_capital
    positions = {}
    trades = []
    equity_curve = []
    
    all_dates = sorted(stock_data['trade_date'].unique())
    
    for date in all_dates:
        # 获取市场状态
        regime = get_market_regime(market_data, date)
        
        day_data = stock_data[stock_data['trade_date'] == date]
        
        # 根据市场状态选择策略
        for _, row in day_data.iterrows():
            ts_code = row['ts_code']
            close = row['Close']
            
            hist = stock_data[(stock_data['ts_code'] == ts_code) & (stock_data['trade_date'] <= date)]
            if len(hist) < 30:
                continue
            
            signal = None
            
            # 牛市：威廉指标
            if regime == 'bull':
                highest = hist['High'].rolling(14).max().iloc[-1]
                lowest = hist['Low'].rolling(14).min().iloc[-1]
                curr_close = hist['Close'].iloc[-1]
                if not pd.isna(highest) and not pd.isna(lowest):
                    wr = ((highest - curr_close) / (highest - lowest)) * -100
                    prev_wr = ((hist['High'].rolling(14).max().iloc[-2] - hist['Close'].iloc[-2]) / 
                              (hist['High'].rolling(14).max().iloc[-2] - hist['Low'].rolling(14).min().iloc[-2])) * -100
                    
                    if not pd.isna(wr) and not pd.isna(prev_wr):
                        if prev_wr <= -90 and wr > -90:
                            signal = 'buy'
                        elif wr > -10:
                            signal = 'sell'
            
            # 熊市：动量反转
            elif regime == 'bear':
                delta = hist['Close'].diff()
                gain = delta.where(delta > 0, 0).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                
                curr_rsi = rsi.iloc[-1]
                prev_rsi = rsi.iloc[-2]
                
                if not pd.isna(curr_rsi) and not pd.isna(prev_rsi):
                    if prev_rsi <= 25 and curr_rsi > 25:
                        signal = 'buy'
                    elif curr_rsi > 70:
                        signal = 'sell'
            
            # 震荡市：布林带
            else:
                ma = hist['Close'].rolling(20).mean()
                std = hist['Close'].rolling(20).std()
                upper = ma + 2 * std
                lower = ma - 2 * std
                
                curr_close = hist['Close'].iloc[-1]
                prev_close = hist['Close'].iloc[-2]
                curr_lower = lower.iloc[-1]
                prev_lower = lower.iloc[-2]
                
                if not pd.isna(curr_lower):
                    if prev_close <= prev_lower and curr_close > curr_lower:
                        signal = 'buy'
                    elif curr_close >= upper.iloc[-1]:
                        signal = 'sell'
            
            # 执行交易
            if signal == 'buy':
                if ts_code not in positions or positions[ts_code] == 0:
                    shares = int(capital / close / 3)
                    if shares > 0:
                        capital -= close * shares * 1.003
                        positions[ts_code] = {'shares': shares, 'cost': close}
                        trades.append(('BUY', ts_code, close, regime))
            
            elif signal == 'sell' and ts_code in positions:
                pos = positions[ts_code]
                capital += close * pos['shares'] * 0.997
                trades.append(('SELL', ts_code, close, regime))
                del positions[ts_code]
        
        # 权益
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
        GROUP BY d.ts_code ORDER BY SUM(d.amount) DESC LIMIT 10
    """, conn)['ts_code'].tolist()
    conn.close()
    
    print(f"\n📈 股票: {len(stocks)} 只")
    
    # 获取数据
    print("📥 加载数据...")
    df = get_data(stocks, "20200101", "20231231")
    print(f"   数据: {len(df)} 条")
    
    # 市场数据（用所有股票的平均）
    market = df.groupby('trade_date').agg({
        'Close': 'mean', 'High': 'mean', 'Low': 'mean'
    }).reset_index()
    market = market.sort_values('trade_date').reset_index(drop=True)
    print(f"   市场: {len(market)} 天")
    
    # 运行自适应策略
    result = run_adaptive_backtest(df, market)
    result['strategy'] = '自适应策略'
    
    # 汇总
    print("\n" + "="*60)
    print("📊 回测结果汇总 (2020-01-01 ~ 2023-12-31)")
    print("="*60)
    print(f"\n{'策略':<12} {'总收益':>10} {'年化收益':>10} {'最大回撤':>10} {'胜率':>8} {'交易':>6}")
    print("-"*60)
    
    # 之前的结果
    print(f"{'威廉指标':<12} {'7.51%':>10} {'1.90%':>10} {'30.09%':>10} {'58.96%':>8} {'349':>6}")
    print(f"{'动量反转':<12} {'-30.06%':>10} {'-8.87%':>10} {'48.87%':>10} {'60.00%':>8} {'158':>6}")
    print(f"{'布林带':<12} {'-13.92%':>10} {'-3.82%':>10} {'34.33%':>10} {'52.44%':>8} {'172':>6}")
    print(f"{result['strategy']:<12} {result['total_return']:>9.2f}% {result['annual_return']:>9.2f}% {result['max_drawdown']:>9.2f}% {result['win_rate']:>7.2f}% {result['trades']:>6}")
    
    print("-"*60)


if __name__ == "__main__":
    main()
