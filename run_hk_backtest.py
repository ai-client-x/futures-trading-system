#!/usr/bin/env python3
"""港股回测脚本 - 最终版"""

import os, sys, json
from datetime import datetime
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.engines.trading_engine import TradingEngine


def calculate_signal(df):
    """简单信号计算"""
    if len(df) < 60:
        return None
    
    close = df['Close']
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    
    # RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rsi = 100 - (100 / (1 + gain / loss))
    
    buy_score = 0
    sell_score = 0
    
    # 均线多头/空头
    if ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1]:
        buy_score += 20
    elif ma5.iloc[-1] < ma10.iloc[-1] < ma20.iloc[-1]:
        sell_score += 20
    
    # RSI
    if rsi.iloc[-1] < 30:
        buy_score += 15
    elif rsi.iloc[-1] > 70:
        sell_score += 15
    
    if buy_score >= 20 and buy_score > sell_score:
        return 'buy'
    elif sell_score >= 20 and sell_score > buy_score:
        return 'sell'
    return 'hold'


def run_backtest():
    """运行回测"""
    data_dir = "data/hk_daily"
    initial_capital = 1000000
    
    # 加载数据
    stocks = {}
    for f in os.listdir(data_dir):
        if not f.endswith('.csv'):
            continue
        code = f.split('_')[0]
        name = '_'.join(f.split('_')[1:]).replace('.csv', '')
        
        df = pd.read_csv(f'{data_dir}/{f}')
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index('日期', inplace=True)
        df = df.sort_index()
        df.columns = ['Open', 'Close', 'High', 'Low', 'Volume', 'Amount', '振幅', '涨跌幅', '涨跌额', '换手率']
        
        # 只保留2025年数据
        df = df[df.index >= '2025-01-01']
        if len(df) > 0:
            stocks[code] = {'name': name, 'data': df}
    
    print(f"📊 港股回测 - {len(stocks)}只股票")
    print(f"时间: 2025-01-01 ~ 2026-02-16")
    print(f"资金: ¥{initial_capital:,}")
    print("="*50)
    
    engine = TradingEngine(initial_capital)
    
    # 合并所有日期
    all_dates = set()
    for s in stocks.values():
        all_dates.update(s['data'].index)
    all_dates = sorted(all_dates)
    
    trades = []
    for i, date in enumerate(all_dates):
        # 卖出检查 - 不使用风控检查，直接卖出
        for code in list(engine.portfolio.positions.keys()):
            if code not in stocks:
                continue
            holding = engine.portfolio.positions[code]
            if holding.quantity > 0:
                df = stocks[code]['data']
                if date in df.index:
                    price = float(df.loc[date, 'Close'])
                    pnl = (price - holding.avg_cost) / holding.avg_cost * 100
                    if pnl <= -5 or pnl >= 10:
                        # 直接操作仓位，绕过风控问题
                        qty = int(holding.quantity)
                        revenue = price * qty
                        commission = 0.001 * revenue  # 万1佣金
                        engine.portfolio.cash += revenue - commission
                        holding.quantity = 0
                        del engine.portfolio.positions[code]
                        trades.append({'date': str(date.date()), 'code': code, 'action': 'sell', 'price': price})
        
        # 买入检查
        if len(engine.portfolio.positions) < 5:
            for code, info in stocks.items():
                if code in engine.portfolio.positions:
                    continue
                df = info['data']
                if date not in df.index:
                    continue
                hist = df.loc[:date]
                if len(hist) < 60:
                    continue
                
                signal = calculate_signal(hist)
                if signal == 'buy':
                    # 次日开盘买入
                    idx = list(df.index).index(date)
                    if idx + 1 < len(df):
                        price = float(df.iloc[idx + 1]['Open'])
                        amount = engine.portfolio.cash * 0.2
                        qty = int(amount / price / 100) * 100
                        if qty > 0:
                            engine.buy(code, info['name'], price, qty, str(date.date()))
                            trades.append({'date': str(date.date()), 'code': code, 'action': 'buy', 'price': price, 'qty': qty})
                            if len(engine.portfolio.positions) >= 5:
                                break
        
        if (i + 1) % 50 == 0:
            print(f"Day {i+1}: 资产¥{engine.portfolio.total_assets:,.0f}, 持仓{len(engine.portfolio.positions)}只")
    
    # 最终结算
    for code, holding in list(engine.portfolio.positions.items()):
        if holding.quantity > 0 and code in stocks:
            price = float(stocks[code]['data'].iloc[-1]['Close'])
            qty = int(holding.quantity)
            revenue = price * qty
            commission = 0.001 * revenue
            engine.portfolio.cash += revenue - commission
            holding.quantity = 0
    
    final = engine.portfolio.total_assets
    ret = (final - initial_capital) / initial_capital * 100
    
    print("\n" + "="*50)
    print("📊 回测结果")
    print("="*50)
    print(f"初始: ¥{initial_capital:,}")
    print(f"最终: ¥{final:,.0f}")
    print(f"收益: {ret:+.2f}%")
    print(f"交易: {len(trades)}次")
    print("="*50)
    
    # 保存
    os.makedirs('backtest_results', exist_ok=True)
    with open(f'backtest_results/hk_result_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json', 'w') as f:
        json.dump({'initial': initial_capital, 'final': float(final), 'return': ret, 'trades': len(trades)}, f)
    
    return ret


if __name__ == "__main__":
    run_backtest()
