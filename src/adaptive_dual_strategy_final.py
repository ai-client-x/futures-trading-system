#!/usr/bin/env python3
"""
市场状态检测与双策略组合
根据不同市场环境(牛市/熊市/震荡市)自动切换最适合的策略

策略配置:
- 牛市: 成交量突破 + MACD+成交量
- 熊市: 动量反转 + 布林带+RSI
- 震荡市: 威廉指标 + 布林带

回测结果 (2016-2019):
- 总收益: 96.97%
- 年化收益: 20.78%
- 最大回撤: 35.99%
- 胜率: 73.91%
"""

import sqlite3
from signal_strength import calc_signal_strength
import pandas as pd
import numpy as np
from typing import List, Tuple
from datetime import datetime

DB_PATH = "data/stocks.db"


def get_stock_data(ts_codes: list, start_date: str, end_date: str) -> pd.DataFrame:
    """获取股票数据"""
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
    
    df = df.rename(columns={
        'close': 'Close', 'open': 'Open', 
        'high': 'High', 'low': 'Low', 'vol': 'Volume'
    })
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date').reset_index(drop=True)
    
    return df


def get_market_data(start_date: str, end_date: str, top_n: int = 10) -> pd.DataFrame:
    """获取市场指数数据"""
    conn = sqlite3.connect(DB_PATH)
    query = f"""
        SELECT d.ts_code
        FROM daily d
        WHERE d.trade_date >= '{start_date}'
        AND d.trade_date <= '{end_date}'
        GROUP BY d.ts_code
        ORDER BY SUM(d.amount) DESC
        LIMIT {top_n}
    """
    stocks = pd.read_sql(query, conn)['ts_code'].tolist()
    conn.close()
    
    if not stocks:
        return pd.DataFrame()
    
    conn = sqlite3.connect(DB_PATH)
    query = f"""
        SELECT trade_date, close
        FROM daily
        WHERE ts_code IN ({','.join([f"'{s}'" for s in stocks])})
        AND trade_date >= '{start_date}'
        AND trade_date <= '{end_date}'
        ORDER BY trade_date
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    market = df.groupby('trade_date')['close'].mean().reset_index()
    market.columns = ['trade_date', 'Close']
    market['trade_date'] = pd.to_datetime(market['trade_date'])
    market = market.sort_values('trade_date').reset_index(drop=True)
    
    market['High'] = market['Close'] * 1.01
    market['Low'] = market['Close'] * 0.99
    market['Open'] = market['Close']
    market['Volume'] = 100000000
    
    return market


def detect_market_regime(market_data: pd.DataFrame, date: pd.Timestamp) -> str:
    """检测市场状态"""
    hist = market_data[market_data['trade_date'] <= date].tail(120)
    
    if len(hist) < 60:
        return 'consolidation'
    
    close = hist['Close']
    ma20 = close.rolling(20).mean().iloc[-1]
    ma60 = close.rolling(60).mean().iloc[-1]
    ma120 = close.rolling(120).mean().iloc[-1]
    
    if pd.isna(ma20) or pd.isna(ma60) or pd.isna(ma120):
        return 'consolidation'
    
    change = (close.iloc[-1] - close.iloc[-20]) / close.iloc[-20] * 100 if len(close) >= 20 else 0
    
    if ma20 > ma60 > ma120 and change > 5:
        return 'bull'
    elif ma20 < ma60 < ma120 and change < -5:
        return 'bear'
    else:
        return 'consolidation'


def get_all_signals(hist: pd.DataFrame, regime: str) -> List[Tuple[str, str, float]]:
    """获取所有策略信号"""
    signals = []
    close, high, low, volume = hist['Close'], hist['High'], hist['Low'], hist['Volume']
    
    # 1. 威廉指标
    try:
        highest = high.rolling(14).max().iloc[-1]
        lowest = low.rolling(14).min().iloc[-1]
        curr_close = close.iloc[-1]
        
        if not pd.isna(highest) and not pd.isna(lowest):
            wr = ((highest - curr_close) / (highest - lowest)) * -100
            prev_wr = ((high.rolling(14).max().iloc[-2] - close.iloc[-2]) / 
                      (high.rolling(14).max().iloc[-2] - low.rolling(14).min().iloc[-2])) * -100
            
            if not pd.isna(wr) and not pd.isna(prev_wr):
                buy_score = calc_signal_strength(hist, '威廉指标', 'buy')
                if prev_wr <= -90 and wr > -90:
                    signals.append(('威廉指标', 'buy', buy_score))
                elif wr > -10:
                    signals.append(('威廉指标', 'sell', 70))
    except:
        pass
    
    # 2. 动量反转
    try:
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))
        
        curr_rsi = rsi.iloc[-1]
        prev_rsi = rsi.iloc[-2]
        
        if not pd.isna(curr_rsi) and not pd.isna(prev_rsi):
            if prev_rsi <= 25 and curr_rsi > 25:
                signals.append(('动量反转', 'buy', 75))
            elif curr_rsi > 70:
                signals.append(('动量反转', 'sell', 70))
    except:
        pass
    
    # 3. 布林带
    try:
        ma = close.rolling(20).mean()
        std = close.rolling(20).std()
        upper = ma + 2 * std
        lower = ma - 2 * std
        
        curr_c = close.iloc[-1]
        prev_c = close.iloc[-2]
        curr_l = lower.iloc[-1]
        prev_l = lower.iloc[-2]
        
        if not pd.isna(curr_l):
            if prev_c <= prev_l and curr_c > curr_l:
                signals.append(('布林带', 'buy', 65))
            elif curr_c >= upper.iloc[-1]:
                signals.append(('布林带', 'sell', 65))
    except:
        pass
    
    # 4. 成交量突破
    try:
        vol_ma = volume.rolling(20).mean().iloc[-1]
        curr_vol = volume.iloc[-1]
        
        if not pd.isna(vol_ma) and vol_ma > 0:
            if curr_vol > vol_ma * 1.5 and close.iloc[-1] > close.iloc[-2]:
                signals.append(('成交量突破', 'buy', 75))
    except:
        pass
    
    # 5. MACD+成交量
    try:
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd = ema12 - ema26
        vol_ma = volume.rolling(20).mean().iloc[-1]
        
        if not pd.isna(macd.iloc[-1]) and not pd.isna(vol_ma):
            if macd.iloc[-1] > 0 and volume.iloc[-1] > vol_ma:
                signals.append(('MACD+成交量', 'buy', 70))
    except:
        pass
    
    # 6. 布林带+RSI
    try:
        ma = close.rolling(20).mean()
        std = close.rolling(20).std()
        lower = (ma - 2 * std).iloc[-1]
        
        if not pd.isna(lower) and close.iloc[-1] < lower:
            signals.append(('布林带+RSI', 'buy', 75))
    except:
        pass
    
    return signals


def get_best_strategies_for_regime(regime: str) -> List[str]:
    """根据市场状态获取最优的2个策略"""
    strategy_map = {
        'bull': ['成交量突破', 'MACD+成交量'],       # 牛市
        'bear': ['动量反转', '布林带+RSI'],          # 熊市
        'consolidation': ['威廉指标', '布林带']       # 震荡市
    }
    return strategy_map.get(regime, ['威廉指标', '布林带'])


def get_regime_name(regime: str) -> str:
    """获取市场状态中文名"""
    names = {
        'bull': '牛市',
        'bear': '熊市',
        'consolidation': '震荡市'
    }
    return names.get(regime, '未知')


class AdaptiveDualStrategy:
    """自适应双策略组合"""
    
    def __init__(self, initial_capital: float = 1000000):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.positions = {}
        self.trades = []
        self.equity_curve = []
        
    def run(self, stock_df: pd.DataFrame, market_df: pd.DataFrame) -> dict:
        """运行回测"""
        self.capital = self.initial_capital
        self.positions = {}
        self.trades = []
        self.equity_curve = []
        
        all_dates = sorted(stock_df['trade_date'].unique())
        
        for date in all_dates:
            # 获取市场状态
            regime = detect_market_regime(market_df, date)
            best_strategies = get_best_strategies_for_regime(regime)
            
            day_data = stock_df[stock_df['trade_date'] == date]
            
            # 收集信号
            all_signals = []
            for _, row in day_data.iterrows():
                ts_code = row['ts_code']
                close = row['Close']
                
                hist = stock_df[(stock_df['ts_code'] == ts_code) & 
                             (stock_df['trade_date'] <= date)]
                
                if len(hist) < 30:
                    continue
                
                signals = get_all_signals(hist, regime)
                
                for sig in signals:
                    if sig[0] in best_strategies:
                        all_signals.append((ts_code, sig[0], sig[1], sig[2], close))
            
            # 按强度排序，选择信号
            if all_signals:
                all_signals.sort(key=lambda x: x[3], reverse=True)
                
                used_strategies = set()
                for ts_code, strat, action, strength, close in all_signals:
                    if strat in used_strategies:
                        continue
                    
                    if action == 'buy':
                        if ts_code not in self.positions or self.positions[ts_code] == 0:
                            shares = int(self.capital / close / 2)
                            if shares > 0:
                                self.capital -= close * shares * 1.003
                                self.positions[ts_code] = {
                                    'shares': shares, 
                                    'cost': close, 
                                    'strategy': strat
                                }
                                self.trades.append({
                                    'date': date,
                                    'action': 'BUY',
                                    'ts_code': ts_code,
                                    'price': close,
                                    'shares': shares,
                                    'strategy': strat
                                })
                                used_strategies.add(strat)
                    
                    elif action == 'sell' and ts_code in self.positions:
                        pos = self.positions[ts_code]
                        self.capital += close * pos['shares'] * 0.997
                        self.trades.append({
                            'date': date,
                            'action': 'SELL',
                            'ts_code': ts_code,
                            'price': close,
                            'shares': pos['shares'],
                            'strategy': pos['strategy']
                        })
                        del self.positions[ts_code]
            
            # 计算权益
            total = self.capital
            for ts, pos in self.positions.items():
                price = day_data[day_data['ts_code'] == ts]['Close'].values
                if len(price) > 0:
                    total += price[0] * pos['shares']
            self.equity_curve.append({
                'date': date,
                'equity': total
            })
        
        # 清仓
        final_date = all_dates[-1]
        final_data = stock_df[stock_df['trade_date'] == final_date]
        for ts, pos in self.positions.items():
            price = final_data[final_data['ts_code'] == ts]['Close'].values
            if len(price) > 0:
                self.capital += price[0] * pos['shares'] * 0.997
        
        return self._calculate_metrics()
    
    def _calculate_metrics(self) -> dict:
        """计算回测指标"""
        if not self.equity_curve:
            return {}
        
        equity_df = pd.DataFrame(self.equity_curve)
        equity_df['equity'] = equity_df['equity'].astype(float)
        
        # 收益率
        total_return = (self.capital - self.initial_capital) / self.initial_capital * 100
        years = len(equity_df) / 252
        annual_return = ((self.capital / self.initial_capital) ** (1 / years) - 1) * 100 if years > 0 else 0
        
        # 回撤
        equity_df['cummax'] = equity_df['equity'].cummax()
        equity_df['drawdown'] = (equity_df['equity'] - equity_df['cummax']) / equity_df['cummax'] * 100
        max_drawdown = abs(equity_df['drawdown'].min())
        avg_drawdown = equity_df['drawdown'].mean()
        
        # 胜率
        wins, losses = 0, 0
        buy_prices = {}
        for trade in self.trades:
            if trade['action'] == 'BUY':
                buy_prices[trade['ts_code']] = trade['price']
            elif trade['action'] == 'SELL' and trade['ts_code'] in buy_prices:
                if trade['price'] > buy_prices[trade['ts_code']]:
                    wins += 1
                else:
                    losses += 1
        
        win_rate = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
        
        return {
            'total_return': round(total_return, 2),
            'annual_return': round(annual_return, 2),
            'max_drawdown': round(max_drawdown, 2),
            'avg_drawdown': round(avg_drawdown, 2),
            'win_rate': round(win_rate, 2),
            'total_trades': len(self.trades)
        }


def run_backtest(start_date: str = '20160101', end_date: str = '20191231',
                initial_capital: float = 1000000, top_n: int = 10) -> dict:
    """运行回测"""
    # 获取股票列表
    conn = sqlite3.connect(DB_PATH)
    stocks = pd.read_sql(f"""
        SELECT d.ts_code
        FROM daily d
        WHERE d.trade_date >= '{start_date}'
        AND d.trade_date <= '{end_date}'
        GROUP BY d.ts_code
        ORDER BY SUM(d.amount) DESC
        LIMIT {top_n}
    """, conn)['ts_code'].tolist()
    conn.close()
    
    # 获取市场数据
    market_df = get_market_data(start_date, end_date, top_n)
    
    # 获取股票数据
    stock_df = get_stock_data(stocks, start_date, end_date)
    
    # 运行回测
    strategy = AdaptiveDualStrategy(initial_capital)
    result = strategy.run(stock_df, market_df)
    
    return result


if __name__ == "__main__":
    print("=" * 70)
    print("自适应双策略组合回测")
    print("=" * 70)
    
    result = run_backtest('20160101', '20191231')
    
    print(f"\n策略配置:")
    print(f"  牛市: 成交量突破 + MACD+成交量")
    print(f"  熊市: 动量反转 + 布林带+RSI")
    print(f"  震荡: 威廉指标 + 布林带")
    
    print(f"\n回测结果 (2016-01-01 ~ 2019-12-31):")
    print(f"  总收益: {result['total_return']:.2f}%")
    print(f"  年化收益: {result['annual_return']:.2f}%")
    print(f"  最大回撤: {result['max_drawdown']:.2f}%")
    print(f"  平均回撤: {result['avg_drawdown']:.2f}%")
    print(f"  胜率: {result['win_rate']:.2f}%")
    print(f"  交易次数: {result['total_trades']}")
