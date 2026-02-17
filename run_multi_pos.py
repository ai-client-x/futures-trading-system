"""多持仓动态选股池回测"""
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import json
import logging
import os
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 配置
CONFIG_PATH = 'config/trading.yaml'
with open(CONFIG_PATH) as f:
    cfg = yaml.safe_load(f)

DB_PATH = 'data/stocks.db'
INITIAL_CAPITAL = cfg['initial_capital']
BACKTEST_START = cfg['backtest']['start']
BACKTEST_END = cfg['backtest']['end']

# 风控参数
STOP_LOSS = cfg['stop_loss_pct'] / 100
TAKE_PROFIT = cfg['take_profit_pct'] / 100
MAX_POSITIONS = cfg.get('max_positions', 3)

# 基本面筛选
SCREEN = cfg.get('fundamentals', {})

class Backtester:
    def __init__(self):
        self.initial_capital = INITIAL_CAPITAL
        
    def _conn(self):
        return sqlite3.connect(DB_PATH)
    
    def get_stock_pool(self, date: str):
        """从数据库获取符合条件的股票池"""
        conn = self._conn()
        
        # 基本面筛选
        pe_thres = SCREEN.get('max_pe', 25)
        roe_thres = SCREEN.get('min_roe', 10)
        dv_thres = SCREEN.get('min_dv_ratio', 1)
        debt_thres = SCREEN.get('max_debt', 70)
        # cap_thres in billions
        
        df = pd.read_sql(f"""
            SELECT ts_code, name, pe, roe, dv_ratio, debt_to_assets, market_cap
            FROM fundamentals
            WHERE pe > 0 AND pe <= {pe_thres}
              AND roe >= {roe_thres}
              AND dv_ratio >= {dv_thres}
              AND debt_to_assets <= {debt_thres}
              AND market_cap >= 30
            ORDER BY roe DESC
            LIMIT 50
        """, conn)
        conn.close()
        return df.to_dict('records')
    
    def get_price_series(self, code, start_date, end_date):
        conn = self._conn()
        df = pd.read_sql(f"""
            SELECT trade_date, open, high, low, close, vol
            FROM daily
            WHERE ts_code = '{code}' AND trade_date <= '{end_date}'
            ORDER BY trade_date DESC
            LIMIT 120
        """, conn)
        conn.close()
        if len(df) > 60:
            return df.sort_values('trade_date')
        return None
    
    def get_latest_price(self, code, date):
        conn = self._conn()
        df = pd.read_sql(f"""
            SELECT close FROM daily
            WHERE ts_code = '{code}' AND trade_date <= '{date}'
            ORDER BY trade_date DESC LIMIT 1
        """, conn)
        conn.close()
        return df['close'].iloc[0] if len(df) > 0 else None
    
    def run(self, strategy_name, signal_func, start_date, end_date):
        logger.info(f"运行策略: {strategy_name}")
        
        capital = self.initial_capital
        positions = []  # 多持仓: [{code, qty, cost, name}, ...]
        trades = []
        current_pool = []
        last_pool_date = None
        
        # 最大回撤
        peak = self.initial_capital
        max_dd = 0
        
        conn = self._conn()
        trade_dates = pd.read_sql(f"""
            SELECT DISTINCT trade_date FROM daily
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            ORDER BY trade_date
        """, conn)['trade_date'].tolist()
        conn.close()
        
        for i, date in enumerate(trade_dates):
            month = date[:6]
            if month != last_pool_date:
                current_pool = self.get_stock_pool(date)
                last_pool_date = month
            
            if not current_pool:
                continue
            
            # === 卖出检查 ===
            to_sell = []
            for pos in positions:
                df = self.get_price_series(pos['code'], start_date, date)
                if df is not None and len(df) >= 20:
                    sig = signal_func(df)
                    price = df.iloc[-1]['close']
                    if price <= pos['cost'] * (1 - STOP_LOSS) or \
                       price >= pos['cost'] * (1 + TAKE_PROFIT) or \
                       sig == 'sell':
                        to_sell.append((pos, price))
            
            for pos, price in to_sell:
                revenue = price * pos['qty'] * 0.998  # 简化佣金
                capital += revenue
                trades.append({'date': date, 'action': 'sell', 'price': price, 'code': pos['code']})
                positions = [p for p in positions if p['code'] != pos['code']]
            
            # === 买入检查 ===
            if len(positions) < MAX_POSITIONS and capital > 0:
                alloc = capital / (MAX_POSITIONS - len(positions))
                
                for stock in current_pool[:15]:
                    if len(positions) >= MAX_POSITIONS:
                        break
                    
                    code = stock['ts_code']
                    if any(p['code'] == code for p in positions):
                        continue
                    
                    df = self.get_price_series(code, start_date, date)
                    if df is not None and len(df) >= 60:
                        sig = signal_func(df)
                        if sig == 'buy':
                            price = df.iloc[-1]['close']
                            qty = int(alloc / price / 100) * 100
                            if qty > 0:
                                cost = price * qty * 1.001
                                if cost <= capital:
                                    capital -= cost
                                    positions.append({'code': code, 'qty': qty, 'cost': price, 'name': stock.get('name', '')})
                                    trades.append({'date': date, 'action': 'buy', 'price': price, 'code': code})
            
            # === 计算市值 ===
            total = capital
            for pos in positions:
                price = self.get_latest_price(pos['code'], date)
                if price:
                    total += price * pos['qty']
            
            if total > peak:
                peak = total
            dd = (peak - total) / peak if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd
            
            if (i + 1) % 60 == 0:
                logger.info(f"{date}: 现金={capital:,.0f}, 持仓={len(positions)}只, 总值={total:,.0f}")
        
        # 最终清仓
        for pos in positions:
            price = self.get_latest_price(pos['code'], end_date)
            if price:
                capital += price * pos['qty'] * 0.998
        
        ret = (capital - self.initial_capital) / self.initial_capital
        years = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(start_date, '%Y%m%d')).days / 365
        
        return {
            'strategy': strategy_name,
            'total_return': ret * 100,
            'annual_return': ((1 + ret) ** (1/years) - 1) * 100 if years > 0 else 0,
            'max_drawdown': max_dd * 100,
            'trade_count': len(trades)
        }

# 简化信号
def ma_signal(df):
    if len(df) < 60:
        return 'hold'
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma20 = df['close'].rolling(20).mean().iloc[-1]
    prev_ma5 = df['close'].rolling(5).mean().iloc[-2]
    prev_ma20 = df['close'].rolling(20).mean().iloc[-2]
    
    if prev_ma5 <= prev_ma20 and ma5 > ma20:
        return 'buy'
    elif prev_ma5 >= prev_ma20 and ma5 < ma20:
        return 'sell'
    return 'hold'

def break_signal(df):
    if len(df) < 60:
        return 'hold'
    high20 = df['high'].rolling(20).max().iloc[-1]
    close = df['close'].iloc[-1]
    prev_close = df['close'].iloc[-2]
    
    if prev_close < high20 and close > high20:
        return 'buy'
    return 'hold'

def main():
    engine = Backtester()
    
    results = {}
    for name, func in [('均线策略', ma_signal), ('突破策略', break_signal)]:
        result = engine.run(name, func, BACKTEST_START, BACKTEST_END)
        results[name] = result
        print(f"{name}: 收益={result['total_return']:.2f}%, 年化={result['annual_return']:.2f}%, 回撤={result['max_drawdown']:.2f}%, 交易={result['trade_count']}")

if __name__ == '__main__':
    main()
