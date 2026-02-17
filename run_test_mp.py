"""简化版多持仓回测"""
import sqlite3
import pandas as pd

DB_PATH = 'data/stocks.db'

def get_pool(date):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT ts_code, name FROM fundamentals
        WHERE pe > 0 AND pe <= 25 AND roe >= 10 AND dv_ratio >= 1
        AND debt_to_assets <= 70 AND market_cap >= 30
        ORDER BY roe DESC LIMIT 30
    """, conn)
    conn.close()
    return df.to_dict('records')

def get_price(code, date):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"""
        SELECT close FROM daily WHERE ts_code = '{code}' AND trade_date <= '{date}'
        ORDER BY trade_date DESC LIMIT 1
    """, conn)
    conn.close()
    return df['close'].iloc[0] if len(df) > 0 else None

def get_series(code, date):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"""
        SELECT close FROM daily WHERE ts_code = '{code}' AND trade_date <= '{date}'
        ORDER BY trade_date DESC LIMIT 60
    """, conn)
    conn.close()
    return df['close'].tolist() if len(df) >= 60 else None

def ma_signal(prices):
    if len(prices) < 60: return 'hold'
    ma5, ma20 = prices[-5], prices[-20]
    ma5_p, ma20_p = prices[-6], prices[-21]
    if ma5_p <= ma20_p and ma5 > ma20: return 'buy'
    if ma5_p >= ma20_p and ma5 < ma20: return 'sell'
    return 'hold'

def break_signal(prices):
    if len(prices) < 60: return 'hold'
    high20 = max(prices[-20:])
    if prices[-1] > high20 and prices[-2] <= high20: return 'buy'
    return 'hold'

def run(strategy_name, signal_func):
    capital = 1000000
    positions = []
    trades = []
    
    conn = sqlite3.connect(DB_PATH)
    dates = pd.read_sql("""
        SELECT DISTINCT trade_date FROM daily 
        WHERE trade_date >= '20200101' AND trade_date <= '20241231'
        ORDER BY trade_date
    """, conn)['trade_date'].tolist()
    conn.close()
    
    last_month = None
    for i, date in enumerate(dates):
        month = date[:6]
        if month != last_month:
            pool = get_pool(date)
            last_month = month
        
        if not pool: continue
        
        # 卖出
        to_sell = []
        for pos in positions:
            prices = get_series(pos['code'], date)
            if prices:
                sig = signal_func(prices)
                price = prices[-1]
                if price <= pos['cost'] * 0.97 or price >= pos['cost'] * 1.06 or sig == 'sell':
                    to_sell.append((pos, price))
        
        for pos, price in to_sell:
            capital += price * pos['qty'] * 0.998
            trades.append({'date': date, 'code': pos['code'], 'action': 'sell'})
            positions = [p for p in positions if p['code'] != pos['code']]
        
        # 买入
        if len(positions) < 3 and capital > 0:
            alloc = capital / (3 - len(positions))
            for stock in pool[:10]:
                if len(positions) >= 3: break
                code = stock['ts_code']
                if any(p['code'] == code for p in positions): continue
                prices = get_series(code, date)
                if prices and signal_func(prices) == 'buy':
                    price = prices[-1]
                    qty = int(alloc / price / 100) * 100
                    if qty > 0:
                        capital -= price * qty * 1.001
                        positions.append({'code': code, 'qty': qty, 'cost': price})
        
        if (i + 1) % 200 == 0:
            total = capital + sum(get_price(p['code'], date) * p['qty'] for p in positions if get_price(p['code'], date))
            print(f"{date}: 现金={capital:,.0f}, 持仓={len(positions)}只, 总值={total:,.0f}")
    
    # 清仓
    for pos in positions:
        price = get_price(pos['code'], date)
        if price: capital += price * pos['qty'] * 0.998
    
    ret = (capital - 1000000) / 1000000 * 100
    print(f"{strategy_name}: 收益={ret:.2f}%, 交易={len(trades)}")
    return ret

if __name__ == '__main__':
    run('均线策略', ma_signal)
    run('突破策略', break_signal)
