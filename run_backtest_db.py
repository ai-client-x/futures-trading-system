#!/usr/bin/env python3
"""
A股回测系统 - 使用SQLite数据库
"""

import os
import sys
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import sqlite3
from src.engines.trading_engine import TradingEngine
from src.config import config


# ============ 配置 ============
DB_PATH = "data/stocks.db"

# 数据划分 (从config读取)
DEVELOP_START = config.develop_start
DEVELOP_END = config.develop_end
BACKTEST_START = config.backtest_start
BACKTEST_END = config.backtest_end

# 初始资金 (从config读取)
INITIAL_CAPITAL = config.initial_capital

# 策略参数
MAX_POSITIONS = config.max_positions
POSITION_SIZE = config.max_position
STOP_LOSS = config.stop_loss_pct * 100  # 转换为百分比
TAKE_PROFIT = config.take_profit_pct * 100
MIN_SIGNAL_SCORE = 20

# 测试股票数量（从config读取）
TEST_STOCK_COUNT = 10

# 测试股票代码 (从config读取)
TEST_STOCK_CODES = [s['code'] + ('.SH' if s['code'].startswith('6') else '.SZ') 
                   for s in config.stock_pool] if config.stock_pool else [
    '600519.SH', '000858.SZ', '601318.SH', '300750.SZ', '002594.SZ',
    '600036.SH', '600900.SH', '601888.SH', '600276.SH', '000001.SZ'
]


def get_stock_list(limit: int = 0):
    """获取股票列表"""
    conn = sqlite3.connect(DB_PATH)
    
    # 如果有预设的测试股票
    if 'TEST_STOCK_CODES' in globals() and TEST_STOCK_CODES:
        placeholders = ','.join(['?'] * len(TEST_STOCK_CODES))
        df = pd.read_sql(f"SELECT ts_code, name, market FROM stocks WHERE ts_code IN ({placeholders})", 
                       conn, params=TEST_STOCK_CODES)
    elif limit > 0:
        df = pd.read_sql(f"SELECT ts_code, name, market FROM stocks LIMIT {limit}", conn)
    else:
        df = pd.read_sql("SELECT ts_code, name, market FROM stocks", conn)
    
    conn.close()
    return df.to_dict('records')


def get_stock_data(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """获取股票历史数据"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"""
        SELECT trade_date, open, high, low, close, vol, amount
        FROM daily
        WHERE ts_code = '{ts_code}'
        AND trade_date >= '{start_date}'
        AND trade_date <= '{end_date}'
        ORDER BY trade_date
    """, conn)
    conn.close()
    return df


def calculate_signal(df: pd.DataFrame) -> dict:
    """
    计算交易信号
    返回: {'action': 'buy'/'sell'/'hold', 'score': 0-100, 'reason': '...'}
    """
    if df is None or len(df) < 60:
        return {'action': 'hold', 'score': 0, 'reason': '数据不足'}
    
    close = df['close']
    
    # 计算技术指标
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    
    # RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rsi = 100 - (100 / (1 + gain / loss))
    
    # 评分
    buy_score = 0
    sell_score = 0
    reasons = []
    
    # 均线多头排列
    if ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1]:
        buy_score += 20
        reasons.append('均线多头')
    elif ma5.iloc[-1] < ma10.iloc[-1] < ma20.iloc[-1]:
        sell_score += 20
        reasons.append('均线空头')
    
    # RSI超卖/超买
    if rsi.iloc[-1] < 30:
        buy_score += 15
        reasons.append(f'RSI超卖({rsi.iloc[-1]:.0f})')
    elif rsi.iloc[-1] > 70:
        sell_score += 15
        reasons.append(f'RSI超买({rsi.iloc[-1]:.0f})')
    
    # MACD
    if len(ma5) >= 26:
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9).mean()
        if macd.iloc[-1] > signal.iloc[-1]:
            buy_score += 10
        else:
            sell_score += 10
    
    # 判定
    if buy_score >= MIN_SIGNAL_SCORE and buy_score > sell_score:
        return {'action': 'buy', 'score': buy_score, 'reason': '; '.join(reasons)}
    elif sell_score >= MIN_SIGNAL_SCORE and sell_score > buy_score:
        return {'action': 'sell', 'score': sell_score, 'reason': '; '.join(reasons)}
    
    return {'action': 'hold', 'score': max(buy_score, sell_score), 'reason': '观望'}


def run_backtest(stock_list: list, start_date: str, end_date: str, 
                period_name: str = "回测") -> dict:
    """
    运行回测
    
    Args:
        stock_list: 股票列表
        start_date: 开始日期
        end_date: 结束日期
        period_name: 时期名称
    
    Returns:
        回测结果字典
    """
    
    print(f"\n{'='*60}")
    print(f"📊 {period_name} - {start_date} ~ {end_date}")
    print(f"{'='*60}")
    print(f"股票数量: {len(stock_list)}")
    print(f"初始资金: ¥{INITIAL_CAPITAL:,}")
    print(f"="*60)
    
    engine = TradingEngine(INITIAL_CAPITAL)
    
    # 获取所有交易日
    conn = sqlite3.connect(DB_PATH)
    trade_dates = pd.read_sql(f"""
        SELECT DISTINCT trade_date 
        FROM daily 
        WHERE trade_date >= '{start_date}' 
        AND trade_date <= '{end_date}'
        ORDER BY trade_date
    """, conn)['trade_date'].tolist()
    conn.close()
    
    print(f"交易日数: {len(trade_dates)}")
    
    trades = []
    daily_values = []
    
    for i, date in enumerate(trade_dates):
        # 1. 卖出检查（止盈止损）
        for code in list(engine.portfolio.positions.keys()):
            holding = engine.portfolio.positions[code]
            if holding.quantity > 0:
                # 获取当日收盘价
                conn = sqlite3.connect(DB_PATH)
                price_row = conn.execute(f'''
                    SELECT close FROM daily 
                    WHERE ts_code='{code}' AND trade_date='{date}'
                ''').fetchone()
                conn.close()
                
                if price_row:
                    current_price = float(price_row[0])
                    pnl_pct = (current_price - holding.avg_cost) / holding.avg_cost * 100
                    
                    # 止损/止盈
                    if pnl_pct <= STOP_LOSS or pnl_pct >= TAKE_PROFIT:
                        # 直接平仓
                        revenue = current_price * holding.quantity
                        commission = revenue * 0.001
                        engine.portfolio.cash += revenue - commission
                        trades.append({
                            'date': date,
                            'code': code,
                            'action': 'sell',
                            'price': current_price,
                            'pnl_pct': pnl_pct
                        })
                        holding.quantity = 0
                        del engine.portfolio.positions[code]
        
        # 2. 买入检查
        if len(engine.portfolio.positions) < MAX_POSITIONS:
            # 遍历股票找信号
            for stock in stock_list:
                code = stock['ts_code']
                name = stock['name']
                
                # 检查是否已持仓
                if code in engine.portfolio.positions:
                    continue
                
                # 获取历史数据
                conn = sqlite3.connect(DB_PATH)
                df = pd.read_sql(f"""
                    SELECT trade_date, open, high, low, close, vol, amount
                    FROM daily
                    WHERE ts_code = '{code}'
                    AND trade_date <= '{date}'
                    ORDER BY trade_date
                """, conn)
                conn.close()
                
                if len(df) < 60:
                    continue
                
                # 计算信号
                signal = calculate_signal(df)
                
                if signal['action'] == 'buy':
                    # 次日开盘买入
                    next_idx = trade_dates.index(date) + 1
                    if next_idx < len(trade_dates):
                        next_date = trade_dates[next_idx]
                        conn = sqlite3.connect(DB_PATH)
                        open_row = conn.execute(f'''
                            SELECT open FROM daily 
                            WHERE ts_code='{code}' AND trade_date='{next_date}'
                        ''').fetchone()
                        conn.close()
                        
                        if open_row:
                            open_price = open_row[0]
                            amount = engine.portfolio.cash * POSITION_SIZE
                            qty = int(amount / open_price / 100) * 100
                            
                            if qty > 0:
                                engine.buy(code, name, open_price, qty, next_date)
                                trades.append({
                                    'date': next_date,
                                    'code': code,
                                    'action': 'buy',
                                    'price': open_price,
                                    'qty': qty
                                })
        
        # 记录每日资产
        daily_values.append({
            'date': date,
            'assets': engine.portfolio.total_assets
        })
        
        # 进度
        if (i + 1) % 50 == 0:
            ret = (engine.portfolio.total_assets - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
            print(f"Day {i+1}: 资产¥{engine.portfolio.total_assets:,.0f} ({ret:+.2f}%)")
    
    # 最终结算
    conn = sqlite3.connect(DB_PATH)
    last_date = trade_dates[-1]
    for code, holding in list(engine.portfolio.positions.items()):
        if holding.quantity > 0:
            price_row = conn.execute(f'''
                SELECT close FROM daily 
                WHERE ts_code='{code}' AND trade_date='{last_date}'
            ''').fetchone()
            if price_row:
                price = float(price_row[0])
                revenue = price * holding.quantity
                commission = revenue * 0.001
                engine.portfolio.cash += revenue - commission
    conn.close()
    
    # 计算结果
    final_assets = engine.portfolio.total_assets
    total_return = (final_assets - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
    years = len(trade_dates) / 252
    annual_return = total_return / years if years > 0 else 0
    
    # 最大回撤
    peak = INITIAL_CAPITAL
    max_drawdown = 0
    for v in daily_values:
        if v['assets'] > peak:
            peak = v['assets']
        dd = (peak - v['assets']) / peak * 100
        if dd > max_drawdown:
            max_drawdown = dd
    
    # 胜率
    sell_trades = [t for t in trades if t['action'] == 'sell']
    wins = sum(1 for t in sell_trades if t.get('pnl_pct', 0) > 0)
    win_rate = wins / len(sell_trades) * 100 if sell_trades else 0
    
    # 结果
    result = {
        'period': period_name,
        'start_date': start_date,
        'end_date': end_date,
        'initial_capital': INITIAL_CAPITAL,
        'final_assets': final_assets,
        'total_return': total_return,
        'annual_return': annual_return,
        'max_drawdown': max_drawdown,
        'trade_count': len(trades),
        'win_rate': win_rate,
        'stocks': len(stock_list),
        'trade_days': len(trade_dates)
    }
    
    # 打印结果
    print(f"\n{'='*60}")
    print(f"📊 {period_name} 结果")
    print(f"{'='*60}")
    print(f"初始资金:   ¥{INITIAL_CAPITAL:,.0f}")
    print(f"最终资产:   ¥{final_assets:,.0f}")
    print(f"总收益率:   {total_return:+.2f}%")
    print(f"年化收益:   {annual_return:+.2f}%")
    print(f"最大回撤:   {max_drawdown:.2f}%")
    print(f"交易次数:   {len(trades)}")
    print(f"胜率:       {win_rate:.1f}%")
    print(f"{'='*60}")
    
    return result


def main():
    """主函数"""
    
    print("="*60)
    print("📈 A股量化回测系统")
    print("="*60)
    
    # 获取股票列表
    print("\n📋 加载股票列表...")
    stock_list = get_stock_list(TEST_STOCK_COUNT)
    print(f"  股票数量: {TEST_STOCK_COUNT if TEST_STOCK_COUNT > 0 else '全部'}")
    
    # 开发期回测（2020-2022）
    develop_result = run_backtest(
        stock_list, 
        DEVELOP_START, 
        DEVELOP_END, 
        "开发期"
    )
    
    # 回测期（2023-2024）
    backtest_result = run_backtest(
        stock_list,
        BACKTEST_START,
        BACKTEST_END,
        "回测期"
    )
    
    # 保存结果
    os.makedirs('backtest_results', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    results = {
        'develop': develop_result,
        'backtest': backtest_result,
        'config': {
            'max_positions': MAX_POSITIONS,
            'position_size': POSITION_SIZE,
            'stop_loss': STOP_LOSS,
            'take_profit': TAKE_PROFIT,
            'min_signal_score': MIN_SIGNAL_SCORE
        }
    }
    
    filepath = f'backtest_results/full_backtest_{timestamp}.json'
    with open(filepath, 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ 结果已保存: {filepath}")
    
    return results


if __name__ == "__main__":
    main()
