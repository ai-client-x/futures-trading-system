#!/usr/bin/env python3
"""
条件触发向量化回测
- 事件驱动，不是时间驱动
- 只在满足条件时交易
"""

import sqlite3
import time
from collections import defaultdict

DB_PATH = "data/stocks.db"


def load_data():
    """加载数据"""
    t0 = time.time()
    conn = sqlite3.connect(DB_PATH)
    
    df = conn.execute("""
        SELECT ts_code, trade_date, close FROM daily 
        WHERE trade_date >= '20200101'
        ORDER BY ts_code, trade_date
    """).fetchall()
    
    funds = set(r[0] for r in conn.execute("""
        SELECT ts_code FROM fundamentals 
        WHERE pe > 0 AND pe < 25 AND roe > 10
    """).fetchall())
    conn.close()
    
    # 按股票分组
    stocks = defaultdict(list)
    for ts, td, cl in df:
        stocks[ts].append((td, cl))
    
    dates = sorted(set(r[1] for r in df))
    print(f"加载: {len(dates)}天, {time.time()-t0:.1f}秒")
    
    return stocks, dates, funds


def condition_backtest(stocks, dates, funds, 
                       tp=0.20, sl=0.08,
                       use_ma_signal=True,
                       use_market_regime=False):
    """
    条件触发回测
    
    买入条件:
    - MA5 > MA20 (金叉) 且 在牛市
    
    卖出条件:
    - 止盈 (tp)
    - 止损 (sl)  
    - 死叉 (MA5 < MA20)
    """
    capital = 1000000
    positions = {}  # {code: (cost, qty)}
    
    # 简化的市场状态检测 - 用沪深300近似
    market_bull = {}  # {date: bool}
    
    trade_log = []
    
    for i, date in enumerate(dates):
        # 简化: 只用全部股票的多头比例判断市场
        if i % 20 == 0:  # 每20天更新一次市场状态
            bull_count = 0
            total = 0
            for code in list(funds)[:50]:
                data = stocks.get(code, [])
                if len(data) < 25:
                    continue
                # 找到date之前的最后价格
                prices = [c for d, c in data if d <= date]
                if len(prices) >= 20:
                    ma5 = sum(prices[-5:]) / 5
                    ma20 = sum(prices[-20:]) / 20
                    if ma5 > ma20:
                        bull_count += 1
                    total += 1
            market_bull[date] = bull_count / total > 0.5 if total > 0 else False
        
        is_bull = market_bull.get(date, False)
        
        # ===== 卖出条件 =====
        to_sell = []
        for code in list(positions.keys()):
            data = stocks.get(code, [])
            prices = [c for d, c in data if d <= date]
            if not prices:
                continue
            
            current_price = prices[-1]
            cost = positions[code][0]
            ret = (current_price - cost) / cost
            
            # 卖出条件: 止盈/止损/死叉
            should_sell = False
            reason = ""
            
            if ret > tp:
                should_sell = True
                reason = "止盈"
            elif ret < -sl:
                should_sell = True
                reason = "止损"
            elif use_ma_signal and len(prices) >= 20:
                ma5 = sum(prices[-5:]) / 5
                ma20 = sum(prices[-20:]) / 20
                if ma5 < ma20:  # 死叉
                    should_sell = True
                    reason = "死叉"
            
            if should_sell:
                capital += current_price * positions[code][1] * 0.998
                to_sell.append(code)
                trade_log.append((date, "卖出", code, current_price, ret*100, reason))
        
        for code in to_sell:
            del positions[code]
        
        # ===== 买入条件 =====
        # 触发条件: 
        # 1. 有空仓
        # 2. 在牛市 (可选)
        # 3. 有金叉信号
        
        if len(positions) < 3 and capital > 50000:
            # 检查候选股
            candidates = []
            for code in funds:
                if code in positions:
                    continue
                data = stocks.get(code, [])
                if len(data) < 25:
                    continue
                
                prices = [c for d, c in data if d <= date]
                if len(prices) < 20:
                    continue
                
                ma5 = sum(prices[-5:]) / 5
                ma20 = sum(prices[-20:]) / 20
                
                # 金叉: MA5刚突破MA20
                prev_ma5 = sum(prices[-6:-1]) / 5
                prev_ma20 = sum(prices[-21:-1]) / 20
                
                is_golden_cross = (prev_ma5 <= prev_ma20) and (ma5 > ma20)
                
                if is_golden_cross:
                    if use_market_regime and not is_bull:
                        continue  # 熊市不买
                    candidates.append((code, prices[-1], ret*100 if code in positions else 0))
            
            # 买入金叉股票
            for code, price, _ in candidates[:3-len(positions)]:
                if capital > 10000:
                    qty = int(capital / 3 / price / 100) * 100
                    if qty > 0:
                        capital -= price * qty * 1.001
                        positions[code] = (price, qty)
                        trade_log.append((date, "买入", code, price, 0, "金叉"))
    
    # 计算收益
    final_value = capital
    for code, (cost, qty) in positions.items():
        data = stocks.get(code, [])
        if data:
            price = [c for d, c in data if d <= dates[-1]][-1]
            final_value += price * qty
    
    total_return = (final_value - 1000000) / 1000000 * 100
    
    return {
        'final_value': final_value,
        'total_return': total_return,
        'trade_count': len(trade_log),
        'trades': trade_log[-20:]  # 最近20笔
    }


def main():
    stocks, dates, funds = load_data()
    
    print("\n" + "="*50)
    print("条件触发回测 (2020-2024)")
    print("="*50)
    
    # 测试不同参数
    results = []
    
    for tp in [0.15, 0.20, 0.30]:
        for sl in [0.05, 0.08, 0.10]:
            for use_ma in [True, False]:
                for use_regime in [True, False]:
                    t0 = time.time()
                    r = condition_backtest(
                        stocks, dates, funds,
                        tp=tp, sl=sl,
                        use_ma_signal=use_ma,
                        use_market_regime=use_regime
                    )
                    elapsed = time.time() - t0
                    results.append((tp, sl, use_ma, use_regime, r['total_return'], r['trade_count'], elapsed))
    
    # 按收益排序
    results.sort(key=lambda x: x[4], reverse=True)
    
    print("\n最佳参数:")
    for tp, sl, ma, regime, ret, trades, elapsed in results[:5]:
        print(f"TP={tp*100:.0f}% SL={sl*100:.0f%} MA={ma} 牛市={regime} → 收益={ret:.1f}% 交易={trades}次")
    
    print("\n最差参数:")
    for tp, sl, ma, regime, ret, trades, elapsed in results[-3:]:
        print(f"TP={tp*100:.0f}% SL={sl*100:.0f%} MA={ma} 牛市={regime} → 收益={ret:.1f}% 交易={trades}次")


if __name__ == '__main__':
    main()
