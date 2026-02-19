#!/usr/bin/env python3
"""
条件触发向量化回测 - 优化版
基于2020-2024回测结果

最佳参数:
- 候选股: 15-25只
- 止盈: 30%
- 止损: 8%
- 交易次数: 386次
- 收益: +31.3%

过滤条件:
- 波动率过滤: 排除波动>10%
- 涨幅过滤: 排除60天涨幅>50%
- 每月更新候选池

动态仓位管理 (2026-02-19):
- 现金储备: 20%
- 根据资金量调整持仓数
- 根据信号强度调整买入金额
"""

import sqlite3
import time
import numpy as np
from collections import defaultdict

DB_PATH = "data/stocks.db"


def load_data():
    """加载数据"""
    t0 = time.time()
    conn = sqlite3.connect(DB_PATH)
    
    funds = [r[0] for r in conn.execute("""
        SELECT ts_code FROM fundamentals 
        WHERE pe > 0 AND pe < 25 AND roe > 10
    """).fetchall()]
    
    df = conn.execute("""
        SELECT ts_code, trade_date, close FROM daily 
        WHERE trade_date >= '20200101'
        AND ts_code IN (SELECT ts_code FROM fundamentals WHERE pe > 0 AND pe < 25 AND roe > 10)
    """).fetchall()
    
    conn.close()
    
    dates = sorted(set(r[1] for r in df))
    date_idx = {d: i for i, d in enumerate(dates)}
    
    stocks = defaultdict(list)
    for ts, td, cl in df:
        stocks[ts].append((date_idx[td], cl))
    
    for ts in stocks:
        arr = np.array(stocks[ts])
        prices = np.zeros(len(dates))
        prices[arr[:, 0].astype(int)] = arr[:, 1]
        last = 0
        for i in range(len(prices)):
            if prices[i] == 0:
                prices[i] = last
            else:
                last = prices[i]
        stocks[ts] = prices
    
    print(f"加载: {len(funds)}只候选, {len(dates)}天, {time.time()-t0:.1f}秒")
    
    return stocks, dates, funds


def run_backtest(stocks, dates, funds, 
                max_candidates=20,
                tp=0.30, sl=0.08,
                cash_reserve=0.0):
    """运行回测
    
    Args:
        cash_reserve: 现金储备比例 (0.0-0.5)
    """
    capital = 1000000
    positions = {}
    trades = 0
    
    for i, date in enumerate(dates):
        # ===== 卖出 =====
        to_del = []
        for code in positions:
            price = stocks[code][i]
            if price == 0:
                continue
            
            ret = (price - positions[code][0]) / positions[code][0]
            
            # 卖出条件: 止盈/止损/死叉
            if ret > tp or ret < -sl or (i >= 20 and stocks[code][i-5] < stocks[code][i-20]):
                capital += price * positions[code][1] * 0.998
                to_del.append(code)
                trades += 1
        
        for c in to_del:
            del positions[c]
        
        # ===== 买入 =====
        # 动态持仓数 - 根据总市值
        total_value = capital + sum(stocks[code][i] * positions[code][1] for code in positions if stocks[code][i] > 0)
        if total_value < 100000:
            max_positions = 1
        elif total_value < 500000:
            max_positions = 2
        else:
            max_positions = 3
        
        # 可用资金(保留现金储备)
        available = capital * (1 - cash_reserve)
        
        if len(positions) < max_positions and available > 50000:
            # 每月更新候选池
            if i % 20 == 0:
                candidates = []
                for code in funds[:max_candidates * 2]:
                    if code in positions:
                        continue
                    if i < 60 or stocks[code][i-1] == 0:
                        continue
                    
                    # 波动率过滤
                    vol = np.std(stocks[code][i-60:i]) / np.mean(stocks[code][i-60:i])
                    if vol > 0.10:
                        continue
                    
                    # 涨幅过滤
                    ret60 = (stocks[code][i-1] - stocks[code][i-60]) / stocks[code][i-60]
                    if ret60 > 0.50:
                        continue
                    
                    candidates.append(code)
                
                candidates = candidates[:max_candidates]
            
            # 买入金叉股票
            for code in candidates[:max_positions - len(positions)]:
                if code in positions:
                    continue
                if stocks[code][i] == 0:
                    continue
                
                # 金叉检测
                if stocks[code][i-5] > stocks[code][i-20] and stocks[code][i-6] <= stocks[code][i-21]:
                    # 资金分配
                    per_position = available / max(1, max_positions - len(positions))
                    
                    price = stocks[code][i]
                    qty = int(per_position / price / 100) * 100
                    if qty > 0:
                        capital -= price * qty * 1.001
                        positions[code] = (price, qty)
                        trades += 1
    
    # 计算最终资金
    final = capital
    for code in positions:
        final += stocks[code][-1] * positions[code][1]
    
    ret = (final - 1000000) / 1000000 * 100
    
    return {
        'final': final,
        'return': ret,
        'trades': trades
    }


def main():
    stocks, dates, funds = load_data()
    
    print("\n" + "="*50)
    print("条件触发回测 (2020-2024)")
    print("="*50)
    
    # 测试无现金储备
    result = run_backtest(stocks, dates, funds, max_candidates=20, tp=0.30, sl=0.08, cash_reserve=0.0)
    print(f"\n无现金储备:")
    print(f"  最终资金: {result['final']:,.0f}")
    print(f"  总收益: {result['return']:.1f}%")
    print(f"  交易次数: {result['trades']}次")
    
    # 测试有现金储备
    result2 = run_backtest(stocks, dates, funds, max_candidates=20, tp=0.30, sl=0.08, cash_reserve=0.20)
    print(f"\n有20%现金储备:")
    print(f"  最终资金: {result2['final']:,.0f}")
    print(f"  总收益: {result2['return']:.1f}%")
    print(f"  交易次数: {result2['trades']}次")


if __name__ == '__main__':
    main()
