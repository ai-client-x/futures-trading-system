#!/usr/bin/env python3
"""向量化回测引擎 - 主系统向量化版"""

import sqlite3
import time
import numpy as np
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = "data/stocks.db"


class VectorizedBacktest:
    """向量化回测引擎"""
    
    def __init__(self):
        self.initial_capital = 1000000
        
    def load_data(self, start_date='20200101', end_date='20250101'):
        """加载数据"""
        t0 = time.time()
        conn = sqlite3.connect(DB_PATH)
        
        funds = [r[0] for r in conn.execute("""
            SELECT ts_code FROM fundamentals 
            WHERE pe > 0 AND pe < 25 AND roe > 10
        """).fetchall()]
        
        df = conn.execute(f"""
            SELECT ts_code, trade_date, close FROM daily 
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            AND ts_code IN (SELECT ts_code FROM fundamentals WHERE pe > 0 AND pe < 25 AND roe > 10)
        """).fetchall()
        
        conn.close()
        
        dates = sorted(set(r[1] for r in df))
        date_idx = {d: i for i, d in enumerate(dates)}
        
        # 只保留数据足够的股票
        stocks = {}
        valid_funds = []
        for code in funds:
            stock_data = [(date_idx[td], cl) for ts, td, cl in df if ts == code]
            if len(stock_data) >= 100:
                arr = np.array(stock_data)
                prices = np.zeros(len(dates))
                prices[arr[:, 0].astype(int)] = arr[:, 1]
                # forward fill
                last = 0
                for i in range(len(prices)):
                    if prices[i] == 0:
                        prices[i] = last
                    else:
                        last = prices[i]
                stocks[code] = prices
                valid_funds.append(code)
        
        logger.info(f"加载: {len(valid_funds)}只, {len(dates)}天, {time.time()-t0:.1f}秒")
        return stocks, dates, valid_funds
    
    def run(self, tp=0.30, sl=0.08, max_candidates=20):
        """运行回测"""
        stocks, dates, funds = self.load_data()
        
        capital = self.initial_capital
        positions = {}
        trades = 0
        
        for i in range(len(dates)):
            # 卖出
            to_del = []
            for code in positions:
                if i >= len(stocks[code]):
                    continue
                price = stocks[code][i]
                if price == 0:
                    continue
                ret = (price - positions[code][0]) / positions[code][0]
                
                if ret > tp or ret < -sl or (i >= 20 and stocks[code][i-5] < stocks[code][i-20]):
                    capital += price * positions[code][1] * 0.998
                    to_del.append(code)
                    trades += 1
            
            for code in to_del:
                del positions[code]
            
            # 买入
            if len(positions) < 3 and capital > 50000:
                if i % 20 == 0:
                    candidates = []
                    for code in funds[:max_candidates * 2]:
                        if code in positions:
                            continue
                        if i < 60 or i >= len(stocks[code]):
                            continue
                        if stocks[code][i-1] == 0:
                            continue
                        # 波动率过滤
                        vol = np.std(stocks[code][i-60:i]) / np.mean(stocks[code][i-60:i])
                        if vol > 0.10:
                            continue
                        # 涨幅过滤
                        ret60 = (stocks[code][i-1] - stocks[code][i-60]) / stocks[code][i-60]
                        if ret60 > 0.50:
                            continue
                        # 金叉
                        if stocks[code][i-5] > stocks[code][i-20] and stocks[code][i-6] <= stocks[code][i-21]:
                            candidates.append(code)
                    candidates = candidates[:max_candidates]
                
                for code in candidates[:3 - len(positions)]:
                    if code in positions or i >= len(stocks[code]):
                        continue
                    if stocks[code][i] == 0:
                        continue
                    price = stocks[code][i]
                    qty = int(capital / 3 / price / 100) * 100
                    if qty > 0:
                        capital -= price * qty * 1.001
                        positions[code] = (price, qty)
                        trades += 1
        
        # 最终资金
        final = capital
        for code in positions:
            if code in stocks:
                final += stocks[code][-1] * positions[code][1]
        
        total_return = (final - self.initial_capital) / self.initial_capital * 100
        
        return {
            'final_value': final,
            'total_return': total_return,
            'trade_count': trades
        }


def main():
    """主函数"""
    print("\n" + "="*50)
    print("向量化回测引擎")
    print("="*50)
    
    engine = VectorizedBacktest()
    result = engine.run(tp=0.30, sl=0.08, max_candidates=20)
    
    print(f"\n结果:")
    print(f"  最终资金: {result['final_value']:,.0f}")
    print(f"  总收益: {result['total_return']:.1f}%")
    print(f"  交易次数: {result['trade_count']}")
    print("="*50)


if __name__ == '__main__':
    main()
