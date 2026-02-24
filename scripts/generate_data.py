#!/usr/bin/env python3
"""
生成更真实的A股模拟数据
Generate realistic A-Stock mock data
"""

import os
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


# 股票池
STOCKS = [
    {"code": "600519", "name": "贵州茅台", "base_price": 1500, "volatility": 0.025},
    {"code": "000858", "name": "五粮液", "base_price": 150, "volatility": 0.028},
    {"code": "601318", "name": "中国平安", "base_price": 45, "volatility": 0.03},
    {"code": "300750", "name": "宁德时代", "base_price": 200, "volatility": 0.035},
    {"code": "002594", "name": "比亚迪", "base_price": 250, "volatility": 0.032},
]


def generate_realistic_data(stock: dict, start_date: str, end_date: str) -> pd.DataFrame:
    """生成更真实的A股数据"""
    
    # 日期范围
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # 生成交易日（跳过周末）
    dates = pd.date_range(start=start, end=end, freq='B')
    
    base_price = stock['base_price']
    volatility = stock['volatility']
    
    # 设置随机种子
    np.random.seed(hash(stock['code']) % 10000)
    
    # 生成价格走势 - 带有趋势和周期
    n = len(dates)
    
    # 趋势成分（长期上涨）
    trend = np.linspace(0, 0.3, n)  # 5年上涨30%
    
    # 周期成分
    cycle = 0.1 * np.sin(np.linspace(0, 8 * np.pi, n))
    
    # 随机成分
    random_walk = np.cumsum(np.random.normal(0, volatility, n))
    
    # 组合
    returns = trend + cycle + random_walk
    prices = base_price * np.exp(returns)
    
    # 确保价格不会太低
    prices = np.maximum(prices, base_price * 0.3)
    
    # 生成OHLC数据
    df = pd.DataFrame({
        'Open': prices * (1 + np.random.uniform(-0.01, 0.01, n)),
        'High': prices * (1 + np.random.uniform(0.005, 0.03, n)),
        'Low': prices * (1 + np.random.uniform(-0.03, -0.005, n)),
        'Close': prices,
        'Volume': np.random.randint(5000000, 50000000, n)
    }, index=dates)
    
    return df


def main():
    data_dir = "data/daily"
    start_date = "2020-01-01"
    end_date = "2026-02-16"
    
    print("="*60)
    print("📥 生成A股历史数据（模拟）")
    print("="*60)
    print(f"数据目录: {data_dir}")
    print(f"时间范围: {start_date} ~ {end_date}")
    print("="*60)
    
    # 创建目录
    os.makedirs(data_dir, exist_ok=True)
    
    # 生成数据
    for stock in STOCKS:
        code = stock['code']
        name = stock['name']
        
        print(f"\n生成 {code} {name}...")
        
        df = generate_realistic_data(stock, start_date, end_date)
        
        # 保存
        filepath = os.path.join(data_dir, f"{code}_{name}.csv")
        df.to_csv(filepath)
        
        print(f"  ✓ {len(df)} 条数据 -> {filepath}")
    
    print("\n" + "="*60)
    print("✅ 数据生成完成！")
    print("="*60)


if __name__ == "__main__":
    main()
