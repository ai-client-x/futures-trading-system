#!/usr/bin/env python3
"""
股票量化交易系统 - 回测脚本
支持从本地CSV文件读取数据
"""

import os
import sys
import json
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np

from src.config import config
from src.engines.trading_engine import TradingEngine
from src.engines.backtest import Backtester
from src.risk.manager import RiskManager
from src.signals.generator import CompositeSignalGenerator


class DataFetcher:
    """数据获取器 - 优先从本地文件读取"""
    
    def __init__(self, data_dir: str = "data/daily"):
        self.data_dir = data_dir
    
    def fetch(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从本地CSV文件获取数据"""
        
        # 查找本地文件
        for filename in os.listdir(self.data_dir):
            if filename.startswith(code + "_"):
                filepath = os.path.join(self.data_dir, filename)
                print(f"  ✓ 从本地读取 {code}: {filename}")
                
                df = pd.read_csv(filepath, index_col=0, parse_dates=True)
                df = df.sort_index()
                
                # 按日期过滤
                start = datetime.strptime(start_date, "%Y-%m-%d")
                end = datetime.strptime(end_date, "%Y-%m-%d")
                df = df[(df.index >= start) & (df.index <= end)]
                
                return df
        
        print(f"  ✗ 本地没有 {code} 的数据")
        return None


def run_backtest():
    """运行回测"""
    
    # 股票池
    stocks = [
        {"code": "600519", "name": "贵州茅台"},
        {"code": "000858", "name": "五粮液"},
        {"code": "601318", "name": "中国平安"},
        {"code": "300750", "name": "宁德时代"},
        {"code": "002594", "name": "比亚迪"},
    ]
    
    # 回测时间 - 过去1年
    end_date = "2026-02-16"
    start_date = "2025-02-16"
    
    print("="*60)
    print("📊 股票量化交易系统 - 回测")
    print("="*60)
    print(f"数据源: 本地CSV文件")
    print(f"股票池: {[s['code'] for s in stocks]}")
    print(f"回测时间: {start_date} ~ {end_date}")
    print(f"初始资金: ¥1,000,000")
    print("="*60)
    
    # 初始化 - 从本地读取数据
    data_fetcher = DataFetcher(data_dir="data/daily")
    backtester = Backtester(initial_capital=1000000, data_fetcher=data_fetcher)
    
    # 运行回测
    result = backtester.run(
        stocks=stocks,
        start_date=start_date,
        end_date=end_date,
        verbose=True
    )
    
    # 保存结果
    output_dir = "backtest_results"
    os.makedirs(output_dir, exist_ok=True)
    filepath = f"{output_dir}/backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # 构建输出
    output = {
        "generated_at": datetime.now().isoformat(),
        "config": {
            "data_source": "local_csv",
            "initial_capital": result.initial_capital,
            "stocks": stocks,
            "start_date": start_date,
            "end_date": end_date
        },
        "result": result.to_dict()
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ 回测结果已保存: {filepath}")
    
    return result


if __name__ == "__main__":
    run_backtest()
