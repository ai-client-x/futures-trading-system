#!/usr/bin/env python3
"""
下载A股日线数据到本地
Save A-Stock daily data locally
"""

import os
import sys
import json
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np

# 尝试导入 AkShare
try:
    import akshare as ak
    HAS_AKSHARE = True
except ImportError:
    HAS_AKSHARE = False


# 股票池 - 5只股票
STOCKS = [
    {"code": "600519", "name": "贵州茅台"},
    {"code": "000858", "name": "五粮液"},
    {"code": "601318", "name": "中国平安"},
    {"code": "300750", "name": "宁德时代"},
    {"code": "002594", "name": "比亚迪"},
]


def download_stock_data(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """下载单只股票数据"""
    if not HAS_AKSHARE:
        print(f"  ⚠️ AkShare不可用")
        return None
    
    try:
        # 判断交易所
        if code.startswith('6'):
            symbol = f"sh{code}"
        else:
            symbol = f"sz{code}"
        
        # 转换日期格式
        start_str = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d")
        end_str = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d")
        
        print(f"  下载 {code} ({start_str} ~ {end_str})...")
        
        # 使用AkShare获取日K线数据
        df = ak.stock_zh_a_hist(
            symbol=code,  # 直接用股票代码，如 "600519"
            period="daily", 
            start_date=start_str, 
            end_date=end_str, 
            adjust="qfq"
        )
        
        if df is None or df.empty:
            print(f"  ✗ {code} 无数据")
            return None
        
        # 标准化列名
        df = df.rename(columns={
            '日期': 'Date',
            '开盘': 'Open',
            '收盘': 'Close',
            '最高': 'High',
            '最低': 'Low',
            '成交量': 'Volume',
        })
        
        # 转换日期格式
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date')
        df = df.sort_index()
        
        # 只保留需要的列
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
        
        print(f"  ✓ {code} 获取 {len(df)} 条数据")
        return df
        
    except Exception as e:
        print(f"  ✗ {code} 下载失败: {e}")
        return None


def save_to_csv(df: pd.DataFrame, code: str, name: str, data_dir: str):
    """保存到CSV文件"""
    filepath = os.path.join(data_dir, f"{code}_{name}.csv")
    df.to_csv(filepath)
    print(f"  ✓ 保存到 {filepath}")


def main():
    # 配置
    data_dir = "data/daily"
    start_date = "2020-01-01"  # 过去5年
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    print("="*60)
    print("📥 下载A股日线数据到本地")
    print("="*60)
    print(f"数据目录: {data_dir}")
    print(f"股票池: {[s['code'] for s in STOCKS]}")
    print(f"时间范围: {start_date} ~ {end_date}")
    print("="*60)
    
    # 创建目录
    os.makedirs(data_dir, exist_ok=True)
    
    # 下载数据
    success_count = 0
    for stock in STOCKS:
        code = stock['code']
        name = stock['name']
        
        print(f"\n[{STOCKS.index(stock)+1}/{len(STOCKS)}] {code} {name}")
        
        df = download_stock_data(code, start_date, end_date)
        
        if df is not None and not df.empty:
            save_to_csv(df, code, name, data_dir)
            success_count += 1
        else:
            # 保存空文件标记
            print(f"  ✗ 跳过")
    
    print("\n" + "="*60)
    print(f"✅ 下载完成: {success_count}/{len(STOCKS)} 只股票")
    print(f"📁 数据保存在: {data_dir}/")
    print("="*60)


if __name__ == "__main__":
    main()
