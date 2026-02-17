#!/usr/bin/env python3
"""下载港股历史数据"""

import os
import sys
import time
import random

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import akshare as ak


def get_hk_stock_list():
    """获取港股列表"""
    print("📋 获取港股列表...")
    df = ak.stock_hk_spot()
    # 过滤有效股票
    df = df[df['成交额'] > 0]
    print(f"  ✓ 有效港股: {len(df)} 只")
    return df[['代码', '中文名称']].values.tolist()


def download_hk_stocks(data_dir="data/hk_daily", max_stocks=None):
    """下载港股历史数据"""
    
    stocks = get_hk_stock_list()
    
    if max_stocks:
        stocks = stocks[:max_stocks]
    
    print(f"\n📥 开始下载 {len(stocks)} 只港股...")
    print(f"数据目录: {data_dir}")
    print("="*60)
    
    os.makedirs(data_dir, exist_ok=True)
    
    success = 0
    failed = 0
    skip = 0
    
    for i, (code, name) in enumerate(stocks):
        if i % 20 == 0:
            print(f"\n[{i+1}/{len(stocks)}] 进度: {i}/{len(stocks)}")
        
        filepath = os.path.join(data_dir, f"{code}_{name}.csv")
        if os.path.exists(filepath):
            skip += 1
            continue
        
        try:
            time.sleep(random.uniform(1.5, 2.5))
            
            df = ak.stock_hk_hist(
                symbol=code,
                period="daily", 
                start_date="20150101", 
                end_date="20260216"
            )
            
            if df is not None and len(df) > 0:
                df.to_csv(filepath, index=False)
                success += 1
            else:
                failed += 1
            
        except Exception as e:
            failed += 1
            time.sleep(3)
    
    print("\n" + "="*60)
    print(f"✅ 下载完成!")
    print(f"  成功: {success}")
    print(f"  失败: {failed}")
    print(f"  跳过: {skip}")
    print(f"📁 数据保存在: {data_dir}/")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--max', type=int, default=100, help='最大股票数量')
    parser.add_argument('--dir', type=str, default='data/hk_daily', help='数据目录')
    args = parser.parse_args()
    
    download_hk_stocks(data_dir=args.dir, max_stocks=args.max)
