#!/usr/bin/env python3
"""
下载全部A股日线数据到本地（保守版）
"""

import os
import sys
import time
import random

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import akshare as ak


def get_stock_list():
    """获取A股全部股票列表"""
    print("📋 获取A股股票列表...")
    df = ak.stock_info_a_code_name()
    print(f"  ✓ 获取到 {len(df)} 只股票")
    return df[['code', 'name']].values.tolist()


def download_all_stocks(data_dir="data/daily_all", max_stocks=None):
    """下载全部A股数据"""
    
    stocks = get_stock_list()
    
    if max_stocks:
        stocks = stocks[:max_stocks]
    
    print(f"\n📥 开始下载 {len(stocks)} 只股票...")
    print(f"数据目录: {data_dir}")
    print("="*60)
    
    os.makedirs(data_dir, exist_ok=True)
    
    success = 0
    failed = 0
    skip = 0
    
    for i, (code, name) in enumerate(stocks):
        # 每100只打印进度
        if i % 10 == 0:
            print(f"\n[{i+1}/{len(stocks)}] 进度: {i}/{len(stocks)}")
        
        filepath = os.path.join(data_dir, f"{code}_{name}.csv")
        if os.path.exists(filepath):
            skip += 1
            continue
        
        try:
            # 添加随机延迟
            time.sleep(random.uniform(1, 2))
            
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily", 
                start_date="20200101", 
                end_date="20260216", 
                adjust="qfq"
            )
            
            if df is not None and len(df) > 0:
                df.to_csv(filepath, index=False)
                success += 1
            else:
                failed += 1
            
        except Exception as e:
            failed += 1
            # 失败多休息
            time.sleep(3)
    
    print("\n" + "="*60)
    print(f"✅ 下载完成!")
    print(f"  成功: {success}")
    print(f"  失败: {failed}")
    print(f"  跳过: {skip}")
    print(f"📁 数据保存在: {data_dir}/")


if __name__ == "__main__":
    download_all_stocks()
