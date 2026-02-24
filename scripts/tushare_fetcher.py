#!/usr/bin/env python3
"""
Tushare Pro 数据接口封装
用于获取A股、港股数据
"""

import os
import sys
from datetime import datetime

import tushare as ts

class TushareDataFetcher:
    """Tushare数据获取器"""
    
    def __init__(self, token: str = None):
        """
        初始化Tushare
        
        Args:
            token: Tushare Pro API Token
        """
        if token:
            ts.set_token(token)
        self.pro = ts.pro_api()
    
    def get_stock_daily(self, ts_code: str, start_date: str, end_date: str = None) -> 'pd.DataFrame':
        """
        获取日K线数据
        
        Args:
            ts_code: 股票代码，如 '000001.SZ' 或 '600519.SH'
            start_date: 开始日期，格式 'YYYYMMDD'
            end_date: 结束日期，格式 'YYYYMMDD'，默认今天
        
        Returns:
            DataFrame
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        try:
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            return df
        except Exception as e:
            print(f"获取数据失败 {ts_code}: {e}")
            return None
    
    def get_stock_list(self, market: str = 'A') -> 'pd.DataFrame':
        """
        获取股票列表
        
        Args:
            market: 市场类型，'A'=A股，'HK'=港股
        
        Returns:
            DataFrame
        """
        try:
            if market == 'A':
                df = self.pro.stock_basic(exchange='', list_status='L')
            elif market == 'HK':
                df = self.pro.stock_basic(exchange='SEHK', list_status='L')
            return df
        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return None
    
    def get_index_daily(self, ts_code: str, start_date: str, end_date: str = None) -> 'pd.DataFrame':
        """
        获取指数日K线
        
        Args:
            ts_code: 指数代码，如 '000001.SH' (上证指数)
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            DataFrame
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        try:
            df = self.pro.index_daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            return df
        except Exception as e:
            print(f"获取指数数据失败 {ts_code}: {e}")
            return None
    
    def get_futures_daily(self, ts_code: str, start_date: str, end_date: str = None) -> 'pd.DataFrame':
        """
        获取期货日K线
        
        Args:
            ts_code: 期货合约，如 'IF2106.CFE'
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            DataFrame
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        try:
            df = self.pro.futures_daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            return df
        except Exception as e:
            print(f"获取期货数据失败 {ts_code}: {e}")
            return None


def download_all_a_stocks(token: str, data_dir: str = "data/tushare", max_stocks: int = None):
    """
    下载全部A股数据
    
    Args:
        token: Tushare Token
        data_dir: 数据保存目录
        max_stocks: 最大股票数量
    """
    fetcher = TushareDataFetcher(token)
    
    # 获取股票列表
    print("📋 获取A股股票列表...")
    stocks = fetcher.get_stock_list('A')
    if stocks is None:
        print("❌ 获取股票列表失败")
        return
    
    stocks = stocks[stocks['list_status'] == 'L']  # 上市
    print(f"  ✓ 共 {len(stocks)} 只股票")
    
    os.makedirs(data_dir, exist_ok=True)
    
    success = 0
    failed = 0
    
    for i, row in stocks.iterrows():
        if max_stocks and i >= max_stocks:
            break
        
        ts_code = row['ts_code']
        name = row['name']
        
        filepath = f"{data_dir}/{ts_code}_{name}.csv"
        if os.path.exists(filepath):
            success += 1
            continue
        
        try:
            df = fetcher.get_stock_daily(ts_code, '20200101')
            if df is not None and len(df) > 0:
                df.to_csv(filepath, index=False)
                print(f"  ✓ {ts_code} {name}: {len(df)}条")
                success += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            print(f"  ✗ {ts_code}: {e}")
    
    print(f"\n✅ 完成: 成功{success}, 失败{failed}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Tushare数据获取')
    parser.add_argument('--token', type=str, required=True, help='Tushare API Token')
    parser.add_argument('--max', type=int, default=None, help='最大股票数量')
    parser.add_argument('--dir', type=str, default='data/tushare', help='数据目录')
    
    args = parser.parse_args()
    
    download_all_a_stocks(args.token, args.dir, args.max)
