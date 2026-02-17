#!/usr/bin/env python3
"""
基本面数据下载脚本
下载A股全部股票的基本面数据
"""

import os
import sys
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.fundamentals.fetcher import FundamentalFetcher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FundamentalDownloader:
    """
    基本面数据下载器
    """
    
    def __init__(self, token: str, data_dir: str = "data/fundamentals"):
        """
        初始化
        
        Args:
            token: Tushare API Token
            data_dir: 数据保存目录
        """
        import tushare as ts
        ts.set_token(token)
        self.pro = ts.pro_api()
        
        self.fetcher = FundamentalFetcher(self.pro, data_dir)
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
    
    def get_stock_list(self) -> list:
        """获取股票列表"""
        df = self.pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,symbol,name,industry,market,list_date'
        )
        if df is not None:
            return df.to_dict('records')
        return []
    
    def download_single(self, ts_code: str) -> dict:
        """
        下载单只股票基本面
        
        Returns:
            dict: 股票基本信息
        """
        try:
            # 获取基本信息
            basic_df = self.pro.stock_basic(ts_code=ts_code)
            if basic_df is None or len(basic_df) == 0:
                return None
            
            basic = basic_df.iloc[0]
            
            result = {
                'ts_code': ts_code,
                'name': basic.get('name', ''),
                'industry': basic.get('industry', ''),
                'market': basic.get('market', ''),
                'list_date': basic.get('list_date', ''),
            }
            
            # 获取估值数据
            try:
                val_df = self.pro.daily_basic(
                    ts_code=ts_code,
                    fields='ts_code,trade_date,close,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,total_mv,circ_mv,total_share,float_share'
                )
                if val_df is not None and len(val_df) > 0:
                    val = val_df.iloc[-1]
                    result.update({
                        'close': val.get('close'),
                        'pe': val.get('pe'),
                        'pe_ttm': val.get('pe_ttm'),
                        'pb': val.get('pb'),
                        'ps': val.get('ps'),
                        'ps_ttm': val.get('ps_ttm'),
                        'dividend_yield': val.get('dv_ratio'),
                        'market_cap': val.get('total_mv') / 10000 if val.get('total_mv') else None,
                        'circ_market_cap': val.get('circ_mv') / 10000 if val.get('circ_mv') else None,
                        'total_shares': val.get('total_share') / 10000 if val.get('total_share') else None,
                        'circ_shares': val.get('float_share') / 10000 if val.get('float_share') else None,
                        'trade_date': val.get('trade_date'),
                    })
            except Exception as e:
                logger.debug(f"获取估值失败 {ts_code}: {e}")
            
            # 获取财务指标
            try:
                fina_df = self.pro.fina_indicator(
                    ts_code=ts_code,
                    fields='ts_code,end_date,roe,netprofit_margin,grossprofit_margin,expense_ratio,operate_profit_margin,current_ratio,quick_ratio,debt_to_assets,debt_to_equity'
                )
                if fina_df is not None and len(fina_df) > 0:
                    fina = fina_df.iloc[-1]
                    result.update({
                        'roe': fina.get('roe'),
                        'net_profit_margin': fina.get('netprofit_margin'),
                        'gross_profit_margin': fina.get('grossprofit_margin'),
                        'expense_ratio': fina.get('expense_ratio'),
                        'operating_profit_margin': fina.get('operate_profit_margin'),
                        'current_ratio': fina.get('current_ratio'),
                        'quick_ratio': fina.get('quick_ratio'),
                        'debt_ratio': fina.get('debt_to_assets'),
                        'debt_to_equity': fina.get('debt_to_equity'),
                        'report_date': fina.get('end_date'),
                    })
            except Exception as e:
                logger.debug(f"获取财务指标失败 {ts_code}: {e}")
            
            result['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return result
            
        except Exception as e:
            logger.error(f"下载失败 {ts_code}: {e}")
            return None
    
    def download_all(self, max_stocks: int = None, rate_limit: float = 0.5):
        """
        下载全部股票基本面
        
        Args:
            max_stocks: 最大股票数量
            rate_limit: 请求间隔(秒)
        """
        stocks = self.get_stock_list()
        if not stocks:
            logger.error("获取股票列表失败")
            return
        
        logger.info(f"开始下载 {len(stocks)} 只股票基本面数据...")
        
        all_data = []
        success = 0
        failed = 0
        
        for i, stock in enumerate(stocks[:max_stocks]):
            ts_code = stock['ts_code']
            
            if i > 0 and i % rate_limit:
                time.sleep(rate_limit)
            
            data = self.download_single(ts_code)
            
            if data:
                all_data.append(data)
                success += 1
                logger.info(f"[{i+1}/{min(len(stocks), max_stocks or len(stocks))}] ✓ {ts_code} {data.get('name', '')}")
            else:
                failed += 1
                logger.warning(f"[{i+1}] ✗ {ts_code}")
            
            # 每100只保存一次
            if success > 0 and success % 500 == 0:
                self.save_partial(all_data)
        
        # 保存完整数据
        if all_data:
            df = pd.DataFrame(all_data)
            filepath = os.path.join(self.data_dir, 'all_stocks_fundamentals.csv')
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"已保存到: {filepath}")
        
        logger.info(f"完成: 成功 {success}, 失败 {failed}")
        return all_data
    
    def save_partial(self, data: list):
        """保存部分数据"""
        if data:
            df = pd.DataFrame(data)
            filepath = os.path.join(self.data_dir, 'all_stocks_fundamentals_partial.csv')
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"已保存部分数据: {len(data)} 条")
    
    def download_valuation_batch(self, batch_size: int = 1000):
        """
        批量快速获取估值数据
        
        Args:
            batch_size: 每批数量
        """
        stocks = self.get_stock_list()
        logger.info(f"批量获取 {len(stocks)} 只股票估值数据...")
        
        all_data = []
        
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            codes = [s['ts_code'] for s in batch]
            
            try:
                # 获取最新交易日
                trade_date = ''
                daily_df = self.pro.daily_basic(trade_date=trade_date, fields='trade_date')
                if daily_df is not None and len(daily_df) > 0:
                    trade_date = daily_df['trade_date'].max()
                
                # 批量获取
                df = self.pro.daily_basic(
                    ts_code=','.join(codes),
                    trade_date=trade_date,
                    fields='ts_code,close,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,total_mv,circ_mv,total_share,float_share'
                )
                
                if df is not None:
                    all_data.append(df)
                    logger.info(f"获取: {i+len(batch)}/{len(stocks)}")
                    
            except Exception as e:
                logger.error(f"批量获取失败: {e}")
            
            time.sleep(1)  # 避免限流
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            
            # 转换单位
            result['market_cap'] = result['total_mv'] / 10000
            result['circ_market_cap'] = result['circ_mv'] / 10000
            result['total_shares'] = result['total_share'] / 10000
            result['circ_shares'] = result['float_share'] / 10000
            
            # 添加股票名称
            stock_map = {s['ts_code']: s['name'] for s in stocks}
            result['name'] = result['ts_code'].map(stock_map)
            
            # 保存
            filepath = os.path.join(self.data_dir, 'valuation_data.csv')
            result.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"估值数据已保存: {filepath}, 共 {len(result)} 条")
            
            return result
        
        return None


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='基本面数据下载')
    parser.add_argument('--token', type=str, required=True, help='Tushare API Token')
    parser.add_argument('--dir', type=str, default='data/fundamentals', help='数据目录')
    parser.add_argument('--max', type=int, default=None, help='最大股票数量')
    parser.add_argument('--batch', action='store_true', help='批量快速获取估值')
    
    args = parser.parse_args()
    
    downloader = FundamentalDownloader(args.token, args.dir)
    
    if args.batch:
        # 批量快速获取
        downloader.download_valuation_batch()
    else:
        # 完整下载
        downloader.download_all(max_stocks=args.max)


if __name__ == "__main__":
    main()
