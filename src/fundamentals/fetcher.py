#!/usr/bin/env python3
"""
基本面数据获取模块
Fundamental Data Fetcher
"""

import os
import logging
from datetime import datetime
from typing import Optional, Dict, List
from dataclasses import dataclass

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class FundamentalData:
    """基本面数据"""
    code: str
    name: str
    
    # 估值指标
    pe: Optional[float] = None  # 市盈率
    pe_ttm: Optional[float] = None  # 市盈率TTM
    pb: Optional[float] = None  # 市净率
    ps: Optional[float] = None  # 市销率
    ps_ttm: Optional[float] = None  # 市销率TTM
    market_cap: Optional[float] = None  # 总市值(亿)
    circ_market_cap: Optional[float] = None  # 流通市值(亿)
    
    # 盈利能力
    roe: Optional[float] = None  # 净资产收益率(%)
    roe_dt: Optional[float] = None  # 净资产收益率(摊薄)(%)
    net_profit_margin: Optional[float] = None  # 净利率(%)
    gross_profit_margin: Optional[float] = None  # 毛利率(%)
    expense_ratio: Optional[float] = None  # 费用率(%)
    operating_profit_margin: Optional[float] = None  # 营业利润率(%)
    
    # 成长能力
    revenue_growth: Optional[float] = None  # 营业收入增长率(%)
    net_profit_growth: Optional[float] = None  # 净利润增长率(%)
    total_assets_growth: Optional[float] = None  # 总资产增长率(%)
    equity_growth: Optional[float] = None  # 净资产增长率(%)
    
    # 财务健康
    current_ratio: Optional[float] = None  # 流动比率
    quick_ratio: Optional[float] = None  # 速动比率
    debt_ratio: Optional[float] = None  # 资产负债率(%)
    debt_to_equity: Optional[float] = None  # 产权比率
    
    # 营运能力
    inventory_turnover: Optional[float] = None  # 存货周转率
    receivable_turnover: Optional[float] = None  # 应收账款周转率
    asset_turnover: Optional[float] = None  # 总资产周转率
    
    # 股息
    dividend_yield: Optional[float] = None  # 股息率(%)
    dividend_ratio: Optional[float] = None  # 分红送转比例
    
    # 基本信息
    industry: Optional[str] = None  # 所属行业
    market: Optional[str] = None  # 市场(沪/深)
    list_date: Optional[str] = None  # 上市日期
    total_shares: Optional[float] = None  # 总股本(亿股)
    circ_shares: Optional[float] = None  # 流通股本(亿股)
    
    # 更新时间
    report_date: Optional[str] = None  # 报告期
    update_time: Optional[str] = None  # 更新时间


class FundamentalFetcher:
    """
    基本面数据获取器
    支持 Tushare Pro API
    """
    
    def __init__(self, pro_api, cache_dir: str = None):
        """
        初始化
        
        Args:
            pro_api: Tushare Pro API 实例
            cache_dir: 缓存目录
        """
        self.pro = pro_api
        self.cache_dir = cache_dir or "data/fundamentals"
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def get_stock_basic(self, ts_code: str = None) -> pd.DataFrame:
        """
        获取股票基本信息
        
        Args:
            ts_code: 股票代码，默认全部
            
        Returns:
            DataFrame
        """
        try:
            df = self.pro.stock_basic(
                ts_code=ts_code,
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market,list_date,delist_date,is_hs'
            )
            return df
        except Exception as e:
            logger.error(f"获取股票基本信息失败: {e}")
            return None
    
    def get_financial_indicator(self, ts_code: str, start_date: str = None) -> pd.DataFrame:
        """
        获取财务指标
        
        Args:
            ts_code: 股票代码，如 '000001.SZ'
            start_date: 开始日期，默认前5年
            
        Returns:
            DataFrame
        """
        if start_date is None:
            # 默认取最近4个季度的数据
            start_date = '20200101'
        
        try:
            df = self.pro.financial_indicator(
                ts_code=ts_code,
                start_date=start_date
            )
            return df
        except Exception as e:
            logger.error(f"获取财务指标失败 {ts_code}: {e}")
            return None
    
    def get_fina_indicator(self, ts_code: str, start_date: str = None) -> pd.DataFrame:
        """
        获取主要财务指标 (fina_indicator)
        这个接口更常用
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            
        Returns:
            DataFrame
        """
        if start_date is None:
            start_date = '20200101'
        
        try:
            df = self.pro.fina_indicator(
                ts_code=ts_code,
                start_date=start_date,
                fields='ts_code,end_date,roe,netprofit_margin,grossprofit_margin,'
                      'expense_ratio,operate_profit_margin,profit_to_gr,'
                      'roa,roe_dt,netprofit_margin_dt,'
                      'grossprofit_margin_dt,expense_ratio_dt,operate_profit_margin_dt,'
                      'revenue_growth_year,netprofit_growth_year,total_assets,total_liab,'
                      'current_ratio,quick_ratio,debt_to_assets,debt_to_equity,'
                      'inventory_turnover,receivable_turnover,assets_turnover'
            )
            return df
        except Exception as e:
            logger.error(f"获取主要财务指标失败 {ts_code}: {e}")
            return None
    
    def get_daily_basic(self, ts_code: str = None, trade_date: str = None) -> pd.DataFrame:
        """
        获取每日指标 (估值指标)
        
        Args:
            ts_code: 股票代码
            trade_date: 交易日期
            
        Returns:
            DataFrame: 包含 pe, pb, ps, dv_ratio, circ_market_cap 等
        """
        try:
            df = self.pro.daily_basic(
                ts_code=ts_code,
                trade_date=trade_date,
                fields='ts_code,trade_date,close,pe,pe_ttm,pb,ps,ps_ttm,'
                      'dv_ratio,dv_ttm,total_share,float_share,free_share,'
                      'total_mv,circ_mv'
            )
            return df
        except Exception as e:
            logger.error(f"获取每日指标失败: {e}")
            return None
    
    def get_fina_mainbz(self, ts_code: str) -> pd.DataFrame:
        """
        获取主营业务
        
        Args:
            ts_code: 股票代码
            
        Returns:
            DataFrame
        """
        try:
            df = self.pro.fina_mainbz(
                ts_code=ts_code,
                type='1'  # 按产品
            )
            return df
        except Exception as e:
            logger.error(f"获取主营业务失败 {ts_code}: {e}")
            return None
    
    def get_stock_company(self, ts_code: str = None) -> pd.DataFrame:
        """
        获取公司信息
        
        Args:
            ts_code: 股票代码
            
        Returns:
            DataFrame
        """
        try:
            df = self.pro.stock_company(
                ts_code=ts_code,
                fields='ts_code,exchange,chairman,manager,reg_capital,setup_date,province,city,introduction,website,email,office,employees'
            )
            return df
        except Exception as e:
            logger.error(f"获取公司信息失败: {e}")
            return None
    
    def get_shareholder(self, ts_code: str, start_date: str = None) -> pd.DataFrame:
        """
        获取股东人数
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            
        Returns:
            DataFrame
        """
        try:
            df = self.pro.stock_shareholder(
                ts_code=ts_code,
                start_date=start_date or '20200101'
            )
            return df
        except Exception as e:
            logger.error(f"获取股东人数失败 {ts_code}: {e}")
            return None
    
    def get_growth_indicator(self, ts_code: str) -> pd.DataFrame:
        """
        获取成长能力指标
        
        Args:
            ts_code: 股票代码
            
        Returns:
            DataFrame
        """
        try:
            df = self.pro.growth_indicator(
                ts_code=ts_code,
                fields='ts_code,end_date,netprofit_margin,grossprofit_margin,'
                      'revenue_growth,netprofit_growth,total_assets_growth,equity_growth'
            )
            return df
        except Exception as e:
            logger.error(f"获取成长指标失败 {ts_code}: {e}")
            return None
    
    def get_liability_indicator(self, ts_code: str) -> pd.DataFrame:
        """
        获取负债指标
        
        Args:
            ts_code: 股票代码
            
        Returns:
            DataFrame
        """
        try:
            df = self.pro.liability_indicator(
                ts_code=ts_code,
                fields='ts_code,end_date,current_ratio,quick_ratio,'
                      'debt_to_assets,debt_to_equity'
            )
            return df
        except Exception as e:
            logger.error(f"获取负债指标失败 {ts_code}: {e}")
            return None
    
    def get_operate_indicator(self, ts_code: str) -> pd.DataFrame:
        """
        获取营运能力指标
        
        Args:
            ts_code: 股票代码
            
        Returns:
            DataFrame
        """
        try:
            df = self.pro.operate_indicator(
                ts_code=ts_code,
                fields='ts_code,end_date,inventory_turnover,receivable_turnover,assets_turnover'
            )
            return df
        except Exception as e:
            logger.error(f"获取营运指标失败 {ts_code}: {e}")
            return None
    
    def get_valuation(self, ts_code: str = None, trade_date: str = None) -> pd.DataFrame:
        """
        获取估值指标
        
        Args:
            ts_code: 股票代码
            trade_date: 交易日期
            
        Returns:
            DataFrame
        """
        try:
            df = self.pro.daily_basic(
                ts_code=ts_code,
                trade_date=trade_date,
                fields='ts_code,trade_date,close,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv,circ_mv'
            )
            return df
        except Exception as e:
            logger.error(f"获取估值指标失败: {e}")
            return None
    
    def get_fundamental_data(self, ts_code: str) -> Optional[FundamentalData]:
        """
        获取完整基本面数据
        
        Args:
            ts_code: 股票代码
            
        Returns:
            FundamentalData 对象
        """
        try:
            # 获取基本信息
            basic_df = self.pro.stock_basic(ts_code=ts_code)
            if basic_df is None or len(basic_df) == 0:
                return None
            
            basic = basic_df.iloc[0]
            
            # 获取最新估值数据
            val_df = self.get_valuation(ts_code=ts_code)
            val = None
            if val_df is not None and len(val_df) > 0:
                val = val_df.iloc[-1]
            
            # 获取最新财务指标
            fina_df = self.get_fina_indicator(ts_code=ts_code)
            fina = None
            if fina_df is not None and len(fina_df) > 0:
                fina = fina_df.iloc[-1]
            
            # 构建基本面数据对象
            fd = FundamentalData(
                code=ts_code,
                name=basic.get('name', ''),
                industry=basic.get('industry', ''),
                market=basic.get('market', ''),
                list_date=basic.get('list_date', ''),
            )
            
            # 估值指标
            if val is not None:
                fd.pe = val.get('pe')
                fd.pe_ttm = val.get('pe_ttm')
                fd.pb = val.get('pb')
                fd.ps = val.get('ps')
                fd.ps_ttm = val.get('ps_ttm')
                fd.dividend_yield = val.get('dv_ratio')
                fd.market_cap = val.get('total_mv') / 10000 if val.get('total_mv') else None  # 亿
                fd.circ_market_cap = val.get('circ_mv') / 10000 if val.get('circ_mv') else None
                fd.total_shares = val.get('total_share') / 10000 if val.get('total_share') else None  # 亿
                fd.circ_shares = val.get('float_share') / 10000 if val.get('float_share') else None
            
            # 财务指标
            if fina is not None:
                fd.roe = fina.get('roe')
                fd.roe_dt = fina.get('roe_dt')
                fd.net_profit_margin = fina.get('netprofit_margin')
                fd.gross_profit_margin = fina.get('grossprofit_margin')
                fd.expense_ratio = fina.get('expense_ratio')
                fd.operating_profit_margin = fina.get('operate_profit_margin')
                fd.current_ratio = fina.get('current_ratio')
                fd.quick_ratio = fina.get('quick_ratio')
                fd.debt_ratio = fina.get('debt_to_assets')
                fd.debt_to_equity = fina.get('debt_to_equity')
                fd.inventory_turnover = fina.get('inventory_turnover')
                fd.receivable_turnover = fina.get('receivable_turnover')
                fd.asset_turnover = fina.get('assets_turnover')
                fd.report_date = fina.get('end_date')
            
            fd.update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return fd
            
        except Exception as e:
            logger.error(f"获取基本面数据失败 {ts_code}: {e}")
            return None
    
    def get_all_valuation(self, trade_date: str = None) -> pd.DataFrame:
        """
        批量获取全部A股估值数据
        
        Args:
            trade_date: 交易日期，默认最新
            
        Returns:
            DataFrame
        """
        try:
            # 先获取所有股票列表
            stocks = self.get_stock_basic()
            if stocks is None:
                return None
            
            # 获取最新交易日期
            if trade_date is None:
                daily = self.pro.daily_basic(trade_date='', fields='trade_date')
                if daily is not None and len(daily) > 0:
                    trade_date = daily['trade_date'].max()
            
            # 批量获取估值 (需要分批)
            all_data = []
            batch_size = 5000
            codes = stocks['ts_code'].tolist()
            
            for i in range(0, len(codes), batch_size):
                batch_codes = codes[i:i+batch_size]
                df = self.pro.daily_basic(
                    ts_code=','.join(batch_codes),
                    trade_date=trade_date,
                    fields='ts_code,close,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,total_mv,circ_mv'
                )
                if df is not None:
                    all_data.append(df)
                
                logger.info(f"获取估值数据: {i+len(batch_codes)}/{len(codes)}")
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            return None
            
        except Exception as e:
            logger.error(f"批量获取估值数据失败: {e}")
            return None
    
    def save_to_csv(self, fd: FundamentalData, filepath: str = None):
        """保存基本面数据到CSV"""
        if filepath is None:
            filepath = os.path.join(self.cache_dir, f"{fd.code}.csv")
        
        data = {
            'code': fd.code,
            'name': fd.name,
            'pe': fd.pe,
            'pe_ttm': fd.pe_ttm,
            'pb': fd.pb,
            'ps': fd.ps,
            'ps_ttm': fd.ps_ttm,
            'market_cap': fd.market_cap,
            'circ_market_cap': fd.circ_market_cap,
            'roe': fd.roe,
            'net_profit_margin': fd.net_profit_margin,
            'gross_profit_margin': fd.gross_profit_margin,
            'expense_ratio': fd.expense_ratio,
            'operating_profit_margin': fd.operating_profit_margin,
            'current_ratio': fd.current_ratio,
            'quick_ratio': fd.quick_ratio,
            'debt_ratio': fd.debt_ratio,
            'debt_to_equity': fd.debt_to_equity,
            'inventory_turnover': fd.inventory_turnover,
            'receivable_turnover': fd.receivable_turnover,
            'asset_turnover': fd.asset_turnover,
            'dividend_yield': fd.dividend_yield,
            'industry': fd.industry,
            'market': fd.market,
            'list_date': fd.list_date,
            'total_shares': fd.total_shares,
            'circ_shares': fd.circ_shares,
            'report_date': fd.report_date,
            'update_time': fd.update_time
        }
        
        df = pd.DataFrame([data])
        df.to_csv(filepath, index=False)
        logger.info(f"已保存基本面数据: {filepath}")


def test():
    """测试函数"""
    import tushare as ts
    ts.set_token('your_token_here')
    pro = ts.pro_api()
    
    fetcher = FundamentalFetcher(pro)
    
    # 测试获取单只股票基本面
    fd = fetcher.get_fundamental_data('600519.SH')
    if fd:
        print(f"\n=== 贵州茅台基本面数据 ===")
        print(f"代码: {fd.code} {fd.name}")
        print(f"行业: {fd.industry}")
        print(f"PE: {fd.pe}")
        print(f"PB: {fd.pb}")
        print(f"ROE: {fd.roe}")
        print(f"毛利率: {fd.gross_profit_margin}%")
        print(f"净利率: {fd.net_profit_margin}%")
        print(f"市值: {fd.market_cap}亿")


if __name__ == "__main__":
    test()
