#!/usr/bin/env python3
"""
基本面因子与选股模块
Fundamental Factor & Stock Screening
"""

import logging
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class FactorType(Enum):
    """因子类型"""
    VALUATION = "valuation"      # 估值因子
    PROFITABILITY = "profitability"  # 盈利因子
    GROWTH = "growth"            # 成长因子
    HEALTH = "health"            # 财务健康因子
    OPERATION = "operation"      # 营运因子
    DIVIDEND = "dividend"        # 股息因子
    SIZE = "size"                # 规模因子


@dataclass
class Factor:
    """因子定义"""
    name: str
    field: str  # 数据字段名
    factor_type: FactorType
    direction: int = 1  # 1: 越大越好, -1: 越小越好
    description: str = ""


# 常用因子定义
FACTORS = {
    # 估值因子 (越低越好)
    'pe': Factor('市盈率', 'pe', FactorType.VALUATION, -1, 'PE TTM'),
    'pe_ttm': Factor('市盈率TTM', 'pe_ttm', FactorType.VALUATION, -1, '滚动市盈率'),
    'pb': Factor('市净率', 'pb', FactorType.VALUATION, -1, '市净率'),
    'ps': Factor('市销率', 'ps', FactorType.VALUATION, -1, '市销率'),
    
    # 盈利因子 (越大越好)
    'roe': Factor('净资产收益率', 'roe', FactorType.PROFITABILITY, 1, 'ROE'),
    'roe_dt': Factor('净资产收益率(摊薄)', 'roe_dt', FactorType.PROFITABILITY, 1, 'ROE摊薄'),
    'net_profit_margin': Factor('净利率', 'net_profit_margin', FactorType.PROFITABILITY, 1, '净利润率'),
    'gross_profit_margin': Factor('毛利率', 'gross_profit_margin', FactorType.PROFITABILITY, 1, '毛利率'),
    'operating_profit_margin': Factor('营业利润率', 'operating_profit_margin', FactorType.PROFITABILITY, 1, '营业利润率'),
    
    # 成长因子 (越大越好)
    'revenue_growth': Factor('营收增长率', 'revenue_growth', FactorType.GROWTH, 1, '营业收入增长'),
    'net_profit_growth': Factor('净利润增长率', 'net_profit_growth', FactorType.GROWTH, 1, '净利润增长'),
    'total_assets_growth': Factor('总资产增长率', 'total_assets_growth', FactorType.GROWTH, 1, '总资产增长'),
    'equity_growth': Factor('净资产增长率', 'equity_growth', FactorType.GROWTH, 1, '净资产增长'),
    
    # 财务健康因子
    'current_ratio': Factor('流动比率', 'current_ratio', FactorType.HEALTH, 1, '流动资产/流动负债'),
    'quick_ratio': Factor('速动比率', 'quick_ratio', FactorType.HEALTH, 1, '速动资产/流动负债'),
    'debt_ratio': Factor('资产负债率', 'debt_ratio', FactorType.HEALTH, -1, '负债/资产'),
    'debt_to_equity': Factor('产权比率', 'debt_to_equity', FactorType.HEALTH, -1, '负债/所有者权益'),
    
    # 营运因子
    'inventory_turnover': Factor('存货周转率', 'inventory_turnover', FactorType.OPERATION, 1, '存货周转次数'),
    'receivable_turnover': Factor('应收账款周转率', 'receivable_turnover', FactorType.OPERATION, 1, '应收周转次数'),
    'asset_turnover': Factor('总资产周转率', 'asset_turnover', FactorType.OPERATION, 1, '总资产周转次数'),
    
    # 股息因子 (越大越好)
    'dividend_yield': Factor('股息率', 'dividend_yield', FactorType.DIVIDEND, 1, '分红/股价'),
    
    # 规模因子
    'market_cap': Factor('总市值', 'market_cap', FactorType.SIZE, 1, '总市值(亿)'),
    'circ_market_cap': Factor('流通市值', 'circ_market_cap', FactorType.SIZE, 1, '流通市值(亿)'),
}


class FundamentalScreener:
    """
    基本面选股器
    基于财务指标筛选股票
    """
    
    def __init__(self, data: pd.DataFrame = None):
        """
        初始化
        
        Args:
            data: 基本面数据DataFrame
        """
        self.data = data
    
    def load_data(self, data: pd.DataFrame):
        """加载数据"""
        self.data = data
    
    def load_from_csv(self, filepath: str):
        """从CSV加载数据"""
        self.data = pd.read_csv(filepath)
        logger.info(f"加载数据: {len(self.data)} 条")
    
    def filter(self, 
               conditions: Dict[str, tuple] = None,
               exclude_conditions: Dict[str, tuple] = None) -> pd.DataFrame:
        """
        筛选股票
        
        Args:
            conditions: 筛选条件, {'field': (min, max)}
            exclude_conditions: 排除条件
            
        Returns:
            筛选后的DataFrame
        """
        if self.data is None or len(self.data) == 0:
            logger.warning("无数据")
            return pd.DataFrame()
        
        df = self.data.copy()
        
        # 应用筛选条件
        if conditions:
            for field, (min_val, max_val) in conditions.items():
                if field not in df.columns:
                    logger.warning(f"字段不存在: {field}")
                    continue
                
                if min_val is not None:
                    df = df[df[field] >= min_val]
                if max_val is not None:
                    df = df[df[field] <= max_val]
        
        # 应用排除条件
        if exclude_conditions:
            for field, (min_val, max_val) in exclude_conditions.items():
                if field not in df.columns:
                    continue
                
                if min_val is not None:
                    df = df[df[field] < min_val]
                if max_val is not None:
                    df = df[df[field] > max_val]
        
        logger.info(f"筛选结果: {len(df)} 只股票")
        return df
    
    def rank(self, 
             factors: List[str], 
             weights: List[float] = None,
             top_n: int = 50) -> pd.DataFrame:
        """
        因子评分选股
        
        Args:
            factors: 因子列表
            weights: 因子权重
            top_n: 选取前N只
            
        Returns:
            排序后的DataFrame
        """
        if self.data is None or len(self.data) == 0:
            return pd.DataFrame()
        
        df = self.data.copy()
        
        # 默认权重
        if weights is None:
            weights = [1.0] * len(factors)
        
        # 归一化权重
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]
        
        # 计算每个因子的得分
        for i, factor in enumerate(factors):
            if factor not in df.columns:
                logger.warning(f"因子不存在: {factor}")
                continue
            
            # 获取因子定义
            factor_info = FACTORS.get(factor)
            direction = factor_info.direction if factor_info else 1
            
            # 去除空值
            col_data = df[factor].dropna()
            if len(col_data) == 0:
                continue
            
            # 标准化 (Min-Max)
            min_val = col_data.min()
            max_val = col_data.max()
            
            if max_val > min_val:
                if direction == 1:
                    df[f'{factor}_score'] = (df[factor] - min_val) / (max_val - min_val)
                else:
                    df[f'{factor}_score'] = (max_val - df[factor]) / (max_val - min_val)
            else:
                df[f'{factor}_score'] = 0.5
        
        # 计算综合得分
        score_cols = [f'{f}_score' for f in factors if f in df.columns]
        if score_cols:
            df['composite_score'] = sum(df[col] * weights[i] for i, col in enumerate(score_cols) if i < len(weights))
            
            # 排序
            df = df.sort_values('composite_score', ascending=False)
            
            # 取前N只
            if top_n > 0:
                df = df.head(top_n)
        
        logger.info(f"因子评分选股: {len(df)} 只")
        return df
    
    def get_low_pe_stocks(self, max_pe: float = 20, min_pe: float = 0) -> pd.DataFrame:
        """低PE选股 (价值投资)"""
        return self.filter({'pe': (min_pe, max_pe)})
    
    def get_high_roe_stocks(self, min_roe: float = 15) -> pd.DataFrame:
        """高ROE选股 (盈利能力)"""
        return self.filter({'roe': (min_roe, None)})
    
    def get_growth_stocks(self, min_growth: float = 20) -> pd.DataFrame:
        """成长股筛选"""
        return self.filter({'net_profit_growth': (min_growth, None)})
    
    def get_dividend_stocks(self, min_yield: float = 2) -> pd.DataFrame:
        """高股息选股"""
        return self.filter({'dividend_yield': (min_yield, None)})
    
    def get_quality_stocks(self, 
                          min_roe: float = 15,
                          max_debt: float = 50,
                          min_growth: float = 10) -> pd.DataFrame:
        """优质股筛选 (高ROE + 低负债 + 成长)"""
        return self.filter({
            'roe': (min_roe, None),
            'debt_ratio': (None, max_debt),
            'net_profit_growth': (min_growth, None)
        })


class FactorCalculator:
    """
    因子计算器
    计算复合因子
    """
    
    @staticmethod
    def calculate_peg(pe: float, growth: float) -> float:
        """计算PEG"""
        if pe and growth and growth > 0:
            return pe / growth
        return None
    
    @staticmethod
    def calculate_pb_roe(pb: float, roe: float) -> float:
        """计算PB-ROE因子 (巴菲特的观点)"""
        if pb and roe and pb > 0:
            return roe / pb
        return None
    
    @staticmethod
    def calculate_ev_ebitda(ev: float, ebitda: float) -> float:
        """计算EV/EBITDA"""
        if ev and ebitda and ebitda > 0:
            return ev / ebitda
        return None
    
    @staticmethod
    def add_factors(df: pd.DataFrame) -> pd.DataFrame:
        """添加计算因子"""
        result = df.copy()
        
        # PEG
        if 'pe' in result.columns and 'net_profit_growth' in result.columns:
            result['peg'] = result.apply(
                lambda x: FactorCalculator.calculate_peg(x['pe'], x['net_profit_growth']), 
                axis=1
            )
        
        # PB-ROE
        if 'pb' in result.columns and 'roe' in result.columns:
            result['pb_roe'] = result.apply(
                lambda x: FactorCalculator.calculate_pb_roe(x['pb'], x['roe']),
                axis=1
            )
        
        return result


# 预设选股策略
SCREENING_STRATEGIES = {
    '价值投资': {
        'factors': ['pe', 'pb', 'roe', 'dividend_yield'],
        'weights': [0.3, 0.2, 0.3, 0.2],
        'top_n': 30
    },
    '成长投资': {
        'factors': ['revenue_growth', 'net_profit_growth', 'roe', 'asset_turnover'],
        'weights': [0.3, 0.3, 0.2, 0.2],
        'top_n': 30
    },
    '质量投资': {
        'factors': ['roe', 'gross_profit_margin', 'current_ratio', 'debt_ratio'],
        'weights': [0.4, 0.2, 0.2, 0.2],
        'top_n': 30
    },
    '红利策略': {
        'factors': ['dividend_yield', 'roe', 'debt_ratio', 'pe'],
        'weights': [0.5, 0.2, 0.15, 0.15],
        'top_n': 30
    },
    '低估价值': {
        'factors': ['pe', 'pb', 'ps', 'market_cap'],
        'weights': [0.35, 0.35, 0.15, 0.15],
        'top_n': 50
    },
    '均衡策略': {
        'factors': ['roe', 'revenue_growth', 'net_profit_growth', 'dividend_yield', 'debt_ratio'],
        'weights': [0.25, 0.2, 0.2, 0.15, 0.2],
        'top_n': 30
    }
}


def test():
    """测试"""
    # 模拟数据
    data = pd.DataFrame({
        'code': ['600519', '000858', '601318'],
        'name': ['贵州茅台', '五粮液', '中国平安'],
        'pe': [35.0, 25.0, 12.0],
        'pb': [10.0, 5.0, 1.5],
        'roe': [30.0, 25.0, 15.0],
        'net_profit_growth': [15.0, 20.0, 10.0],
        'dividend_yield': [1.5, 2.0, 3.0],
        'debt_ratio': [30.0, 40.0, 70.0]
    })
    
    screener = FundamentalScreener(data)
    
    # 测试筛选
    print("=== 低PE股票 ===")
    result = screener.get_low_pe_stocks(max_pe=20)
    print(result[['code', 'name', 'pe']])
    
    print("\n=== 优质股票 ===")
    result = screener.get_quality_stocks(min_roe=15, max_debt=50)
    print(result[['code', 'name', 'roe', 'debt_ratio']])
    
    print("\n=== 因子评分 ===")
    result = screener.rank(['pe', 'roe', 'dividend_yield'], weights=[0.4, 0.4, 0.2], top_n=10)
    print(result[['code', 'name', 'composite_score']].head())


if __name__ == "__main__":
    test()
