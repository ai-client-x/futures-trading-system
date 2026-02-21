#!/usr/bin/env python3
"""
混合选股策略
结合基本面筛选和技术面信号
"""

import logging
from typing import List, Dict, Optional
import sqlite3
import pandas as pd
import os
import sys

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ..models import Signal
from ..config import config
from ..signals.fundamental_generator import HybridSignalGenerator
from ..signals.generator import CompositeSignalGenerator
from ..signal_strength import calc_signal_strength

logger = logging.getLogger(__name__)


class HybridStrategy:
    """
    混合选股策略
    基本面筛选 + 技术面择时
    """
    
    def __init__(self, db_path: str = "data/stocks.db"):
        self.db_path = db_path
        self.signal_generator = HybridSignalGenerator(db_path)
        self.config = config
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def screen_candidates(self, 
                         max_pe: float = 25,
                         min_roe: float = 10,
                         min_dv_ratio: float = 1,
                         max_debt: float = 70,
                         min_market_cap: float = 30,
                         limit: int = 100) -> List[Dict]:
        """
        基本面初选
        
        Args:
            max_pe: 最大市盈率
            min_roe: 最小ROE(%)
            min_dv_ratio: 最小股息率(%)
            max_debt: 最大资产负债率(%)
            min_market_cap: 最小市值(亿)
            limit: 返回数量
        
        Returns:
            候选股票列表
        """
        conn = self.get_connection()
        
        query = f"""
            SELECT 
                ts_code, name, close, pe, roe, dv_ratio, 
                debt_to_assets, market_cap
            FROM fundamentals
            WHERE 1=1
        """
        
        if max_pe:
            query += f" AND pe > 0 AND pe <= {max_pe}"
        if min_roe:
            query += f" AND roe >= {min_roe}"
        if min_dv_ratio:
            query += f" AND dv_ratio >= {min_dv_ratio}"
        if max_debt < 100:
            query += f" AND debt_to_assets <= {max_debt}"
        if min_market_cap:
            query += f" AND market_cap >= {min_market_cap}"
        
        query += f" ORDER BY roe DESC, dv_ratio DESC LIMIT {limit}"
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df.to_dict('records')
    
    def generate_signals(self, candidates: List[Dict] = None) -> List[Signal]:
        """
        为候选股票生成交易信号 (含动态评分)
        
        Args:
            candidates: 候选股票列表，默认从基本面筛选
        
        Returns:
            Signal 列表 (包含动态信号强度评分)
        """
        signals = []
        
        # 如果没有提供候选，则进行基本面筛选
        if candidates is None:
            candidates = self.screen_candidates()
        
        logger.info(f"为 {len(candidates)} 只候选股票生成信号...")
        
        for i, candidate in enumerate(candidates):
            ts_code = candidate['ts_code']
            
            signal = self.signal_generator.generate(ts_code)
            
            if signal:
                # 获取历史数据计算动态评分
                try:
                    conn = self.get_connection()
                    df = pd.read_sql(f"SELECT * FROM daily WHERE ts_code='{ts_code}' ORDER BY trade_date DESC LIMIT 60", conn)
                    conn.close()
                    
                    if len(df) >= 30:
                        df = df.sort_values('trade_date')
                        df = df.rename(columns={'close': 'Close', 'high': 'High', 'low': 'Low', 'vol': 'Volume'})
                        
                        # 计算各策略信号强度
                        total_score = 0
                        count = 0
                        for strat in ['威廉指标', 'RSI逆势', '动量反转', '布林带', '成交量突破', 'MACD+成交量']:
                            score = calc_signal_strength(df, strat, 'buy')
                            if score > 0:
                                total_score += score
                                count += 1
                        
                        # 平均信号强度
                        signal.strength = total_score / count if count > 0 else 50
                except:
                    pass
                
                signals.append(signal)
            
            if (i + 1) % 20 == 0:
                logger.info(f"已处理: {i+1}/{len(candidates)}")
        
        return signals
    
    def get_buy_signals(self, 
                        max_pe: float = 25,
                        min_roe: float = 10,
                        min_dv_ratio: float = 1,
                        max_debt: float = 70,
                        min_market_cap: float = 30,
                        hybrid_mode: str = "strict") -> List[Signal]:
        """
        获取买入信号
        
        Args:
            hybrid_mode: "strict"(严格), "loose"(宽松), "fundamental_only"(仅基本面)
        
        Returns:
            买入信号列表
        """
        # 基本面初选
        candidates = self.screen_candidates(
            max_pe=max_pe,
            min_roe=min_roe,
            min_dv_ratio=min_dv_ratio,
            max_debt=max_debt,
            min_market_cap=min_market_cap,
            limit=100
        )
        
        logger.info(f"基本面初选: {len(candidates)} 只")
        
        # 生成信号
        signals = []
        for candidate in candidates:
            ts_code = candidate['ts_code']
            
            signal = self.signal_generator.generate(
                ts_code, 
                fundamental_conditions={
                    'max_pe': max_pe,
                    'min_roe': min_roe,
                    'min_dv_ratio': min_dv_ratio,
                    'max_debt': max_debt,
                    'min_market_cap': min_market_cap
                },
                hybrid_mode=hybrid_mode
            )
            
            if signal and signal.action == "buy":
                signals.append(signal)
        
        # 按强度排序
        signals.sort(key=lambda x: x.strength, reverse=True)
        
        logger.info(f"买入信号: {len(signals)} 只")
        
        return signals
    
    def print_signals(self, signals: List[Signal], title: str = "交易信号"):
        """打印信号"""
        print("\n" + "=" * 70)
        print(f"📈 {title} (共 {len(signals)} 只)")
        print("=" * 70)
        
        for i, signal in enumerate(signals, 1):
            ind = signal.indicators
            print(f"\n{i}. {signal.code} {signal.name}")
            print(f"   价格: ¥{signal.price:.2f} | 操作: {signal.action} | 强度: {signal.strength}")
            print(f"   估值: PE={ind.get('pe', 'N/A'):.1f}, ROE={ind.get('roe', 'N/A'):.1f}%, 股息={ind.get('dv_ratio', 'N/A'):.2f}%")
            print(f"   技术: {ind.get('tech_action', 'N/A')} (强度:{ind.get('tech_strength', 0)})")
            print(f"   原因: {signal.reason}")
        
        print("\n" + "=" * 70)


def run_hybrid_strategy():
    """运行混合选股策略"""
    logging.basicConfig(level=logging.INFO)
    
    strategy = HybridStrategy()
    
    # 获取买入信号
    signals = strategy.get_buy_signals(
        max_pe=25,
        min_roe=10,
        min_dv_ratio=1,
        max_debt=70,
        min_market_cap=30,
        hybrid_mode="strict"
    )
    
    # 打印结果
    strategy.print_signals(signals, "混合选股策略 - 买入信号")
    
    return signals


if __name__ == "__main__":
    run_hybrid_strategy()
