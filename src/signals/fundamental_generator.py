#!/usr/bin/env python3
"""
基本面信号生成器
Fundamental Signal Generator
"""

import logging
from datetime import datetime
from typing import Optional, Dict
from abc import ABC, abstractmethod

import pandas as pd
import numpy as np
import sqlite3

from ..models import Signal
from ..config import config

logger = logging.getLogger(__name__)


class FundamentalSignalGenerator:
    """
    基本面信号生成器
    基于基本面数据生成买入/卖出信号
    """
    
    def __init__(self, db_path: str = "data/stocks.db"):
        self.db_path = db_path
    
    def _get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def get_fundamental_data(self, ts_code: str) -> Optional[Dict]:
        """
        获取单只股票的基本面数据
        
        Args:
            ts_code: 股票代码
        
        Returns:
            基本面数据字典
        """
        conn = self._get_connection()
        
        try:
            df = pd.read_sql(f"""
                SELECT * FROM fundamentals WHERE ts_code = '{ts_code}'
            """, conn)
            
            conn.close()
            
            if df is not None and len(df) > 0:
                return df.iloc[0].to_dict()
            
        except Exception as e:
            logger.error(f"获取基本面数据失败 {ts_code}: {e}")
        finally:
            conn.close()
        
        return None
    
    def get_fundamental_scores(self, ts_code: str, conditions: Dict = None) -> Dict:
        """
        获取基本面评分
        
        Args:
            ts_code: 股票代码
            conditions: 筛选条件配置
        
        Returns:
            评分结果
        """
        if conditions is None:
            conditions = {
                'max_pe': 25,
                'min_roe': 10,
                'min_dv_ratio': 1,
                'max_debt': 70,
                'min_market_cap': 30
            }
        
        fundamental = self.get_fundamental_data(ts_code)
        
        if fundamental is None:
            return {
                'valid': False,
                'reason': '无基本面数据',
                'score': 0
            }
        
        score = 0
        reasons = []
        valid = True
        
        # PE 评分 (越低越好, 但不能为负)
        pe = fundamental.get('pe')
        if pe and pe > 0:
            if pe <= conditions.get('max_pe', 25):
                score += 25
                reasons.append(f"PE={pe:.1f}符合")
            elif pe <= conditions.get('max_pe', 25) * 1.5:
                score += 10
                reasons.append(f"PE={pe:.1f}偏高")
            else:
                valid = False
                reasons.append(f"PE={pe:.1f}过高")
        else:
            valid = False
            reasons.append("PE无效")
        
        # ROE 评分 (越高越好)
        roe = fundamental.get('roe')
        if roe and roe > 0:
            if roe >= conditions.get('min_roe', 10):
                score += 25
                reasons.append(f"ROE={roe:.1f}%")
            elif roe >= conditions.get('min_roe', 10) * 0.7:
                score += 15
                reasons.append(f"ROE={roe:.1f}%一般")
            else:
                score += 5
        else:
            reasons.append("ROE无效")
        
        # 股息率评分
        dv = fundamental.get('dv_ratio')
        if dv and dv > 0:
            if dv >= conditions.get('min_dv_ratio', 1):
                score += 20
                reasons.append(f"股息率={dv:.2f}%")
            elif dv >= conditions.get('min_dv_ratio', 1) * 0.5:
                score += 10
        else:
            reasons.append("无股息")
        
        # 资产负债率评分 (越低越好)
        debt = fundamental.get('debt_to_assets')
        if debt is not None:
            if debt <= conditions.get('max_debt', 70):
                score += 15
                reasons.append(f"负债率={debt:.1f}%")
            else:
                score -= 10
                reasons.append(f"负债率{debt:.1f}%偏高")
        
        # 市值评分 (越大越好)
        mc = fundamental.get('market_cap')
        if mc and mc > 0:
            if mc >= conditions.get('min_market_cap', 30):
                score += 15
            elif mc >= 10:  # 10亿以上
                score += 5
        
        # 综合判断
        if score >= 60 and valid:
            action = "buy"
            strength = min(score, 100)
        elif score >= 40:
            action = "hold"
            strength = score
        else:
            action = "sell"
            strength = max(50 - score, 0)
        
        return {
            'valid': valid,
            'action': action,
            'score': score,
            'strength': strength,
            'reasons': reasons,
            'fundamental': fundamental
        }
    
    def generate(self, ts_code: str, conditions: Dict = None) -> Optional[Signal]:
        """
        生成基本面信号
        
        Args:
            ts_code: 股票代码
            conditions: 筛选条件
        
        Returns:
            Signal 对象
        """
        result = self.get_fundamental_scores(ts_code, conditions)
        
        if result.get('fundamental') is None:
            return None
        
        fundamental = result['fundamental']
        
        return Signal(
            code=ts_code,
            name=fundamental.get('name', ''),
            action=result['action'],
            price=fundamental.get('close', 0),
            strength=result['strength'],
            reason="; ".join(result['reasons']),
            timestamp=datetime.now().isoformat(),
            indicators={
                'pe': fundamental.get('pe'),
                'roe': fundamental.get('roe'),
                'dv_ratio': fundamental.get('dv_ratio'),
                'debt_to_assets': fundamental.get('debt_to_assets'),
                'market_cap': fundamental.get('market_cap')
            }
        )


class HybridSignalGenerator:
    """
    混合信号生成器
    结合基本面 + 技术面
    """
    
    def __init__(self, db_path: str = "data/stocks.db"):
        self.db_path = db_path
        self.fundamental_gen = FundamentalSignalGenerator(db_path)
        
        # 导入技术面信号生成器
        from ..signals.generator import CompositeSignalGenerator
        self.technical_gen = CompositeSignalGenerator()
    
    def get_stock_price_data(self, ts_code: str, days: int = 60) -> Optional[pd.DataFrame]:
        """获取价格数据"""
        conn = sqlite3.connect(self.db_path)
        
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 30)
        
        try:
            df = pd.read_sql(f"""
                SELECT trade_date, open, high, low, close, vol
                FROM daily
                WHERE ts_code = '{ts_code}'
                  AND trade_date <= '{end_date.strftime('%Y%m%d')}'
                  AND trade_date >= '{start_date.strftime('%Y%m%d')}'
                ORDER BY trade_date ASC
            """, conn)
            
            if df is not None and len(df) >= 20:
                # 转换列名（首字母大写）
                rename_map = {
                    'open': 'Open', 'high': 'High', 'low': 'Low',
                    'close': 'Close', 'vol': 'Volume'
                }
                df = df.rename(columns=rename_map)
                return df
                
        except Exception as e:
            logger.error(f"获取价格数据失败 {ts_code}: {e}")
        finally:
            conn.close()
        
        return None
    
    def generate(self, ts_code: str, 
                 fundamental_conditions: Dict = None,
                 hybrid_mode: str = "strict") -> Optional[Signal]:
        """
        生成混合信号
        
        Args:
            ts_code: 股票代码
            fundamental_conditions: 基本面筛选条件
            hybrid_mode: 
                - "strict": 基本面和技术面都必须通过
                - "loose": 任一条件通过即可
                - "fundamental_only": 只看基本面
        
        Returns:
            Signal 对象
        """
        # 基本面信号
        fundamental_result = self.fundamental_gen.get_fundamental_scores(ts_code, fundamental_conditions)
        
        if fundamental_result.get('fundamental') is None:
            return None
        
        fundamental = fundamental_result['fundamental']
        fund_action = fundamental_result['action']
        fund_strength = fundamental_result['score']
        
        # 技术面信号
        tech_action = "hold"
        tech_strength = 50
        tech_reason = "数据不足"
        
        price_data = self.get_stock_price_data(ts_code)
        if price_data is not None and len(price_data) >= 20:
            tech_signal = self.technical_gen.generate(price_data, ts_code, fundamental.get('name', ''))
            if tech_signal:
                tech_action = tech_signal.action
                tech_strength = tech_signal.strength
                tech_reason = tech_signal.reason
        
        # 混合决策
        if hybrid_mode == "fundamental_only":
            final_action = fund_action
            final_strength = fund_strength
            reason = f"基本面: {'; '.join(fundamental_result['reasons'])}"
        
        elif hybrid_mode == "strict":
            # 必须基本面和技术面都看好
            if fund_action == "buy" and tech_action == "buy":
                final_action = "buy"
                final_strength = min(fund_strength, tech_strength)
                reason = f"基本面OK + 技术面买入: {tech_reason}"
            elif fund_action == "buy" and tech_action == "hold":
                final_action = "hold"
                final_strength = fund_strength
                reason = f"基本面OK, 技术面观望: {tech_reason}"
            else:
                final_action = "sell"
                final_strength = max(fund_strength - 30, 0)
                reason = f"基本面一般, 技术面看跌: {tech_reason}"
        
        elif hybrid_mode == "loose":
            # 任一条件通过即可
            if fund_action == "buy" or tech_action == "buy":
                final_action = "buy"
                final_strength = max(fund_strength, tech_strength)
                reason = f"基本面:{fund_action} 或 技术面:{tech_action}"
            else:
                final_action = "hold"
                final_strength = min(fund_strength, tech_strength)
                reason = "观望"
        
        else:
            final_action = fund_action
            final_strength = fund_strength
            reason = "; ".join(fundamental_result['reasons'])
        
        return Signal(
            code=ts_code,
            name=fundamental.get('name', ''),
            action=final_action,
            price=fundamental.get('close', 0),
            strength=final_strength,
            reason=reason,
            timestamp=datetime.now().isoformat(),
            indicators={
                'pe': fundamental.get('pe'),
                'roe': fundamental.get('roe'),
                'dv_ratio': fundamental.get('dv_ratio'),
                'debt_to_assets': fundamental.get('debt_to_assets'),
                'market_cap': fundamental.get('market_cap'),
                'tech_action': tech_action,
                'tech_strength': tech_strength
            }
        )


def test():
    """测试"""
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from src.config import config
    
    generator = HybridSignalGenerator()
    
    # 从数据库获取符合基本面条件的测试股票
    import sqlite3
    conn = sqlite3.connect('data/stocks.db')
    df = pd.read_sql(f"""
        SELECT ts_code FROM fundamentals
        WHERE pe > 0 AND pe <= {config.max_pe}
          AND roe >= {config.min_roe}
        LIMIT 5
    """, conn)
    conn.close()
    
    test_codes = df['ts_code'].tolist() if df is not None else ['000858.SZ', '600519.SH']
    
    for code in test_codes:
        print(f"\n{'='*50}")
        signal = generator.generate(code)
        
        if signal:
            print(f"{code} {signal.name}")
            print(f"  操作: {signal.action} (强度:{signal.strength})")
            print(f"  原因: {signal.reason}")
            print(f"  指标: PE={signal.indicators.get('pe')}, ROE={signal.indicators.get('roe')}")
            print(f"  技术: {signal.indicators.get('tech_action')}")


if __name__ == "__main__":
    test()
