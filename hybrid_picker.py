#!/usr/bin/env python3
"""
混合选股系统
基本面选股 + 技术面验证
"""

import os
import sys
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.signals.generator import CompositeSignalGenerator
from src.data.fetcher import DataSource


class HybridStockPicker:
    """
    混合选股器
    基本面筛选 + 技术面验证
    """
    
    def __init__(self, db_path: str = "data/stocks.db"):
        self.db_path = db_path
        self.signal_generator = CompositeSignalGenerator()
        self.data_source = DataSource()
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def screen_by_fundamentals(self, 
                                max_pe: float = 20,
                                min_roe: float = 10,
                                min_dv_ratio: float = 0,
                                max_debt: float = 100,
                                min_market_cap: float = 0,
                                limit: int = 100) -> pd.DataFrame:
        """
        基本面筛选
        
        Args:
            max_pe: 最大市盈率
            min_roe: 最小净资产收益率(%)
            min_dv_ratio: 最小股息率(%)
            max_debt: 最大资产负债率(%)
            min_market_cap: 最小市值(亿)
            limit: 返回数量限制
        
        Returns:
            符合条件的股票DataFrame
        """
        conn = self.get_connection()
        
        query = f"""
            SELECT 
                ts_code, name, close, pe, pb, dv_ratio, 
                total_mv, circ_mv, market_cap,
                roe, netprofit_margin, grossprofit_margin,
                debt_to_assets, current_ratio
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
        
        return df
    
    def get_stock_price_data(self, ts_code: str, days: int = 60) -> Optional[pd.DataFrame]:
        """
        获取股票价格数据（从本地数据库）
        
        Args:
            ts_code: 股票代码
            days: 获取天数
        
        Returns:
            K线数据DataFrame
        """
        conn = self.get_connection()
        
        # 计算日期范围
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 30)  # 多取一些数据
        
        try:
            df = pd.read_sql(f"""
                SELECT 
                    trade_date, open, high, low, close, 
                    pre_close, change, pct_chg, vol, amount
                FROM daily
                WHERE ts_code = '{ts_code}'
                  AND trade_date <= '{end_date.strftime('%Y%m%d')}'
                  AND trade_date >= '{start_date.strftime('%Y%m%d')}'
                ORDER BY trade_date ASC
            """, conn)
            
            conn.close()
            
            if df is not None and len(df) >= 20:
                # 转换列名以匹配信号生成器（首字母大写）
                rename_map = {
                    'open': 'Open', 'high': 'High', 'low': 'Low', 
                    'close': 'Close', 'vol': 'Volume', 'amount': 'Amount'
                }
                df = df.rename(columns=rename_map)
                return df
                
        except Exception as e:
            print(f"获取数据失败 {ts_code}: {e}")
        
        if conn:
            conn.close()
        
        return None
    
    def verify_by_technical(self, ts_code: str) -> Dict:
        """
        技术面验证
        
        Args:
            ts_code: 股票代码
        
        Returns:
            技术分析结果
        """
        df = self.get_stock_price_data(ts_code)
        
        if df is None or len(df) < 20:
            return {
                'valid': False,
                'reason': '数据不足',
                'signal': None
            }
        
        # 生成技术信号
        signal = self.signal_generator.generate(df, ts_code, "")
        
        if signal is None:
            return {
                'valid': False,
                'reason': '无法生成信号',
                'signal': None
            }
        
        # 判断是否适合买入
        valid = False
        reason = ""
        
        if signal.action == "buy":
            valid = True
            reason = f"买入信号 (强度:{signal.strength})"
        elif signal.action == "hold" and signal.strength >= 40:
            # 轻微买入信号也可以考虑
            if "金叉" in signal.reason or "多头" in signal.reason:
                valid = True
                reason = f"持有信号 (强度:{signal.strength}, {signal.reason})"
            else:
                reason = f"观望 (强度:{signal.strength})"
        else:
            reason = f"卖出信号 ({signal.reason})"
        
        return {
            'valid': valid,
            'reason': reason,
            'signal': signal,
            'indicators': signal.indicators if signal else {}
        }
    
    def pick_stocks(self, 
                   fundamental_conditions: Dict = None,
                   technical_verify: bool = True,
                   top_n: int = 30) -> List[Dict]:
        """
        混合选股
        
        Args:
            fundamental_conditions: 基本面筛选条件
            technical_verify: 是否进行技术面验证
            top_n: 最终返回数量
        
        Returns:
            选股结果列表
        """
        # 默认基本面条件
        if fundamental_conditions is None:
            fundamental_conditions = {
                'max_pe': 20,
                'min_roe': 10,
                'min_dv_ratio': 1,  # 股息率 > 1%
                'max_debt': 70,
                'min_market_cap': 50,  # 市值 > 50亿
                'limit': 100
            }
        
        print("=" * 60)
        print("📊 混合选股系统")
        print("=" * 60)
        print(f"基本面条件: PE<{fundamental_conditions.get('max_pe')}, ROE>{fundamental_conditions.get('min_roe')}%, 股息>{fundamental_conditions.get('min_dv_ratio')}%, 负债<{fundamental_conditions.get('max_debt')}%, 市值>{fundamental_conditions.get('min_market_cap')}亿")
        print("=" * 60)
        
        # 第一步：基本面筛选
        print("\n🔍 第一步：基本面筛选...")
        df = self.screen_by_fundamentals(**fundamental_conditions)
        print(f"   符合基本面条件: {len(df)} 只")
        
        if len(df) == 0:
            print("   没有符合基本面条件的股票")
            return []
        
        # 第二步：技术面验证
        results = []
        
        if technical_verify:
            print("\n🔍 第二步：技术面验证...")
            for i, row in df.iterrows():
                ts_code = row['ts_code']
                name = row['name']
                
                tech_result = self.verify_by_technical(ts_code)
                
                result = {
                    'ts_code': ts_code,
                    'name': name,
                    'close': row['close'],
                    'pe': row['pe'],
                    'roe': row['roe'],
                    'dv_ratio': row['dv_ratio'],
                    'market_cap': row['market_cap'],
                    'debt_to_assets': row['debt_to_assets'],
                    'technical_valid': tech_result['valid'],
                    'technical_reason': tech_result['reason'],
                    'indicators': tech_result.get('indicators', {})
                }
                
                results.append(result)
                
                if (i + 1) % 10 == 0:
                    print(f"   已验证: {i+1}/{len(df)}")
            
            # 过滤出技术面也通过的
            valid_results = [r for r in results if r['technical_valid']]
            print(f"   技术面通过: {len(valid_results)} / {len(results)}")
            
            # 按基本面得分排序
            valid_results.sort(key=lambda x: (x['roe'] or 0) + (x['dv_ratio'] or 0) * 2, reverse=True)
        else:
            # 只返回基本面筛选结果
            results = df.to_dict('records')
        
        # 返回前N只
        final_results = results[:top_n]
        
        print("\n" + "=" * 60)
        print(f"📈 最终选股结果 (TOP {len(final_results)})")
        print("=" * 60)
        
        for i, r in enumerate(final_results, 1):
            print(f"\n{i}. {r['ts_code']} {r['name']}")
            print(f"   价格: ¥{r['close']:.2f} | PE: {r['pe']:.1f} | ROE: {r['roe']:.1f}%")
            print(f"   股息: {r['dv_ratio']:.2f}% | 市值: {r['market_cap']:.0f}亿 | 负债: {r['debt_to_assets']:.1f}%")
            if technical_verify:
                print(f"   ✅ 技术面: {r['technical_reason']}")
                if r.get('indicators'):
                    ind = r['indicators']
                    print(f"   📊 指标: RSI={ind.get('rsi', 'N/A')}, MACD={ind.get('macd', 'N/A')}")
        
        print("\n" + "=" * 60)
        
        return final_results
    
    def save_results(self, results: List[Dict], filepath: str = None):
        """保存选股结果"""
        if filepath is None:
            filepath = f"data/selected_stocks_{datetime.now().strftime('%Y%m%d')}.csv"
        
        df = pd.DataFrame(results)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"\n💾 选股结果已保存到: {filepath}")


def main():
    """测试"""
    picker = HybridStockPicker()
    
    # 选股条件
    conditions = {
        'max_pe': 25,          # PE < 25
        'min_roe': 10,        # ROE > 10%
        'min_dv_ratio': 1,    # 股息率 > 1%
        'max_debt': 70,       # 资产负债率 < 70%
        'min_market_cap': 30, # 市值 > 30亿
        'limit': 50
    }
    
    # 执行选股
    results = picker.pick_stocks(
        fundamental_conditions=conditions,
        technical_verify=True,
        top_n=20
    )
    
    # 保存结果
    if results:
        picker.save_results(results)


if __name__ == "__main__":
    main()
