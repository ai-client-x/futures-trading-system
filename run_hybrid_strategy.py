#!/usr/bin/env python3
"""
混合选股策略 - 独立运行版本
"""

import logging
import sqlite3
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HybridStrategy:
    """混合选股策略"""
    
    def __init__(self, db_path: str = "data/stocks.db"):
        self.db_path = db_path
        
        # 延迟导入
        from src.signals.fundamental_generator import HybridSignalGenerator
        self.signal_generator = HybridSignalGenerator(db_path)
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def screen_candidates(self, 
                         max_pe: float = 25,
                         min_roe: float = 10,
                         min_dv_ratio: float = 1,
                         max_debt: float = 70,
                         min_market_cap: float = 30,
                         limit: int = 100):
        """基本面初选"""
        conn = self.get_connection()
        
        query = f"""
            SELECT ts_code, name, close, pe, roe, dv_ratio, debt_to_assets, market_cap
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
    
    def get_buy_signals(self, 
                        max_pe: float = 25,
                        min_roe: float = 10,
                        min_dv_ratio: float = 1,
                        max_debt: float = 70,
                        min_market_cap: float = 30,
                        hybrid_mode: str = "strict"):
        """获取买入信号"""
        candidates = self.screen_candidates(
            max_pe=max_pe, min_roe=min_roe, min_dv_ratio=min_dv_ratio,
            max_debt=max_debt, min_market_cap=min_market_cap, limit=100
        )
        
        logger.info(f"基本面初选: {len(candidates)} 只")
        
        signals = []
        for candidate in candidates:
            ts_code = candidate['ts_code']
            
            signal = self.signal_generator.generate(
                ts_code, 
                fundamental_conditions={
                    'max_pe': max_pe, 'min_roe': min_roe,
                    'min_dv_ratio': min_dv_ratio, 'max_debt': max_debt,
                    'min_market_cap': min_market_cap
                },
                hybrid_mode=hybrid_mode
            )
            
            if signal and signal.action == "buy":
                signals.append(signal)
        
        signals.sort(key=lambda x: x.strength, reverse=True)
        logger.info(f"买入信号: {len(signals)} 只")
        
        return signals
    
    def print_signals(self, signals, title: str = "交易信号"):
        """打印信号"""
        print("\n" + "=" * 70)
        print(f"📈 {title} (共 {len(signals)} 只)")
        print("=" * 70)
        
        for i, signal in enumerate(signals, 1):
            ind = signal.indicators
            pe = ind.get('pe')
            roe = ind.get('roe')
            dv = ind.get('dv_ratio')
            print(f"\n{i}. {signal.code} {signal.name}")
            print(f"   价格: ¥{signal.price:.2f} | 操作: {signal.action} | 强度: {signal.strength}")
            pe_str = f"{pe:.1f}" if pe else "N/A"
            roe_str = f"{roe:.1f}" if roe else "N/A"
            dv_str = f"{dv:.2f}" if dv else "N/A"
            print(f"   估值: PE={pe_str}, ROE={roe_str}%, 股息={dv_str}%")
            print(f"   技术: {ind.get('tech_action', 'N/A')} (强度:{ind.get('tech_strength', 0)})")
            print(f"   原因: {signal.reason}")
        
        print("\n" + "=" * 70)


def main():
    import sys
    sys.path.insert(0, '.')
    
    strategy = HybridStrategy()
    
    # 获取买入信号
    signals = strategy.get_buy_signals(
        max_pe=25, min_roe=10, min_dv_ratio=1,
        max_debt=70, min_market_cap=30, hybrid_mode='strict'
    )
    
    strategy.print_signals(signals, '混合选股策略 - 买入信号')


if __name__ == "__main__":
    main()
