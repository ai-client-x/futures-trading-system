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
from ..market_regime import MarketRegimeDetectorV2

logger = logging.getLogger(__name__)


class HybridStrategy:
    """
    混合选股策略
    基本面筛选 + 技术面择时 + 市场环境自适应
    """
    
    # 26策略擅长市场环境映射
    STRATEGY_MARKET_MAP = {
        '牛市': ['成交量突破', 'MACD+成交量', 'MACD策略', '突破前高', '均线发散', 
                '量价齐升', 'RSI趋势', '趋势过滤', '均线策略', '均线交叉强度',
                '收盘站均线', '成交量+均线', '突破确认', '平台突破'],
        '熊市': ['动量反转', '威廉指标', 'RSI逆势', '双底形态', '缩量回调', 'MACD背离'],
        '震荡市': ['布林带', 'RSI+均线', '布林带+RSI', '支撑阻力', '波动率突破', '均线收复']
    }
    
    def __init__(self, db_path: str = "data/stocks.db"):
        self.db_path = db_path
        self.signal_generator = HybridSignalGenerator(db_path)
        self.config = config
        self.regime_detector = MarketRegimeDetectorV2()
        self.current_regime = None
        self.selected_strategies = []
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def detect_market_regime(self, market_data: pd.DataFrame = None) -> str:
        """
        识别当前市场环境
        
        Returns:
            '牛市' | '熊市' | '震荡市'
        """
        if market_data is None:
            # 获取市场指数数据
            market_data = self._get_market_index()
        
        regime = self.regime_detector.detect(market_data)
        self.current_regime = regime
        
        # 选择适合当前市场的策略
        self.selected_strategies = self.STRATEGY_MARKET_MAP.get(regime, ['威廉指标', 'RSI逆势', '动量反转'])
        
        logger.info(f"市场环境: {regime}, 选用策略: {self.selected_strategies}")
        
        return regime
    
    def _get_market_index(self) -> pd.DataFrame:
        """获取市场指数数据"""
        conn = self.get_connection()
        # 用所有股票的平均价格作为市场指数
        df = pd.read_sql("""
            SELECT trade_date, AVG(close) as Close
            FROM daily
            WHERE trade_date >= date('now', '-60 days')
            GROUP BY trade_date
            ORDER BY trade_date
        """, conn)
        conn.close()
        return df
    
    def check_positions_for_regime_change(self, positions: List[Dict], current_data: pd.DataFrame) -> List[str]:
        """
        检查持仓是否需要卖出（市场环境改变）
        
        Args:
            positions: 当前持仓列表
            current_data: 当前市场数据
        
        Returns:
            需要卖出的股票代码列表
        """
        if not positions:
            return []
        
        # 检测新的市场环境
        new_regime = self.detect_market_regime(current_data)
        
        # 如果市场环境没变，不需要卖出
        if new_regime == self.current_regime:
            return []
        
        logger.warning(f"市场环境改变: {self.current_regime} -> {new_regime}")
        
        # 获取新环境不擅长的策略
        old_strategies = self.STRATEGY_MARKET_MAP.get(self.current_regime, [])
        new_strategies = self.STRATEGY_MARKET_MAP.get(new_regime, [])
        
        # 找出需要卖出的股票（持仓策略不擅长新环境）
        to_sell = []
        for pos in positions:
            pos_strategy = pos.get('strategy', '')
            if pos_strategy and pos_strategy not in new_strategies:
                to_sell.append(pos['ts_code'])
                logger.info(f"卖出 {pos['ts_code']}: 策略 {pos_strategy} 不擅长 {new_regime}")
        
        return to_sell
    
    def get_selected_strategies(self) -> List[str]:
        """获取当前选中的策略列表"""
        return self.selected_strategies
    
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
                        
                        # 只计算选中的策略信号强度
                        total_score = 0
                        count = 0
                        # 如果没有选中策略，使用默认6个
                        strategies_to_use = self.selected_strategies if self.selected_strategies else                             ['威廉指标', 'RSI逆势', '动量反转', '布林带', '成交量突破', 'MACD+成交量']
                        
                        for strat in strategies_to_use:
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
