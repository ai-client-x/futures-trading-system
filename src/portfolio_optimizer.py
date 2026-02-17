#!/usr/bin/env python3
"""
组合优化模块
Portfolio Optimization
"""

import logging
from typing import List, Dict, Tuple
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class PortfolioOptimizer:
    """
    组合优化器
    基于 Markowitz 模型优化仓位分配
    """
    
    def __init__(self, db_path: str = "data/stocks.db"):
        self.db_path = db_path
    
    def _get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def get_stock_returns(self, ts_codes: List[str], days: int = 252) -> pd.DataFrame:
        """
        获取股票收益率数据
        
        Args:
            ts_codes: 股票代码列表
            days: 历史天数
        
        Returns:
            日收益率 DataFrame
        """
        conn = self._get_connection()
        
        # 计算日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 30)
        
        # 批量查询
        placeholders = ','.join([f"'{c}'" for c in ts_codes])
        
        query = f"""
            SELECT ts_code, trade_date, close
            FROM daily
            WHERE ts_code IN ({placeholders})
              AND trade_date >= '{start_date.strftime('%Y%m%d')}'
            ORDER BY ts_code, trade_date
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df is None or len(df) == 0:
            return pd.DataFrame()
        
        # 转换为收益率
        df = df.pivot(index='trade_date', columns='ts_code', values='close')
        returns = df.pct_change().dropna()
        
        return returns
    
    def calculate_metrics(self, returns: pd.DataFrame) -> Dict:
        """
        计算组合指标
        
        Args:
            returns: 收益率 DataFrame
        
        Returns:
            指标字典
        """
        if len(returns) == 0:
            return {}
        
        # 年化收益率
        mean_return = returns.mean() * 252
        
        # 年化波动率
        volatility = returns.std() * np.sqrt(252)
        
        # 相关性矩阵
        correlation = returns.corr()
        
        # 协方差矩阵
        covariance = returns.cov() * 252
        
        return {
            'mean_return': mean_return.to_dict(),
            'volatility': volatility.to_dict(),
            'correlation': correlation.to_dict(),
            'covariance': covariance.to_dict()
        }
    
    def optimize_equal_weight(self, signals: List, capital: float) -> List[Dict]:
        """
        等权分配
        
        Args:
            signals: 选股信号列表
            capital: 总资金
        
        Returns:
            仓位分配列表
        """
        n = len(signals)
        if n == 0:
            return []
        
        weight = 1.0 / n
        positions = []
        
        for signal in signals:
            positions.append({
                'code': signal.code,
                'name': signal.name,
                'weight': weight,
                'amount': capital * weight / signal.price,
                'value': capital * weight,
                'signal_strength': signal.strength
            })
        
        return positions
    
    def optimize_risk_parity(self, returns: pd.DataFrame, signals: List, capital: float) -> List[Dict]:
        """
        风险平价策略
        每只股票贡献相同的风险
        
        Args:
            returns: 收益率数据
            signals: 选股信号
            capital: 总资金
        
        Returns:
            仓位分配
        """
        if len(returns) == 0 or len(signals) == 0:
            return self.optimize_equal_weight(signals, capital)
        
        # 获取有数据的股票
        valid_codes = [s.code for s in signals if s.code in returns.columns]
        
        if len(valid_codes) == 0:
            return self.optimize_equal_weight(signals, capital)
        
        # 计算波动率
        vol = returns[valid_codes].std() * np.sqrt(252)
        
        # 风险平价权重 = 1/波动率
        inv_vol = 1 / vol
        weights = inv_vol / inv_vol.sum()
        
        # 构建结果
        positions = []
        code_weights = weights.to_dict()
        
        for signal in signals:
            code = signal.code
            weight = code_weights.get(code, 0)
            
            positions.append({
                'code': code,
                'name': signal.name,
                'weight': weight,
                'amount': int(capital * weight / signal.price / 100) * 100,  # 整手
                'value': capital * weight,
                'signal_strength': signal.strength,
                'volatility': vol.get(code, 0) * 100
            })
        
        return positions
    
    def optimize_momentum_weighted(self, signals: List, capital: float, 
                                  returns: pd.DataFrame = None, lookback: int = 60) -> List[Dict]:
        """
        动量加权
        近期表现好的多配
        
        Args:
            signals: 选股信号
            capital: 总资金
            returns: 收益率数据
            lookback: 回看天数
        
        Returns:
            仓位分配
        """
        if not signals:
            return []
        
        # 如果没有收益率数据，等权
        if returns is None or len(returns) == 0:
            return self.optimize_equal_weight(signals, capital)
        
        # 获取近期收益
        recent = returns.tail(lookback)
        
        # 计算动量分数 (累计收益)
        momentum = (1 + recent).prod() - 1
        
        # 只考虑有信号的股票
        valid_codes = [s.code for s in signals if s.code in momentum.index]
        
        if not valid_codes:
            return self.optimize_weighted_by_signal(signals, capital)
        
        # 动量加权
        mom = momentum[valid_codes]
        # 过滤负收益
        mom = mom.clip(lower=0.001)
        weights = mom / mom.sum()
        
        positions = []
        code_weights = weights.to_dict()
        
        for signal in signals:
            code = signal.code
            weight = code_weights.get(code, 0)
            
            positions.append({
                'code': code,
                'name': signal.name,
                'weight': weight,
                'amount': int(capital * weight / signal.price / 100) * 100,
                'value': capital * weight,
                'signal_strength': signal.strength,
                'momentum': momentum.get(code, 0) * 100
            })
        
        return positions
    
    def optimize_weighted_by_signal(self, signals: List, capital: float) -> List[Dict]:
        """
        按信号强度加权
        
        Args:
            signals: 选股信号
            capital: 总资金
        
        Returns:
            仓位分配
        """
        if not signals:
            return []
        
        # 信号强度作为权重
        strengths = np.array([s.strength for s in signals])
        weights = strengths / strengths.sum()
        
        positions = []
        
        for i, signal in enumerate(signals):
            positions.append({
                'code': signal.code,
                'name': signal.name,
                'weight': weights[i],
                'amount': int(capital * weights[i] / signal.price / 100) * 100,
                'value': capital * weights[i],
                'signal_strength': signal.strength
            })
        
        return positions
    
    def optimize_minimum_variance(self, returns: pd.DataFrame, signals: List, 
                                  capital: float, risk_aversion: float = 1.0) -> List[Dict]:
        """
        最小方差组合
        
        使用 Markowitz 均值-方差优化
        
        Args:
            returns: 收益率数据
            signals: 选股信号
            capital: 总资金
            risk_aversion: 风险厌恶系数
        
        Returns:
            仓位分配
        """
        if len(returns) == 0 or len(signals) == 0:
            return self.optimize_equal_weight(signals, capital)
        
        valid_codes = [s.code for s in signals if s.code in returns.columns]
        
        if len(valid_codes) < 2:
            return self.optimize_equal_weight(signals, capital)
        
        # 子集数据
        ret = returns[valid_codes]
        
        # 预期收益和协方差
        expected_returns = ret.mean() * 252
        cov_matrix = ret.cov() * 252
        
        n = len(valid_codes)
        
        try:
            # 简化优化：使用解析解
            # 最小方差权重 = (1/风险厌恶) * Cov^(-1) * 1
            inv_cov = np.linalg.inv(cov_matrix.values + np.eye(n) * 0.01)  # 加正则化
            ones = np.ones(n)
            weights = inv_cov @ ones
            weights = weights / weights.sum()
            
            # 确保非负
            weights = np.maximum(weights, 0)
            weights = weights / weights.sum()
            
        except:
            # 如果优化失败，使用等权
            weights = np.ones(n) / n
        
        # 构建结果
        positions = []
        for i, code in enumerate(valid_codes):
            signal = next((s for s in signals if s.code == code), None)
            if signal:
                positions.append({
                    'code': code,
                    'name': signal.name,
                    'weight': weights[i],
                    'amount': int(capital * weights[i] / signal.price / 100) * 100,
                    'value': capital * weights[i],
                    'signal_strength': signal.strength
                })
        
        return positions
    
    def optimize_max_sharpe(self, returns: pd.DataFrame, signals: List, 
                           capital: float, risk_free_rate: float = 0.03) -> List[Dict]:
        """
        最大夏普比率组合
        
        Args:
            returns: 收益率数据
            signals: 选股信号
            capital: 总资金
            risk_free_rate: 无风险利率
        
        Returns:
            仓位分配
        """
        if len(returns) == 0 or len(signals) == 0:
            return self.optimize_equal_weight(signals, capital)
        
        valid_codes = [s.code for s in signals if s.code in returns.columns]
        
        if len(valid_codes) < 2:
            return self.optimize_equal_weight(signals, capital)
        
        ret = returns[valid_codes]
        
        expected_returns = ret.mean() * 252 - risk_free_rate
        cov_matrix = ret.cov() * 252
        
        n = len(valid_codes)
        
        try:
            # 夏普比率最大化 (简化版)
            # 使用逆协方差加权
            inv_cov = np.linalg.inv(cov_matrix.values + np.eye(n) * 0.01)
            weights = inv_cov @ expected_returns.values
            weights = np.maximum(weights, 0)
            
            if weights.sum() > 0:
                weights = weights / weights.sum()
            else:
                weights = np.ones(n) / n
                
        except:
            weights = np.ones(n) / n
        
        positions = []
        for i, code in enumerate(valid_codes):
            signal = next((s for s in signals if s.code == code), None)
            if signal:
                positions.append({
                    'code': code,
                    'name': signal.name,
                    'weight': weights[i],
                    'amount': int(capital * weights[i] / signal.price / 100) * 100,
                    'value': capital * weights[i],
                    'signal_strength': signal.strength
                })
        
        return positions
    
    def optimize(self, signals: List, capital: float, 
                method: str = "signal_strength",
                returns: pd.DataFrame = None) -> List[Dict]:
        """
        组合优化主方法
        
        Args:
            signals: 选股信号列表
            capital: 总资金
            method: 优化方法
                - "equal": 等权分配
                - "risk_parity": 风险平价
                - "momentum": 动量加权
                - "min_variance": 最小方差
                - "max_sharpe": 最大夏普
                - "signal_strength": 按信号强度
        
        Returns:
            仓位分配列表
        """
        if not signals:
            logger.warning("没有选股信号")
            return []
        
        if method == "equal":
            return self.optimize_equal_weight(signals, capital)
        
        elif method == "risk_parity":
            return self.optimize_risk_parity(returns, signals, capital)
        
        elif method == "momentum":
            return self.optimize_momentum_weighted(signals, capital, returns)
        
        elif method == "min_variance":
            return self.optimize_minimum_variance(returns, signals, capital)
        
        elif method == "max_sharpe":
            return self.optimize_max_sharpe(returns, signals, capital)
        
        elif method == "signal_strength":
            return self.optimize_weighted_by_signal(signals, capital)
        
        else:
            return self.optimize_equal_weight(signals, capital)
    
    def print_allocation(self, positions: List[Dict], title: str = "仓位分配"):
        """打印仓位分配"""
        if not positions:
            print("无仓位分配")
            return
        
        total_value = sum(p['value'] for p in positions)
        
        print("\n" + "=" * 70)
        print(f"📊 {title}")
        print("=" * 70)
        print(f"{'代码':<12} {'名称':<10} {'权重':<10} {'金额':<12} {'数量':<8} {'备注'}")
        print("-" * 70)
        
        for p in positions:
            code = p['code']
            name = p['name']
            weight = p['weight'] * 100
            value = p['value']
            amount = p.get('amount', 0)
            
            # 备注
            notes = []
            if 'signal_strength' in p:
                notes.append(f"强度:{p['signal_strength']}")
            if 'volatility' in p:
                notes.append(f"波动:{p['volatility']:.1f}%")
            if 'momentum' in p:
                notes.append(f"动量:{p['momentum']:.1f}%")
            
            note_str = ", ".join(notes) if notes else "-"
            
            print(f"{code:<12} {name:<8} {weight:>6.1f}% ¥{value:>10,.0f} {amount:>6} {note_str}")
        
        print("-" * 70)
        print(f"{'合计':<12} {'':<10} {'100.0%':<10} ¥{total_value:>10,.0f}")
        print("=" * 70)


def test():
    """测试"""
    import sys
    sys.path.insert(0, '.')
    
    from run_hybrid_strategy import HybridStrategy
    
    # 获取选股信号
    strategy = HybridStrategy()
    signals = strategy.get_buy_signals(
        max_pe=25, min_roe=10, min_dv_ratio=1,
        max_debt=70, min_market_cap=30, hybrid_mode='strict'
    )
    
    if not signals:
        print("无买入信号")
        return
    
    # 获取收益率数据
    optimizer = PortfolioOptimizer()
    codes = [s.code for s in signals]
    returns = optimizer.get_stock_returns(codes, days=120)
    
    capital = 1000000  # 100万
    
    # 测试不同优化方法
    methods = ["equal", "signal_strength", "risk_parity", "momentum"]
    
    for method in methods:
        positions = optimizer.optimize(signals, capital, method, returns)
        optimizer.print_allocation(positions, f"{method} 优化")


if __name__ == "__main__":
    test()
