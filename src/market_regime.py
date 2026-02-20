#!/usr/bin/env python3
"""
市场状态判断模块 V2
Market Regime Detection V2
增强版：多指标综合判断，提高准确率

判断方法：
1. 趋势判断：均线排列 + MACD + 趋势强度 (ADX)
2. 动量判断：RSI + 价格动量
3. 波动判断：布林带宽度 + 波动率
4. 成交量判断：量价配合
"""

import logging
from typing import Optional, Dict, List
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class MarketRegimeDetectorV2:
    """
    增强版市场状态检测器
    
    综合多指标判断：
    - 趋势指标：均线、MACD、ADX
    - 动量指标：RSI、价格动量
    - 波动指标：布林带宽度、ATR
    - 成交量指标：量价关系
    """
    
    def __init__(self):
        # 均线周期配置
        self.ma_short = 20    # 短期均线
        self.ma_medium = 60   # 中期均线  
        self.ma_long = 120   # 长期均线
        
        # 趋势判断参数
        self.trend_period = 20
        self.volatility_period = 20
        
        # 分数阈值
        self.bull_threshold = 0.3    # 牛市分数阈值
        self.bear_threshold = -0.3   # 熊市分数阈值
    
    def _calculate_ma(self, close: pd.Series) -> pd.DataFrame:
        """计算均线"""
        return pd.DataFrame({
            'ma20': close.rolling(window=20).mean(),
            'ma60': close.rolling(window=60).mean(),
            'ma120': close.rolling(window=120).mean(),
        })
    
    def _calculate_rsi(self, close: pd.Series, period: int = 14) -> pd.Series:
        """计算RSI"""
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def _calculate_macd(self, close: pd.Series) -> pd.DataFrame:
        """计算MACD"""
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        histogram = macd - signal
        return pd.DataFrame({
            'macd': macd,
            'signal': signal,
            'histogram': histogram
        })
    
    def _calculate_adx(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """计算ADX (趋势强度指标)"""
        # 计算True Range
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # 计算Directional Movement
        plus_dm = high.diff()
        minus_dm = -low.diff()
        
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0
        
        # 计算平滑值
        tr_smooth = tr.rolling(window=period).mean()
        plus_dm_smooth = plus_dm.rolling(window=period).mean()
        minus_dm_smooth = minus_dm.rolling(window=period).mean()
        
        # 计算DI
        plus_di = 100 * plus_dm_smooth / tr_smooth
        minus_di = 100 * minus_dm_smooth / tr_smooth
        
        # 计算DX
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        
        # 计算ADX
        adx = dx.rolling(window=period).mean()
        
        return adx
    
    def _calculate_atr(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """计算ATR (平均真实波幅)"""
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()
    
    def _calculate_bollinger_width(self, close: pd.Series, period: int = 20) -> pd.Series:
        """计算布林带宽度"""
        middle = close.rolling(window=period).mean()
        std = close.rolling(window=period).std()
        upper = middle + 2 * std
        lower = middle - 2 * std
        return (upper - lower) / middle * 100
    
    def _calculate_volume_profile(self, close: pd.Series, volume: pd.Series) -> float:
        """计算量价配合度"""
        if len(close) < 20 or len(volume) < 20:
            return 0
        
        # 价格变化与成交量相关性
        price_change = close.pct_change()
        volume_change = volume.pct_change()
        
        corr = price_change.corr(volume_change)
        return corr if not pd.isna(corr) else 0
    
    def _score_ma_arrangement(self, ma: pd.DataFrame) -> tuple:
        """
        评分均线排列
        返回: (score, reason)
        score范围: -1 (完全空头) ~ 1 (完全多头)
        """
        latest = ma.iloc[-1]
        
        # 多头排列：MA20 > MA60 > MA120
        # 空头排列：MA20 < MA60 < MA120
        
        # 检查排列
        bull_arrangement = (latest['ma20'] > latest['ma60'] > latest['ma120'])
        bear_arrangement = (latest['ma20'] < latest['ma60'] < latest['ma120'])
        
        if bull_arrangement:
            # 计算偏离程度
            deviation = (latest['ma20'] - latest['ma120']) / latest['ma120'] * 100
            score = min(1.0, deviation / 10)  # 偏离10%得满分1.0
            return score, "均线多头排列"
        
        elif bear_arrangement:
            deviation = (latest['ma120'] - latest['ma20']) / latest['ma120'] * 100
            score = -min(1.0, deviation / 10)
            return score, "均线空头排列"
        
        else:
            # 均线纠缠
            # 检查短期均线位置
            if latest['ma20'] > latest['ma60']:
                score = 0.2
                return score, "短期均线偏多"
            elif latest['ma20'] < latest['ma60']:
                score = -0.2
                return score, "短期均线偏空"
            return 0, "均线纠缠"
    
    def _score_macd(self, macd_data: pd.DataFrame) -> tuple:
        """评分MACD"""
        if len(macd_data) < 2:
            return 0, "数据不足"
        
        latest = macd_data.iloc[-1]
        prev = macd_data.iloc[-2]
        
        # 金叉/死叉判断
        if latest['histogram'] > 0 and prev['histogram'] <= 0:
            return 0.5, "MACD金叉"
        elif latest['histogram'] < 0 and prev['histogram'] >= 0:
            return -0.5, "MACD死叉"
        
        # 零轴位置
        if latest['macd'] > 0 and latest['signal'] > 0:
            return 0.3, "MACD零轴上方"
        elif latest['macd'] < 0 and latest['signal'] < 0:
            return -0.3, "MACD零轴下方"
        
        return 0, "MACD中性"
    
    def _score_rsi(self, rsi: pd.Series) -> tuple:
        """评分RSI"""
        latest = rsi.iloc[-1]
        
        if pd.isna(latest):
            return 0, "RSI数据不足"
        
        # 超买超卖
        if latest < 30:
            return 0.3, f"RSI超卖({latest:.1f})"
        elif latest > 70:
            return -0.3, f"RSI超买({latest:.1f})"
        
        # 趋势判断
        if latest > 55:
            return 0.2, f"RSI偏强({latest:.1f})"
        elif latest < 45:
            return -0.2, f"RSI偏弱({latest:.1f})"
        
        return 0, f"RSI中性({latest:.1f})"
    
    def _score_price_trend(self, close: pd.Series) -> tuple:
        """评分价格趋势"""
        # 多周期趋势
        changes = {}
        for period in [5, 10, 20, 60]:
            if len(close) >= period + 1:
                changes[period] = (close.iloc[-1] - close.iloc[-period]) / close.iloc[-period] * 100
        
        if not changes:
            return 0, "数据不足"
        
        # 20日趋势（主要）
        change_20d = changes.get(20, 0)
        
        if change_20d > 10:
            score = min(1.0, change_20d / 15)
            return score, f"20日涨幅{change_20d:.1f}%"
        elif change_20d < -10:
            score = max(-1.0, change_20d / 15)
            return score, f"20日跌幅{abs(change_20d):.1f}%"
        
        # 检查多周期一致性
        positive_count = sum(1 for c in changes.values() if c > 0)
        negative_count = sum(1 for c in changes.values() if c < 0)
        
        if positive_count >= 3:
            return 0.4, "多周期上涨一致"
        elif negative_count >= 3:
            return -0.4, "多周期下跌一致"
        
        return 0, f"20日波动{change_20d:.1f}%"
    
    def _score_adx(self, adx: pd.Series) -> tuple:
        """评分ADX（趋势强度）"""
        latest = adx.iloc[-1]
        
        if pd.isna(latest):
            return 0, "ADX数据不足"
        
        if latest > 25:
            # 强趋势
            return 0.2, f"强趋势(ADX={latest:.1f})"
        elif latest < 20:
            return -0.1, f"弱趋势(ADX={latest:.1f})"
        
        return 0, f"中性趋势(ADX={latest:.1f})"
    
    def _score_volatility(self, bw: pd.Series, atr: pd.Series, close: pd.Series) -> tuple:
        """评分波动性"""
        latest_bw = bw.iloc[-1]
        
        if pd.isna(latest_bw):
            return 0, "波动数据不足"
        
        # 布林带宽度判断趋势/震荡
        if latest_bw > 10:
            return 0.1, f"高波动({latest_bw:.1f}%)"
        elif latest_bw < 5:
            return -0.1, f"低波动({latest_bw:.1f}%)"
        
        return 0, f"中等波动({latest_bw:.1f}%)"
    
    def detect(self, df: pd.DataFrame, index_code: str = "000300") -> Dict:
        """
        检测市场状态
        
        Args:
            df: 指数数据 (必须包含 OHLCV)
            index_code: 指数代码
        
        Returns:
            {
                'regime': 'bull' | 'bear' | 'consolidation',
                'confidence': 0-100,
                'indicators': {...},
                'scores': {...}
            }
        """
        if df is None or len(df) < self.ma_long + 10:
            logger.warning(f"数据不足，需要{self.ma_long + 10}天数据")
            return self._default_result()
        
        try:
            close = df['Close']
            high = df.get('High', close)
            low = df.get('Low', close)
            volume = df.get('Volume', pd.Series([1] * len(close)))
            
            # 计算各项指标
            ma = self._calculate_ma(close)
            rsi = self._calculate_rsi(close)
            macd_data = self._calculate_macd(close)
            adx = self._calculate_adx(high, low, close)
            bw = self._calculate_bollinger_width(close)
            atr = self._calculate_atr(high, low, close)
            
            # 收集各项评分
            scores = {}
            reasons = []
            
            # 1. 均线排列评分
            ma_score, ma_reason = self._score_ma_arrangement(ma)
            scores['ma'] = ma_score
            reasons.append(ma_reason)
            
            # 2. MACD评分
            macd_score, macd_reason = self._score_macd(macd_data)
            scores['macd'] = macd_score
            reasons.append(macd_reason)
            
            # 3. RSI评分
            rsi_score, rsi_reason = self._score_rsi(rsi)
            scores['rsi'] = rsi_score
            reasons.append(rsi_reason)
            
            # 4. 价格趋势评分
            trend_score, trend_reason = self._score_price_trend(close)
            scores['trend'] = trend_score
            reasons.append(trend_reason)
            
            # 5. ADX评分
            adx_score, adx_reason = self._score_adx(adx)
            scores['adx'] = adx_score
            reasons.append(adx_reason)
            
            # 6. 波动性评分
            vol_score, vol_reason = self._score_volatility(bw, atr, close)
            scores['volatility'] = vol_score
            reasons.append(vol_reason)
            
            # 计算综合分数（加权平均）
            weights = {
                'ma': 0.25,      # 均线权重最高
                'macd': 0.20,    # MACD趋势确认
                'rsi': 0.15,     # RSI动量
                'trend': 0.25,   # 价格趋势
                'adx': 0.10,     # ADX趋势强度
                'volatility': 0.05  # 波动性参考
            }
            
            total_score = sum(scores[k] * weights[k] for k in weights)
            
            # 收集详细指标
            indicators = {
                'price': round(close.iloc[-1], 2),
                'price_change_20d': round((close.iloc[-1] - close.iloc[-20]) / close.iloc[-20] * 100, 2) if len(close) >= 21 else 0,
                'ma20': round(ma['ma20'].iloc[-1], 2),
                'ma60': round(ma['ma60'].iloc[-1], 2),
                'ma120': round(ma['ma120'].iloc[-1], 2),
                'rsi': round(rsi.iloc[-1], 2) if not pd.isna(rsi.iloc[-1]) else None,
                'macd': round(macd_data['macd'].iloc[-1], 2),
                'adx': round(adx.iloc[-1], 2) if not pd.isna(adx.iloc[-1]) else None,
                'bb_width': round(bw.iloc[-1], 2) if not pd.isna(bw.iloc[-1]) else None,
                'price_vs_ma20': round((close.iloc[-1] - ma['ma20'].iloc[-1]) / ma['ma20'].iloc[-1] * 100, 2),
            }
            
            # 判断市场状态
            if total_score >= self.bull_threshold:
                # 牛市
                confidence = min(100, 50 + total_score * 100)
                regime = 'bull'
                regime_name = '牛市'
                
            elif total_score <= self.bear_threshold:
                # 熊市
                confidence = min(100, 50 + abs(total_score) * 100)
                regime = 'bear'
                regime_name = '熊市'
                
            else:
                # 震荡市
                # 震荡市的置信度与偏离零点的距离成反比
                confidence = max(30, 80 - abs(total_score) * 100)
                regime = 'consolidation'
                regime_name = '震荡市'
            
            return {
                'regime': regime,
                'regime_name': regime_name,
                'confidence': round(confidence, 1),
                'total_score': round(total_score, 3),
                'indicators': indicators,
                'scores': {k: round(v, 3) for k, v in scores.items()},
                'reason': "; ".join(reasons[:3])
            }
            
        except Exception as e:
            logger.error(f"市场状态判断错误: {e}")
            return self._default_result()
    
    def _default_result(self) -> Dict:
        """默认结果"""
        return {
            'regime': 'consolidation',
            'regime_name': '震荡市',
            'confidence': 50,
            'total_score': 0,
            'indicators': {},
            'scores': {},
            'reason': '数据不足，默认震荡市'
        }
    
    def get_strategy_for_regime(self, regime: str) -> str:
        """根据市场状态获取对应策略"""
        strategy_map = {
            'bull': 'williams',
            'bear': 'momentum_reversal',
            'consolidation': 'bollinger'
        }
        return strategy_map.get(regime, 'bollinger')
    
    def get_regime_name(self, regime: str) -> str:
        """获取市场状态中文名称"""
        names = {
            'bull': '牛市',
            'bear': '熊市',
            'consolidation': '震荡市'
        }
        return names.get(regime, '未知')


# 兼容旧版本
class MarketRegimeDetector(MarketRegimeDetectorV2):
    """兼容旧版本"""
    pass


def detect_market_regime(df: pd.DataFrame) -> Dict:
    """便捷函数：检测市场状态"""
    detector = MarketRegimeDetectorV2()
    return detector.detect(df)


if __name__ == "__main__":
    import math
    
    print("=" * 60)
    print("市场状态判断 V2 测试")
    print("=" * 60)
    
    # 测试1：模拟牛市数据
    print("\n📈 牛市测试")
    bull_data = pd.DataFrame({
        'Close': [100 + i * 0.8 for i in range(150)],
        'High': [100 + i * 0.8 + 5 for i in range(150)],
        'Low': [100 + i * 0.8 - 5 for i in range(150)],
        'Volume': [1000000] * 150
    })
    result = detect_market_regime(bull_data)
    print(f"状态: {result['regime_name']}, 置信度: {result['confidence']}%, 分数: {result['total_score']}")
    print(f"原因: {result['reason']}")
    print(f"指标分: {result['scores']}")
    
    # 测试2：模拟熊市数据
    print("\n📉 熊市测试")
    bear_data = pd.DataFrame({
        'Close': [100 - i * 0.8 for i in range(150)],
        'High': [100 - i * 0.8 + 5 for i in range(150)],
        'Low': [100 - i * 0.8 - 5 for i in range(150)],
        'Volume': [1000000] * 150
    })
    result = detect_market_regime(bear_data)
    print(f"状态: {result['regime_name']}, 置信度: {result['confidence']}%, 分数: {result['total_score']}")
    print(f"原因: {result['reason']}")
    print(f"指标分: {result['scores']}")
    
    # 测试3：模拟震荡市数据
    print("\n🔄 震荡市测试")
    consolidate_data = pd.DataFrame({
        'Close': [100 + math.sin(i/10) * 10 for i in range(150)],
        'High': [100 + math.sin(i/10) * 10 + 5 for i in range(150)],
        'Low': [100 + math.sin(i/10) * 10 - 5 for i in range(150)],
        'Volume': [1000000] * 150
    })
    result = detect_market_regime(consolidate_data)
    print(f"状态: {result['regime_name']}, 置信度: {result['confidence']}%, 分数: {result['total_score']}")
    print(f"原因: {result['reason']}")
    print(f"指标分: {result['scores']}")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
