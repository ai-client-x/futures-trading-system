#!/usr/bin/env python3
"""
信号生成模块
Signal Generator
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict
from abc import ABC, abstractmethod

import pandas as pd
import numpy as np

from ..models import Signal

logger = logging.getLogger(__name__)


class TechnicalIndicators:
    """技术指标计算"""
    
    @staticmethod
    def calculate_ma(series: pd.Series, period: int) -> pd.Series:
        """移动平均线"""
        return series.rolling(window=period).mean()
    
    @staticmethod
    def calculate_ema(series: pd.Series, period: int) -> pd.Series:
        """指数移动平均线"""
        return series.ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
        """RSI指标"""
        delta = series.diff()
        gain = delta.where(delta > 0, 0).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def calculate_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
        """MACD指标"""
        ema_fast = series.ewm(span=fast, adjust=False).mean()
        ema_slow = series.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram
    
    @staticmethod
    def calculate_bollinger_bands(series: pd.Series, period: int = 20, std_dev: float = 2):
        """布林带"""
        ma = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        upper = ma + (std * std_dev)
        lower = ma - (std * std_dev)
        return upper, ma, lower
    
    @staticmethod
    def calculate_volume_ma(series: pd.Series, period: int = 20) -> pd.Series:
        """成交量均线"""
        return series.rolling(window=period).mean()


class SignalGeneratorBase(ABC):
    """信号生成器基类"""
    
    def __init__(self, name: str = "SignalGenerator"):
        self.name = name
        self.indicators = TechnicalIndicators()
    
    @abstractmethod
    def generate(self, df: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """
        生成交易信号
        
        Args:
            df: 股票数据 (必须包含 OHLCV)
            code: 股票代码
            name: 股票名称
        
        Returns:
            Signal 或 None
        """
        pass
    
    def _check_ma_crossover(self, df: pd.DataFrame, fast: int, slow: int) -> tuple:
        """检查均线金叉/死叉"""
        if len(df) < slow + 2:
            return None, None
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        ma_fast = self.indicators.calculate_ma(df['Close'], fast)
        ma_slow = self.indicators.calculate_ma(df['Close'], slow)
        
        latest_fast = ma_fast.iloc[-1]
        latest_slow = ma_slow.iloc[-1]
        prev_fast = ma_fast.iloc[-2]
        prev_slow = ma_slow.iloc[-2]
        
        if pd.isna(latest_fast) or pd.isna(latest_slow):
            return None, None
        
        # 金叉
        if prev_fast <= prev_slow and latest_fast > latest_slow:
            return "golden_cross", f"MA{fast}上穿MA{slow}金叉"
        
        # 死叉
        if prev_fast >= prev_slow and latest_fast < latest_slow:
            return "death_cross", f"MA{fast}下穿MA{slow}死叉"
        
        return None, None
    
    def _check_ma排列(self, df: pd.DataFrame, periods: List[int]) -> Optional[str]:
        """检查均线多头/空头排列"""
        if len(df) < max(periods) + 1:
            return None
        
        close = df['Close']
        mas = {p: self.indicators.calculate_ma(close, p).iloc[-1] for p in periods}
        
        if any(pd.isna(v) for v in mas.values()):
            return None
        
        # 多头排列
        if all(mas[periods[i]] > mas[periods[i+1]] for i in range(len(periods)-1)):
            return "多头排列"
        
        # 空头排列
        if all(mas[periods[i]] < mas[periods[i+1]] for i in range(len(periods)-1)):
            return "空头排列"
        
        return None
    
    def _check_rsi(self, df: pd.DataFrame, period: int = 14) -> tuple:
        """检查RSI超买超卖"""
        rsi = self.indicators.calculate_rsi(df['Close'], period).iloc[-1]
        
        if pd.isna(rsi):
            return None, None
        
        if rsi < 30:
            return "oversold", f"RSI超卖({rsi:.1f})"
        elif rsi > 70:
            return "overbought", f"RSI超买({rsi:.1f})"
        
        return None, None
    
    def _check_macd(self, df: pd.DataFrame) -> tuple:
        """检查MACD金叉/死叉"""
        macd, signal, hist = self.indicators.calculate_macd(df['Close'])
        
        latest_macd = macd.iloc[-1]
        latest_signal = signal.iloc[-1]
        latest_hist = hist.iloc[-1]
        prev_hist = hist.iloc[-2]
        
        if pd.isna(latest_macd):
            return None, None
        
        # 金叉
        if latest_macd > latest_signal and prev_hist <= 0:
            return "golden_cross", "MACD金叉"
        
        # 死叉
        if latest_macd < latest_signal and prev_hist >= 0:
            return "death_cross", "MACD死叉"
        
        return None, None
    
    def _check_bollinger(self, df: pd.DataFrame) -> tuple:
        """检查布林带"""
        upper, middle, lower = self.indicators.calculate_bollinger_bands(df['Close'])
        
        current_price = df['Close'].iloc[-1]
        upper_price = upper.iloc[-1]
        lower_price = lower.iloc[-1]
        
        if pd.isna(upper_price):
            return None, None
        
        if current_price < lower_price:
            return "touch_lower", "触及布林下轨"
        elif current_price > upper_price:
            return "touch_upper", "触及布林上轨"
        
        return None, None


class CompositeSignalGenerator(SignalGeneratorBase):
    """
    复合信号生成器
    综合多种技术指标生成交易信号
    """
    
    def __init__(self, ma_periods: List[int] = None):
        super().__init__("CompositeSignal")
        self.ma_periods = ma_periods or [5, 10, 20, 60]
    
    def generate(self, df: pd.DataFrame, code: str, name: str = "") -> Optional[Signal]:
        """生成复合交易信号"""
        if df is None or len(df) < 60:
            return None
        
        try:
            # 计算技术指标
            close = df['Close']
            
            # 均线
            mas = {p: self.indicators.calculate_ma(close, p) for p in self.ma_periods}
            
            # RSI
            rsi = self.indicators.calculate_rsi(close)
            
            # MACD
            macd, macd_signal, macd_hist = self.indicators.calculate_macd(close)
            
            # 布林带
            bb_upper, bb_middle, bb_lower = self.indicators.calculate_bollinger_bands(close)
            
            latest = df.iloc[-1]
            current_price = latest['Close']
            
            # 评分系统
            buy_score = 0
            sell_score = 0
            reasons = []
            indicators = {}
            
            # 1. 均线分析
            ma_trend = self._check_ma排列(df, self.ma_periods)
            if ma_trend == "多头排列":
                buy_score += 20
                reasons.append("均线多头排列")
            elif ma_trend == "空头排列":
                sell_score += 20
                reasons.append("均线空头排列")
            
            # 均线金叉/死叉
            cross_type, cross_reason = self._check_ma_crossover(df, 5, 20)
            if cross_type == "golden_cross":
                buy_score += 25
                reasons.append(cross_reason)
            elif cross_type == "death_cross":
                sell_score += 25
                reasons.append(cross_reason)
            
            # 2. RSI分析
            rsi_type, rsi_reason = self._check_rsi(df)
            if rsi_type == "oversold":
                buy_score += 15
                reasons.append(rsi_reason)
                indicators['rsi'] = round(rsi.iloc[-1], 2)
            elif rsi_type == "overbought":
                sell_score += 15
                reasons.append(rsi_reason)
                indicators['rsi'] = round(rsi.iloc[-1], 2)
            
            # 3. MACD分析
            macd_type, macd_reason = self._check_macd(df)
            if macd_type == "golden_cross":
                buy_score += 20
                reasons.append(macd_reason)
            elif macd_type == "death_cross":
                sell_score += 20
                reasons.append(macd_reason)
            
            # 记录MACD值
            indicators['macd'] = round(macd.iloc[-1], 2)
            
            # 4. 布林带分析
            bb_type, bb_reason = self._check_bollinger(df)
            if bb_type == "touch_lower":
                buy_score += 15
                reasons.append(bb_reason)
            elif bb_type == "touch_upper":
                sell_score += 15
                reasons.append(bb_reason)
            
            # 5. 成交量分析
            if len(df) >= 20:
                vol_ma = self.indicators.calculate_volume_ma(df['Volume'])
                if latest['Volume'] > vol_ma.iloc[-1] * 1.5:
                    if latest['Close'] > df['Close'].iloc[-2]:
                        buy_score += 10
                        reasons.append("放量上涨")
                    else:
                        sell_score += 10
                        reasons.append("放量下跌")
            
            # 判断信号 - 降低阈值以产生更多交易
            if buy_score >= 15 and buy_score >= sell_score:
                action = "buy"
                strength = min(buy_score, 100)
                reason = "; ".join(reasons[:3])
            elif sell_score >= 15 and sell_score >= buy_score:
                action = "sell"
                strength = min(sell_score, 100)
                reason = "; ".join(reasons[:3])
            else:
                action = "hold"
                strength = 50
                if buy_score > sell_score:
                    reason = f"轻微买入信号 (买入:{buy_score}, 卖出:{sell_score})"
                elif sell_score > buy_score:
                    reason = f"轻微卖出信号 (买入:{buy_score}, 卖出:{sell_score})"
                else:
                    reason = "观望"
            
            return Signal(
                code=code,
                name=name,
                action=action,
                price=round(current_price, 2),
                strength=strength,
                reason=reason,
                timestamp=datetime.now().isoformat(),
                indicators=indicators
            )
            
        except Exception as e:
            logger.error(f"信号生成错误 {code}: {e}")
            return None
