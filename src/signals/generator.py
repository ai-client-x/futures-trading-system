#!/usr/bin/env python3
"""
信号生成模块
Signal Generator for Futures
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict
from abc import ABC, abstractmethod

import pandas as pd
import numpy as np

from ..models import Signal
from ..config import config

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
    def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """ATR指标（平均真实波幅）"""
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        return atr
    
    @staticmethod
    def calculate_bollinger_bands(series: pd.Series, period: int = 20, std_dev: float = 2):
        """布林带"""
        ma = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        upper = ma + (std * std_dev)
        lower = ma - (std * std_dev)
        return upper, ma, lower


class FuturesSignalGenerator:
    """
    期货交易信号生成器
    """
    
    def __init__(self):
        self.indicators = TechnicalIndicators()
        self.config = config
        self.contracts = self.config.contracts
    
    def generate(self, df: pd.DataFrame, code: str, direction: str = None) -> Optional[Signal]:
        """
        生成交易信号
        
        Args:
            df: 期货数据 (必须包含 OHLCV)
            code: 合约代码
            direction: 指定方向 (None表示双向)
        
        Returns:
            Signal 或 None
        """
        if df is None or len(df) < 60:
            return None
        
        try:
            close = df['Close']
            high = df['High']
            low = df['Low']
            volume = df['Volume']
            
            # 计算技术指标
            ma5 = self.indicators.calculate_ma(close, 5)
            ma10 = self.indicators.calculate_ma(close, 10)
            ma20 = self.indicators.calculate_ma(close, 20)
            
            rsi = self.indicators.calculate_rsi(close)
            macd, macd_signal, macd_hist = self.indicators.calculate_macd(close)
            atr = self.indicators.calculate_atr(high, low, close)
            
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            current_price = latest['Close']
            
            # 评分系统
            buy_score = 0
            sell_score = 0
            reasons = []
            indicators = {}
            
            contract = self.contracts.get(code, {})
            name = contract.get("name", code)
            
            # 1. 均线系统分析
            if pd.notna(ma5.iloc[-1]) and pd.notna(ma20.iloc[-1]):
                if ma5.iloc[-1] > ma20.iloc[-1]:
                    buy_score += 15
                    reasons.append("均线多头排列")
                else:
                    sell_score += 15
                    reasons.append("均线空头排列")
            
            # 均线金叉/死叉
            if pd.notna(ma5.iloc[-1]) and pd.notna(ma10.iloc[-1]):
                if ma5.iloc[-2] <= ma10.iloc[-2] and ma5.iloc[-1] > ma10.iloc[-1]:
                    buy_score += 20
                    reasons.append("MA5上穿MA10金叉")
                elif ma5.iloc[-2] >= ma10.iloc[-2] and ma5.iloc[-1] < ma10.iloc[-1]:
                    sell_score += 20
                    reasons.append("MA5下穿MA10死叉")
            
            # 2. RSI分析
            rsi_val = rsi.iloc[-1]
            if pd.notna(rsi_val):
                indicators['rsi'] = round(rsi_val, 2)
                if rsi_val < 30:
                    buy_score += 15
                    reasons.append(f"RSI超卖({rsi_val:.1f})")
                elif rsi_val > 70:
                    sell_score += 15
                    reasons.append(f"RSI超买({rsi_val:.1f})")
            
            # 3. MACD分析
            macd_val = macd.iloc[-1]
            macd_sig_val = macd_signal.iloc[-1]
            macd_hist_val = macd_hist.iloc[-1]
            macd_hist_prev = macd_hist.iloc[-2]
            
            if pd.notna(macd_val):
                indicators['macd'] = round(macd_val, 2)
                
                if macd_val > macd_sig_val and macd_hist_prev <= 0:
                    buy_score += 15
                    reasons.append("MACD金叉")
                elif macd_val < macd_sig_val and macd_hist_prev >= 0:
                    sell_score += 15
                    reasons.append("MACD死叉")
            
            # 4. 突破分析
            highest_20 = high.rolling(20).max().iloc[-1]
            lowest_20 = low.rolling(20).min().iloc[-1]
            
            if current_price > highest_20:
                buy_score += 20
                reasons.append("突破20日高点")
            elif current_price < lowest_20:
                sell_score += 20
                reasons.append("跌破20日低点")
            
            # 5. 成交量分析
            avg_volume = volume.rolling(20).mean().iloc[-1]
            if latest['Volume'] > avg_volume * 1.5:
                if current_price > prev['Close']:
                    buy_score += 10
                    reasons.append("放量上涨")
                else:
                    sell_score += 10
                    reasons.append("放量下跌")
            
            # 判断信号
            # 优先处理平仓信号
            if direction == "long" and sell_score >= 25:
                action = "close"
                signal_direction = "long"
                strength = min(sell_score, 100)
                reason = "; ".join(reasons[:2])
            elif direction == "short" and buy_score >= 25:
                action = "close"
                signal_direction = "short"
                strength = min(buy_score, 100)
                reason = "; ".join(reasons[:2])
            elif buy_score >= 30 and buy_score > sell_score:
                action = "open"
                signal_direction = "long"
                strength = min(buy_score, 100)
                reason = "; ".join(reasons[:2])
            elif sell_score >= 30 and sell_score > buy_score:
                action = "open"
                signal_direction = "short"
                strength = min(sell_score, 100)
                reason = "; ".join(reasons[:2])
            else:
                return None
            
            return Signal(
                code=code,
                name=name,
                action=action,
                direction=signal_direction,
                strength=strength,
                reason=reason,
                price=round(current_price, 2),
                timestamp=datetime.now().isoformat(),
                indicators=indicators
            )
            
        except Exception as e:
            logger.error(f"信号生成错误 {code}: {e}")
            return None
    
    def generate_all_signals(self, price_data: Dict[str, pd.DataFrame]) -> List[Signal]:
        """
        为所有合约生成信号
        
        Args:
            price_data: {code: DataFrame}
        
        Returns:
            信号列表
        """
        signals = []
        
        for code, df in price_data.items():
            signal = self.generate(df, code)
            if signal:
                signals.append(signal)
        
        return signals
