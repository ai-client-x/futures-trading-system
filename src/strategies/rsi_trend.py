"""
RSI趋势策略
RSI趋势跟踪
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    RSI趋势信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    period = 12
    delta = close.diff()
    gain = delta.where(delta > 0, 0).ewm(span=period).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(span=period).mean()
    rsi = 100 - (100 / (1 + gain / (loss + 1e-10)))
    rsi_ma = rsi.rolling(10).mean()
    curr_rsi = rsi.iloc[-1]
    rsi_above_ma = 1 if curr_rsi > rsi_ma.iloc[-1] else 0
    rsi_trend = rsi.diff().iloc[-1]
    rsi_recent = rsi.tail(20)
    if len(rsi_recent) < 10:
        return 50
    mean_rsi = rsi_recent.mean()
    std_rsi = rsi_recent.std() + 1e-10
    score = 50 + rsi_above_ma * 10 + (rsi_trend > 0) * 10 + (curr_rsi - mean_rsi) / std_rsi * 10
        return max(0, min(100, score))
    except:
        return 50
