"""
收盘站均线策略
收盘价站上均线
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    收盘站均线信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    ma = close.rolling(20).mean()
    curr_above = 1 if close.iloc[-1] > ma.iloc[-1] else 0
    ma_above_ma10 = 1 if ma.iloc[-1] > close.rolling(10).mean().iloc[-1] else 0
    ma_recent = close.rolling(20).mean().tail(20)
    close_recent = close.tail(20)
    if len(ma_recent) < 10:
        return 50
    mean_ma = ma_recent.mean()
    std_ma = ma_recent.std() + 1e-10
    score = 50 + curr_above * 20 + ma_above_ma10 * 10 + (close.iloc[-1] - mean_ma) / std_ma * 10
        return max(0, min(100, score))
    except:
        return 50
