"""
双底形态策略
双底形态
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    双底形态信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    low = hist['Low']
    low_20 = low.rolling(20).min()
    curr_low = low.iloc[-1]
    near_low = 1 if abs(curr_low - low_20.iloc[-1]) / (low_20.iloc[-1] + 1e-10) < 0.02 else 0
    price_recent = close.tail(10)
    has_rebound = 1 if (price_recent.max() - price_recent.min()) / price_recent.mean() > 0.05 else 0
    score = 50 + near_low * 30 + has_rebound * 10
        return max(0, min(100, score))
    except:
        return 50
