"""
突破前高策略
突破前高
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    突破前高信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    high_20 = high.rolling(20).max()
    curr_above_high = 1 if close.iloc[-1] > high_20.iloc[-1] else 0
    volume_increase = volume.iloc[-1] / (volume.rolling(20).mean().iloc[-1] + 1e-10)
    close_recent = close.tail(20)
    if len(close_recent) < 10:
        return 50
    mean_close = close_recent.mean()
    std_close = close_recent.std() + 1e-10
    score = 50 + curr_above_high * 25 + (volume_increase - 1) * 10 + (close.iloc[-1] - mean_close) / std_close * 15
        return max(0, min(100, score))
    except:
        return 50
