"""
平台突破策略
平台突破
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    平台突破信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    high_60 = high.rolling(60).max()
    close_above_high = 1 if close.iloc[-1] > high_60.iloc[-1] else 0
    vol_ma = volume.rolling(20).mean()
    vol_increase = 1 if volume.iloc[-1] > vol_ma.iloc[-1] * 1.3 else 0
    near_high = 1 if abs(close.iloc[-1] - high_60.iloc[-1]) / (high_60.iloc[-1] + 1e-10) < 0.03 else 0
    score = 50 + close_above_high * 25 + vol_increase * 15 + near_high * 10
        return max(0, min(100, score))
    except:
        return 50
