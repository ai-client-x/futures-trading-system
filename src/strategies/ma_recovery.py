"""
均线收复策略
均线收复
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    均线收复信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    ma20 = close.rolling(20).mean()
    below_ma20 = 1 if close.iloc[-1] < ma20.iloc[-1] else 0
    recovering = 1 if close.iloc[-1] > close.iloc[-3] and close.iloc[-3] < ma20.iloc[-3] else 0
    ma20_trend = (ma20 - ma20.shift(5)).iloc[-1]
    score = 50 - below_ma20 * 20 + recovering * 30 + ma20_trend * 20
        return max(0, min(100, score))
    except:
        return 50
