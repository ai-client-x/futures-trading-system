"""
均线策略策略
均线策略
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    均线策略信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()
    golden_cross = 1 if ma5.iloc[-1] > ma10.iloc[-1] and ma10.iloc[-1] > ma20.iloc[-1] else 0
    above_ma20 = 1 if close.iloc[-1] > ma20.iloc[-1] else 0
    score = 50 + golden_cross * 25 + above_ma20 * 15
        return max(0, min(100, score))
    except:
        return 50
