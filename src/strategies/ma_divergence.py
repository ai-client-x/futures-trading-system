"""
均线发散策略
均线发散
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    均线发散信号分数 (0-100)
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
    divergence = (ma5 - ma20).iloc[-1] - (ma5 - ma20).iloc[-5]
    divergence_recent = ((close.rolling(5).mean() - close.rolling(20).mean()) - (close.rolling(5).mean() - close.rolling(20).mean()).shift(5)).tail(20)
    if len(divergence_recent) < 10:
        return 50
    mean_div = divergence_recent.mean()
    std_div = divergence_recent.std() + 1e-10
    score = 50 + divergence / std_div * 20 + (divergence > 0) * 10
        return max(0, min(100, score))
    except:
        return 50
