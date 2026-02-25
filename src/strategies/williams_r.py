"""
威廉指标策略
Williams %R - 逆势策略，超卖时买入
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    威廉指标信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    period = 14
    highest = high.rolling(period).max()
    lowest = low.rolling(period).min()
    wr_series = -100 * (highest - close) / (highest - lowest + 1e-10)
    wr = wr_series.iloc[-1]
    wr_recent = wr_series.tail(20).dropna()
    if len(wr_recent) < 10:
        return 50
    mean_wr = wr_recent.mean()
    std_wr = wr_recent.std() + 1e-10
    if action == 'buy':
        score = 50 + (mean_wr - wr) / std_wr * 20
    else:
        score = 50 + (wr - mean_wr) / std_wr * 20
        return max(0, min(100, score))
    except:
        return 50
