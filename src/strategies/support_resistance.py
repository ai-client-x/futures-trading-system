"""
支撑阻力策略
支撑阻力位交易
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    支撑阻力信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    low_20 = low.rolling(20).min()
    high_20 = high.rolling(20).max()
    curr_price = close.iloc[-1]
    position = ((curr_price - low_20) / (high_20 - low_20 + 1e-10)).iloc[-1]
    position_recent = ((close - low.rolling(20).min()) / (high.rolling(20).max() - low.rolling(20).min() + 1e-10)).tail(20)
    if len(position_recent) < 10:
        return 50
    mean_pos = position_recent.mean()
    std_pos = position_recent.std() + 1e-10
    if action == 'buy':
        score = 50 + (mean_pos - position) / std_pos * 20
    else:
        score = 50 + (position - mean_pos) / std_pos * 20
        return max(0, min(100, score))
    except:
        return 50
