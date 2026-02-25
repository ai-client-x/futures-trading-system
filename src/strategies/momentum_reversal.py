"""
动量反转策略
下跌后反弹
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    动量反转信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    period = 14
    change = ((close - close.shift(period)) / (close.shift(period) + 1e-10) * 100).iloc[-1]
    change_recent = ((close - close.shift(period)) / (close.shift(period) + 1e-10) * 100).tail(20)
    if len(change_recent) < 10:
        return 50
    mean_change = change_recent.mean()
    std_change = change_recent.std() + 1e-10
    if action == 'buy':
        score = 50 + (mean_change - change) / std_change * 20
    else:
        score = 50 + (change - mean_change) / std_change * 20
        return max(0, min(100, score))
    except:
        return 50
