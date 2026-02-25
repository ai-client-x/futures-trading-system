"""
布林带策略
Bollinger Bands - 突破/回归
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    布林带信号分数 (0-100)
    """
    close = hist['Close']
    
    period = 20
    ma = close.rolling(period).mean()
    std = close.rolling(period).std()
    
    # 价格在布林带中的位置 (0=下轨, 1=上轨)
    position_series = (close - (ma - 2*std)) / (4*std + 1e-10)
    curr_position = position_series.iloc[-1]
    
    # 突破上下轨
    upper = ma + 2*std
    lower = ma - 2*std
    at_lower = 1 if close.iloc[-1] <= lower.iloc[-1] else 0
    at_upper = 1 if close.iloc[-1] >= upper.iloc[-1] else 0
    
    # 相对位置
    position_recent = position_series.tail(20).dropna()
    if len(position_recent) < 10:
        return 50
    
    mean_pos = position_recent.mean()
    std_pos = position_recent.std() + 1e-10
    
    if action == 'buy':
        # 接近下轨或突破时给高分
        score = 50 + at_lower * 25 + (mean_pos - curr_position) / std_pos * 15
    else:
        # 接近上轨时给高分
        score = 50 + at_upper * 25 + (curr_position - mean_pos) / std_pos * 15
    
    return max(0, min(100, score))
