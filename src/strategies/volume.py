"""
成交量策略
Volume - 放量/缩量
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    成交量信号分数 (0-100)
    """
    volume = hist['Volume']
    close = hist['Close']
    
    period = 20
    vol_ma = volume.rolling(period).mean()
    
    # 当前成交量相对均值
    curr_ratio = volume.iloc[-1] / (vol_ma.iloc[-1] + 1e-10)
    
    # 放量程度
    vol_up = 1 if curr_ratio > 1.5 else 0
    vol_down = 1 if curr_ratio < 0.7 else 0
    
    # 量价配合
    price_up = 1 if close.iloc[-1] > close.iloc[-5] else 0
    
    # 相对位置
    ratio_series = (volume / (vol_ma + 1e-10)).tail(20)
    if len(ratio_series) < 10:
        return 50
    
    mean_ratio = ratio_series.mean()
    std_ratio = ratio_series.std() + 1e-10
    
    if action == 'buy':
        # 放量且上涨
        score = 50 + vol_up * 25 + price_up * 10 + (curr_ratio - mean_ratio) / std_ratio * 10
    else:
        # 缩量
        score = 50 + vol_down * 25 - price_up * 10 + (mean_ratio - curr_ratio) / std_ratio * 10
    
    return max(0, min(100, score))
