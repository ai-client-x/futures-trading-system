"""
量价齐升策略
放量且上涨
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    量价齐升信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    change = (close.iloc[-1] - close.iloc[-5]) / close.iloc[-5] * 100
    vol_ma5 = volume.rolling(5).mean()
    vol_ma20 = volume.rolling(20).mean()
    vol_ratio = (vol_ma5 / (vol_ma20 + 1e-10)).iloc[-1]
    price_change = change
    change_recent = ((close - close.shift(5)) / (close.shift(5) + 1e-10) * 100).tail(20)
    vol_ratio_recent = (vol_ma5 / (vol_ma20 + 1e-10)).tail(20)
    if len(change_recent) < 10:
        return 50
    mean_change = change_recent.mean()
    std_change = change_recent.std() + 1e-10
    mean_vol = vol_ratio_recent.mean()
    std_vol = vol_ratio_recent.std() + 1e-10
    price_score = 50 + (price_change - mean_change) / std_change * 15
    vol_score = 50 + (vol_ratio - mean_vol) / std_vol * 15
    score = (price_score + vol_score) / 2 if action == 'buy' else 50
        return max(0, min(100, score))
    except:
        return 50
