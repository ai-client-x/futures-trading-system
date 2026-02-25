"""
缩量回调策略
缩量回调
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    缩量回调信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    vol_ma = volume.rolling(20).mean()
    vol_ratio = (volume / (vol_ma + 1e-10)).iloc[-1]
    price_change = (close - close.shift(5)) / (close.shift(5) + 1e-10) * 100
    is_pullback = 1 if price_change.iloc[-1] < -3 else 0
    is_volume_down = 1 if vol_ratio < 0.8 else 0
    vol_ratio_recent = (volume / (vol_ma + 1e-10)).tail(20)
    if len(vol_ratio_recent) < 10:
        return 50
    mean_vol = vol_ratio_recent.mean()
    std_vol = vol_ratio_recent.std() + 1e-10
    score = 50 + is_pullback * 20 + is_volume_down * 20 + (mean_vol - vol_ratio) / std_vol * 10
        return max(0, min(100, score))
    except:
        return 50
