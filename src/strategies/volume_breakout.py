"""
成交量突破策略
成交量突破
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    成交量突破信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    period = 20
    vol_ma = volume.rolling(period).mean()
    curr_vol = volume.iloc[-1]
    vol_ratio = (volume / (vol_ma + 1e-10)).iloc[-1]
    vol_ratio_recent = (volume / (vol_ma + 1e-10)).tail(20)
    if len(vol_ratio_recent) < 10:
        return 50
    mean_vol = vol_ratio_recent.mean()
    std_vol = vol_ratio_recent.std() + 1e-10
    if action == 'buy':
        score = 50 + (vol_ratio - mean_vol) / std_vol * 20
    else:
        score = 50 + (mean_vol - vol_ratio) / std_vol * 20
        return max(0, min(100, score))
    except:
        return 50
