"""
波动率突破策略
波动率突破
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    波动率突破信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    vol_curr = ((high - low) / close * 100).iloc[-1]
    vol_ma = ((high.rolling(20).max() - low.rolling(20).min()) / close * 100)
    vol_ratio = (vol_curr / (vol_ma + 1e-10)).iloc[-1]
    vol_ratio_recent = ((high - low).rolling(20) / close * 100).tail(20)
    if len(vol_ratio_recent) < 10:
        return 50
    mean_vol = vol_ratio_recent.mean()
    std_vol = vol_ratio_recent.std() + 1e-10
    if action == 'buy':
        score = 50 + (vol_ratio - mean_vol) / std_vol * 20
    else:
        score = 50
        return max(0, min(100, score))
    except:
        return 50
