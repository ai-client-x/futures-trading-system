"""
趋势过滤策略
趋势过滤
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    趋势过滤信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    trend = 1 if ma5.iloc[-1] > ma20.iloc[-1] else -1
    trend_change = (ma5 - ma20).diff().iloc[-1]
    trend_recent = (ma5 - ma20).tail(20)
    if len(trend_recent) < 10:
        return 50
    mean_trend = trend_recent.mean()
    std_trend = trend_recent.std() + 1e-10
    if action == 'buy':
        score = 50 + trend * 20 + (trend_change > 0) * 10 + (trend - mean_trend/std_trend) * 10
    else:
        score = 50 - trend * 20
        return max(0, min(100, score))
    except:
        return 50
