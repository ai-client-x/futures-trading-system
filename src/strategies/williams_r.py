"""
威廉指标策略
Williams %R - 逆势策略，超卖时买入
"""

import pandas as pd
import numpy as np


def williams_r(hist: pd.DataFrame, action: str = 'buy', period: int = 14) -> float:
    """
    威廉指标信号分数 (0-100)
    
    原理：
    - WR < -80 表示超卖，可能反弹
    - WR > -20 表示超买，可能回调
    
    评分方法：使用相对位置评分，分数应接近正态分布
    """
    high = hist['High']
    low = hist['Low']
    close = hist['Close']
    
    highest = high.rolling(period).max()
    lowest = low.rolling(period).min()
    curr_close = close.iloc[-1]
    
    wr_series = -100 * (highest - close) / (highest - lowest + 1e-10)
    wr = wr_series.iloc[-1]
    
    if pd.isna(wr):
        return 50
    
    # 使用最近20天计算相对位置
    wr_recent = wr_series.tail(20).dropna()
    if len(wr_recent) < 10:
        return 50
    
    mean_wr = wr_recent.mean()
    std_wr = wr_recent.std() + 1e-10
    
    if action == 'buy':
        # WR越低（超卖）分数越高
        score = 50 + (mean_wr - wr) / std_wr * 20
    else:
        # WR越高（超买）分数越高
        score = 50 + (wr - mean_wr) / std_wr * 20
    
    return max(0, min(100, score))
