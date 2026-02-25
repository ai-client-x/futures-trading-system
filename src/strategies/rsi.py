"""
RSI策略
Relative Strength Index - 超买超卖指标
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    RSI信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    
    period = 14
    delta = close.diff()
    gain = delta.where(delta > 0, 0).ewm(span=period).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(span=period).mean()
    rs = gain / (loss + 1e-10)
    rsi = 100 - (100 / (1 + rs))
    curr_rsi = rsi.iloc[-1]
    
    if pd.isna(curr_rsi):
        return 50
    
    # 使用最近20天计算相对位置
    rsi_recent = rsi.tail(20).dropna()
    if len(rsi_recent) < 10:
        return 50
    
    mean_rsi = rsi_recent.mean()
    std_rsi = rsi_recent.std() + 1e-10
    
    if action == 'buy':
        # RSI低于均值时分数高（超卖）
        score = 50 + (mean_rsi - curr_rsi) / std_rsi * 20
    else:
        # RSI高于均值时分数高（超买）
        score = 50 + (curr_rsi - mean_rsi) / std_rsi * 20
    
    return max(0, min(100, score))
