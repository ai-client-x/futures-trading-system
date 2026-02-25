"""
RSI逆势策略
RSI逆势 - 超卖时买入
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    RSI逆势信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    period = 12
    delta = close.diff()
    gain = delta.where(delta > 0, 0).ewm(span=period).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(span=period).mean()
    rs = gain / (loss + 1e-10)
    rsi = 100 - (100 / (1 + rs))
    curr_rsi = rsi.iloc[-1]
    rsi_recent = rsi.tail(20).dropna()
    if len(rsi_recent) < 10:
        return 50
    mean_rsi = rsi_recent.mean()
    std_rsi = rsi_recent.std() + 1e-10
    if action == 'buy':
        score = 50 + (mean_rsi - curr_rsi) / std_rsi * 20
    else:
        score = 50 + (curr_rsi - mean_rsi) / std_rsi * 20
        return max(0, min(100, score))
    except:
        return 50
