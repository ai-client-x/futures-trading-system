"""
MACD背离策略
MACD底背离
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    MACD背离信号分数 (0-100)
    使用相对位置评分，分数应接近正态分布
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:

    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    macd = ema12 - ema26
    price_low = low.rolling(20).min()
    macd_low = macd.rolling(20).min()
    price_dev = ((close - price_low) / (price_low + 1e-10)).iloc[-1]
    macd_dev = ((macd - macd_low) / (abs(macd_low) + 1e-10)).iloc[-1]
    price_dev_recent = ((close - low.rolling(20).min()) / (low.rolling(20).min() + 1e-10)).tail(20)
    macd_dev_recent = ((macd - macd.rolling(20).min()) / (macd.rolling(20).min().abs() + 1e-10)).tail(20)
    if len(price_dev_recent) < 10:
        return 50
    mean_price = price_dev_recent.mean()
    std_price = price_dev_recent.std() + 1e-10
    mean_macd = macd_dev_recent.mean()
    std_macd = macd_dev_recent.std() + 1e-10
    if action == 'buy':
        score = 50 + (macd_dev - price_dev - (mean_macd - mean_price)) / (std_macd + std_price) * 20
    else:
        score = 50
        return max(0, min(100, score))
    except:
        return 50
