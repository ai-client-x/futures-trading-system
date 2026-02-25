"""
MACD策略
Moving Average Convergence Divergence
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    MACD信号分数 (0-100)
    """
    close = hist['Close']
    
    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    macd_hist = macd - signal
    
    # MACD金叉/死叉
    golden_cross = 1 if macd_hist.iloc[-1] > 0 and macd_hist.iloc[-2] <= 0 else 0
    death_cross = 1 if macd_hist.iloc[-1] < 0 and macd_hist.iloc[-2] >= 0 else 0
    
    # MACD柱方向
    hist_up = 1 if macd_hist.iloc[-1] > macd_hist.iloc[-5] else 0
    
    # 相对位置
    macd_recent = macd_hist.tail(20)
    if len(macd_recent) < 10:
        return 50
    
    mean_macd = macd_recent.mean()
    std_macd = macd_recent.std() + 1e-10
    
    if action == 'buy':
        score = 50 + golden_cross * 30 + hist_up * 10 + (macd_hist.iloc[-1] - mean_macd) / std_macd * 10
    else:
        score = 50 + death_cross * 30 - hist_up * 10 + (mean_macd - macd_hist.iloc[-1]) / std_macd * 10
    
    return max(0, min(100, score))
