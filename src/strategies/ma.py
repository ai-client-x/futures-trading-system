"""
均线策略
Moving Average - 均线交叉/价格位置
"""

import pandas as pd
import numpy as np


def calc_signal(hist: pd.DataFrame, action: str = 'buy') -> float:
    """
    均线信号分数 (0-100)
    """
    close = hist['Close']
    
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    
    # 当前价格相对均线的位置
    curr_ma5_pos = ((close.iloc[-1] - ma5.iloc[-1]) / (ma5.iloc[-1] + 1e-10))
    curr_ma20_pos = ((close.iloc[-1] - ma20.iloc[-1]) / (ma20.iloc[-1] + 1e-10))
    
    # 多头排列：5>20>60
    bullish = 1 if ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1] else 0
    
    # 均线方向
    ma5_up = 1 if ma5.iloc[-1] > ma5.iloc[-5] else 0
    
    # 相对位置计算
    ma_recent = ((close - close.rolling(20).mean()) / (close.rolling(20).mean() + 1e-10)).tail(20)
    if len(ma_recent) < 10:
        return 50
    
    mean_pos = ma_recent.mean()
    std_pos = ma_recent.std() + 1e-10
    
    if action == 'buy':
        score = 50 + bullish * 20 + ma5_up * 10 + (0 - curr_ma20_pos) / std_pos * 10
    else:
        score = 50 - bullish * 20 - ma5_up * 10 + curr_ma20_pos / std_pos * 10
    
    return max(0, min(100, score))
