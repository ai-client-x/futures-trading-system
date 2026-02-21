"""
26个交易策略的动态信号强度计算
根据实际条件满足程度计算分数，越满足条件分数越高
"""

import pandas as pd
import numpy as np


def calc_signal_strength(hist: pd.DataFrame, strategy_name: str, action: str = 'buy') -> float:
    """
    根据策略条件和股票实际情况计算信号强度分数 (0-100)
    
    Args:
        hist: 历史数据 (包含 OHLCV)
        strategy_name: 策略名称
        action: 'buy' 或 'sell'
    
    Returns:
        分数 (0-100)
    """
    close = hist['Close']
    high = hist['High']
    low = hist['Low']
    volume = hist['Volume']
    
    try:
        # ========== 1. 威廉指标 ==========
        if strategy_name == "威廉指标":
            period = 14
            highest = high.rolling(period).max().iloc[-1]
            lowest = low.rolling(period).min().iloc[-1]
            curr_close = close.iloc[-1]
            
            if pd.isna(highest) or pd.isna(lowest):
                return 0
            
            wr = -100 * (highest - curr_close) / (highest - lowest + 1e-10)
            
            if action == "buy":
                # WR越低分数越高: -100→100分, -90→50分
                return max(0, min(100, (90 + wr) * 10))
            else:
                # 卖出: WR越高分数越高
                return max(0, min(100, (wr + 10) * 2))
        
        # ========== 2. RSI逆势 ==========
        elif strategy_name == "RSI逆势":
            period = 12
            delta = close.diff()
            gain = delta.where(delta > 0, 0).rolling(period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
            rsi = 100 - (100 / (1 + gain / (loss + 1e-10)))
            
            curr_rsi = rsi.iloc[-1]
            
            if action == "buy":
                # RSI越低分数越高: 10→100分, 30→50分
                return max(0, min(100, (30 - curr_rsi) * 5))
            else:
                # RSI越高分数越高
                return max(0, min(100, (curr_rsi - 50) * 2))
        
        # ========== 3. 量价齐升 ==========
        elif strategy_name == "量价齐升":
            # 涨幅 + 放量程度
            change = (close.iloc[-1] - close.iloc[-5]) / close.iloc[-5] * 100
            
            vol_ma5 = volume.rolling(5).mean().iloc[-1]
            vol_ma20 = volume.rolling(20).mean().iloc[-1]
            vol_ratio = vol_ma5 / (vol_ma20 + 1e-10)
            
            if action == "buy":
                # 涨幅越大且放量越多分数越高
                price_score = max(0, min(50, change * 5))
                vol_score = max(0, min(50, (vol_ratio - 1) * 50))
                return price_score + vol_score
            else:
                return 50
        
        # ========== 4. MACD+成交量 ==========
        elif strategy_name == "MACD+成交量":
            ema12 = close.ewm(span=12).mean()
            ema26 = close.ewm(span=26).mean()
            macd = ema12 - ema26
            signal = macd.ewm(span=9).mean()
            macd_hist = macd - signal
            
            vol_ma = volume.rolling(20).mean().iloc[-1]
            curr_vol = volume.iloc[-1]
            
            if action == "buy":
                # MACD金叉 + 放量
                macd_score = max(0, min(50, macd_hist.iloc[-1] * 200 + 50)) if macd_hist.iloc[-1] > 0 else 0
                vol_score = max(0, min(50, (curr_vol / vol_ma - 1) * 50)) if curr_vol > vol_ma else 0
                return macd_score + vol_score
            else:
                return 50
        
        # ========== 5. 动量反转 ==========
        elif strategy_name == "动量反转":
            period = 14
            change = (close.iloc[-1] - close.iloc[-period]) / close.iloc[-period] * 100
            
            if action == "buy":
                # 跌幅越大分数越高: -20%→100分, -5%→50分
                return max(0, min(100, (abs(change) - 5) * 6.67))
            else:
                # 涨幅超过一定幅度
                return min(100, max(30, change * 5))
        
        # ========== 6. 支撑阻力 ==========
        elif strategy_name == "支撑阻力":
            # 接近20日低点
            low_20 = low.rolling(20).min().iloc[-1]
            curr_low = low.iloc[-1]
            
            if action == "buy":
                # 接近20日最低点
                dist = (curr_low - low_20) / (low_20 + 1e-10)
                return max(0, min(100, 100 - dist * 1000))
            else:
                # 接近20日最高点
                high_20 = high.rolling(20).max().iloc[-1]
                curr_high = close.iloc[-1]
                dist = (high_20 - curr_high) / (high_20 + 1e-10)
                return max(0, min(100, 100 - dist * 1000))
        
        # ========== 7. MACD背离 ==========
        elif strategy_name == "MACD背离":
            # MACD底背离: 价格新低但MACD没有新低
            ema12 = close.ewm(span=12).mean()
            ema26 = close.ewm(span=26).mean()
            macd = ema12 - ema26
            
            price_low = low.rolling(20).min().iloc[-1]
            macd_low = macd.rolling(20).min().iloc[-1]
            
            curr_price = close.iloc[-1]
            curr_macd = macd.iloc[-1]
            
            if action == "buy":
                # 价格接近低点但MACD没有新低 = 背离
                price_dev = (curr_price - price_low) / (price_low + 1e-10)
                macd_dev = (curr_macd - macd_low) / (abs(macd_low) + 1e-10)
                return max(0, min(100, 50 + (macd_dev - price_dev) * 100))
            else:
                return 50
        
        # ========== 8. 布林带 ==========
        elif strategy_name == "布林带":
            period = 20
            ma = close.rolling(period).mean()
            std = close.rolling(period).std()
            upper = ma + 2 * std
            lower = ma - 2 * std
            
            curr_c = close.iloc[-1]
            
            if action == "buy":
                # 接近下轨或突破中轨
                if curr_c < lower.iloc[-1]:
                    return max(0, min(100, (lower.iloc[-1] - curr_c) / lower.iloc[-1] * 200 + 50))
                else:
                    return max(0, min(100, 100 - (curr_c - ma.iloc[-1]) / std.iloc[-1] * 30))
            else:
                # 接近上轨
                if curr_c > upper.iloc[-1]:
                    return max(0, min(100, (curr_c - upper.iloc[-1]) / upper.iloc[-1] * 200 + 50))
                else:
                    return max(0, min(100, (curr_c - ma.iloc[-1]) / std.iloc[-1] * 30 + 50))
        
        # ========== 9. MACD策略 ==========
        elif strategy_name == "MACD策略":
            ema12 = close.ewm(span=12).mean()
            ema26 = close.ewm(span=26).mean()
            macd = ema12 - ema26
            signal = macd.ewm(span=9).mean()
            
            macd_hist = macd - signal
            
            if action == "buy":
                # MACD金叉
                return max(0, min(100, macd_hist.iloc[-1] * 200 + 50)) if macd_hist.iloc[-1] > 0 else 0
            else:
                # MACD死叉
                return max(0, min(100, -macd_hist.iloc[-1] * 200 + 50)) if macd_hist.iloc[-1] < 0 else 30
        
        # ========== 10. 成交量突破 ==========
        elif strategy_name == "成交量突破":
            period = 20
            vol_ma = volume.rolling(period).mean().iloc[-1]
            curr_vol = volume.iloc[-1]
            
            if action == "buy":
                # 放量倍数越高分数越高: 1.5倍→60分, 3倍→100分
                return max(0, min(100, (curr_vol / vol_ma - 1.5) * 40 + 60)) if curr_vol > vol_ma * 1.5 else 0
            else:
                return 50
        
        # ========== 11. 波动率突破 ==========
        elif strategy_name == "波动率突破":
            # 波动率突破: 当前波动率 > 平均波动率
            vol_curr = (high.iloc[-1] - low.iloc[-1]) / close.iloc[-1] * 100
            vol_ma = ((high.rolling(20).max() - low.rolling(20).min()) / close).mean() * 100
            
            if action == "buy":
                return max(0, min(100, (vol_curr / (vol_ma + 1e-10) - 1) * 100 + 50))
            else:
                return 50
        
        # ========== 12. 布林带+RSI ==========
        elif strategy_name == "布林带+RSI":
            period = 20
            ma = close.rolling(period).mean()
            std = close.rolling(period).std()
            lower = ma - 2 * std
            
            rsi_period = 12
            delta = close.diff()
            gain = delta.where(delta > 0, 0).rolling(rsi_period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(rsi_period).mean()
            rsi = 100 - (100 / (1 + gain / (loss + 1e-10)))
            
            curr_rsi = rsi.iloc[-1]
            
            if action == "buy":
                # 布林下轨 + RSI超卖
                bb_score = 50 if close.iloc[-1] < lower.iloc[-1] else 30
                rsi_score = max(0, min(50, (30 - curr_rsi) * 5))
                return bb_score + rsi_score
            else:
                return 50
        
        # ========== 13. RSI趋势 ==========
        elif strategy_name == "RSI趋势":
            period = 12
            delta = close.diff()
            gain = delta.where(delta > 0, 0).rolling(period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
            rsi = 100 - (100 / (1 + gain / (loss + 1e-10)))
            
            rsi_ma = rsi.rolling(5).mean()
            
            if action == "buy":
                # RSI低位回升
                curr = rsi.iloc[-1]
                prev = rsi.iloc[-2]
                return max(0, min(100, (curr - prev) * 20 + 50))
            else:
                return max(0, min(100, rsi.iloc[-1] - 50 * 2))
        
        # ========== 14. 收盘站均线 ==========
        elif strategy_name == "收盘站均线":
            ma20 = close.rolling(20).mean().iloc[-1]
            curr_c = close.iloc[-1]
            
            if action == "buy":
                # 站上均线越多分数越高
                return max(0, min(100, (curr_c / ma20 - 1) * 1000 + 50))
            else:
                return max(0, min(100, (ma20 / curr_c - 1) * 1000 + 50))
        
        # ========== 15. 趋势过滤 ==========
        elif strategy_name == "趋势过滤":
            ma20 = close.rolling(20).mean().iloc[-1]
            ma60 = close.rolling(60).mean().iloc[-1]
            
            if action == "buy":
                # MA20向上
                return max(0, min(100, (ma20 / ma60 - 1) * 1000 + 50)) if ma20 > ma60 else 20
            else:
                return 50
        
        # ========== 16. 成交量+均线 ==========
        elif strategy_name == "成交量+均线":
            ma5 = close.rolling(5).mean().iloc[-1]
            ma20 = close.rolling(20).mean().iloc[-1]
            vol_ratio = volume.iloc[-1] / (volume.rolling(20).mean().iloc[-1] + 1e-10)
            
            if action == "buy":
                # 均线多头 + 放量
                ma_score = 50 if ma5 > ma20 else 30
                vol_score = max(0, min(50, (vol_ratio - 1) * 50))
                return ma_score + vol_score
            else:
                return 50
        
        # ========== 17. 均线策略 ==========
        elif strategy_name == "均线策略":
            ma5 = close.rolling(5).mean().iloc[-1]
            ma20 = close.rolling(20).mean().iloc[-1]
            
            if action == "buy":
                # MA5 > MA20 越多分数越高
                return max(0, min(100, (ma5 / ma20 - 1) * 2000 + 50))
            else:
                return max(0, min(100, (ma20 / ma5 - 1) * 2000 + 50))
        
        # ========== 18. 均线交叉强度 ==========
        elif strategy_name == "均线交叉强度":
            ma5 = close.rolling(5).mean()
            ma10 = close.rolling(10).mean()
            ma20 = close.rolling(20).mean()
            
            if action == "buy":
                # 多头排列程度
                score = 0
                if ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1]:
                    score = 100
                elif ma5.iloc[-1] > ma10.iloc[-1]:
                    score = 70
                else:
                    score = 40
                return score
            else:
                return 50
        
        # ========== 19. RSI+均线 ==========
        elif strategy_name == "RSI+均线":
            rsi_period = 12
            delta = close.diff()
            gain = delta.where(delta > 0, 0).rolling(rsi_period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(rsi_period).mean()
            rsi = 100 - (100 / (1 + gain / (loss + 1e-10)))
            
            ma20 = close.rolling(20).mean()
            above_ma = close.iloc[-1] > ma20.iloc[-1]
            
            if action == "buy":
                # RSI超卖 + 站上均线
                rsi_score = max(0, min(50, (30 - rsi.iloc[-1]) * 5))
                ma_score = 50 if above_ma else 25
                return rsi_score + ma_score
            else:
                return 50
        
        # ========== 20. 双底形态 ==========
        elif strategy_name == "双底形态":
            # W底: 两个相近的低点
            low_20 = low.rolling(20).min()
            lows = []
            for i in range(-20, 0):
                if abs(low.iloc[i] - low_20.iloc[i]) / (low_20.iloc[i] + 1e-10) < 0.02:
                    lows.append(low.iloc[i])
            
            if action == "buy":
                # 形成双底
                return min(100, 50 + len(lows) * 20) if len(lows) >= 2 else 30
            else:
                return 50
        
        # ========== 21. 均线发散 ==========
        elif strategy_name == "均线发散":
            ma5 = close.rolling(5).mean().iloc[-1]
            ma10 = close.rolling(10).mean().iloc[-1]
            ma20 = close.rolling(20).mean().iloc[-1]
            
            if action == "buy":
                # 均线发散 (多头排列且差距扩大)
                if ma5 > ma10 > ma20:
                    spread = (ma5 - ma20) / (ma20 + 1e-10)
                    return max(0, min(100, spread * 500 + 50))
                return 30
            else:
                return 50
        
        # ========== 22. 缩量回调 ==========
        elif strategy_name == "缩量回调":
            vol_curr = volume.iloc[-1]
            vol_ma5 = volume.rolling(5).mean().iloc[-1]
            vol_ma20 = volume.rolling(20).mean().iloc[-1]
            
            change = (close.iloc[-1] - close.iloc[-5]) / close.iloc[-5] * 100
            
            if action == "buy":
                # 缩量回调 (跌时缩量)
                vol_score = max(0, min(50, (1 - vol_curr / vol_ma20) * 100))
                price_score = max(0, min(50, abs(change) * 5))
                return vol_score + price_score if change < 0 else 30
            else:
                return 50
        
        # ========== 23. 突破确认 ==========
        elif strategy_name == "突破确认":
            high_20 = high.rolling(20).max().iloc[-2]  # 昨天
            curr_high = close.iloc[-1]
            
            if action == "buy":
                # 突破20日高点
                return max(0, min(100, (curr_high / high_20 - 1) * 1000 + 50))
            else:
                return 50
        
        # ========== 24. 平台突破 ==========
        elif strategy_name == "平台突破":
            # 横盘整理后突破
            price_range = (high.rolling(20).max() - low.rolling(20).min()).iloc[-1]
            avg_range = ((high.rolling(20).max() - low.rolling(20).min())).mean()
            
            if action == "buy":
                # 窄幅震荡后突破
                consolidation = price_range / (avg_range + 1e-10)
                if consolidation < 0.8:  # 窄幅震荡
                    return max(0, min(100, (1 - consolidation) * 100 + 50))
                return 40
            else:
                return 50
        
        # ========== 25. 突破前高 ==========
        elif strategy_name == "突破前高":
            high_20 = high.rolling(20).max().iloc[-1]
            curr_c = close.iloc[-1]
            
            if action == "buy":
                # 突破20日新高
                return max(0, min(100, (curr_c / high_20 - 1) * 1000 + 50))
            else:
                return 50
        
        # ========== 26. 均线收复 ==========
        elif strategy_name == "均线收复":
            ma20 = close.rolling(20).mean().iloc[-1]
            curr_c = close.iloc[-1]
            
            if action == "buy":
                # 站上MA20
                return max(0, min(100, (curr_c / ma20 - 1) * 1000 + 50))
            else:
                return max(0, min(100, (ma20 / curr_c - 1) * 1000 + 50))
        
        # 默认分数
        return 50 if action == 'buy' else 50
        
    except Exception as e:
        return 0


# 导出
__all__ = ['calc_signal_strength']
