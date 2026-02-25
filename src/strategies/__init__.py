"""
交易策略库
每个策略独立一个文件，导出 calc_signal 函数
"""

# 单一指标策略
from .rsi import calc_signal as rsi
from .ma import calc_signal as ma
from .bollinger_bands import calc_signal as bollinger_bands
from .volume import calc_signal as volume
from .macd_indicator import calc_signal as macd_indicator
from .williams_r import calc_signal as williams_r

# 组合策略
from .rsi_contrarian import calc_signal as rsi_contrarian
from .rsi_trend import calc_signal as rsi_trend
from .ma_strategy import calc_signal as ma_strategy
from .ma_divergence import calc_signal as ma_divergence
from .ma_recovery import calc_signal as ma_recovery
from .volume_breakout import calc_signal as volume_breakout
from .volume_price_rise import calc_signal as volume_price_rise
from .volume_pullback import calc_signal as volume_pullback
from .macd_divergence import calc_signal as macd_divergence
from .momentum_reversal import calc_signal as momentum_reversal
from .support_resistance import calc_signal as support_resistance
from .trend_filter import calc_signal as trend_filter
from .close_above_ma import calc_signal as close_above_ma
from .double_bottom import calc_signal as double_bottom
from .breakout_high import calc_signal as breakout_high
from .platform_breakout import calc_signal as platform_breakout
from .volatility_breakout import calc_signal as volatility_breakout

# 策略名称映射
STRATEGIES = {
    # 单一指标 (6个)
    'RSI': rsi,
    '均线': ma,
    '布林带': bollinger_bands,
    '成交量': volume,
    'MACD': macd_indicator,
    '威廉指标': williams_r,
    
    # RSI系列 (2个)
    'RSI逆势': rsi_contrarian,
    'RSI趋势': rsi_trend,
    
    # 均线系列 (3个)
    '均线策略': ma_strategy,
    '均线发散': ma_divergence,
    '均线收复': ma_recovery,
    
    # 成交量系列 (3个)
    '成交量突破': volume_breakout,
    '量价齐升': volume_price_rise,
    '缩量回调': volume_pullback,
    
    # MACD系列 (1个)
    'MACD背离': macd_divergence,
    
    # 其他 (8个)
    '动量反转': momentum_reversal,
    '支撑阻力': support_resistance,
    '趋势过滤': trend_filter,
    '收盘站均线': close_above_ma,
    '双底形态': double_bottom,
    '突破前高': breakout_high,
    '平台突破': platform_breakout,
    '波动率突破': volatility_breakout,
}


def get_strategy(name: str):
    """获取策略函数"""
    return STRATEGIES.get(name)


def list_strategies():
    """列出所有策略"""
    return list(STRATEGIES.keys())
