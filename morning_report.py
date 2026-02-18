#!/usr/bin/env python3
"""
早报生成与推送
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from run_hybrid_strategy import HybridStrategy
from src.notify import FeishuNotifier

def generate_morning_report():
    """生成早报"""
    
    print("=" * 40)
    print("生成早报...")
    
    # 获取选股信号
    strategy = HybridStrategy()
    signals = strategy.get_buy_signals(
        max_pe=25, 
        min_roe=10, 
        min_dv_ratio=1, 
        max_debt=70, 
        min_market_cap=30, 
        hybrid_mode='strict'
    )
    
    # 生成消息
    if signals:
        msg = f"☀️ **早报 - {len(signals)}只候选**\n\n"
        
        # 基本面好的前5只
        for s in signals[:5]:
            pe = s.indicators.get('pe', 0)
            roe = s.indicators.get('roe', 0)
            name = s.name[:8]
            msg += f"• {name} ({s.code}): PE={pe:.1f}, ROE={roe:.1f}%\n"
        
        if len(signals) > 5:
            msg += f"\n...还有 {len(signals)-5} 只"
    else:
        msg = "📊 早报\n\n今日无符合条件的买入信号"
    
    print(msg)
    
    # 发送到飞书
    notifier = FeishuNotifier()
    notifier.send_text(msg)
    print("发送成功!")

if __name__ == "__main__":
    generate_morning_report()
