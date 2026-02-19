#!/usr/bin/env python3
"""
每日简报生成与推送
"""

import os
import sys
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_daily_report():
    """生成每日简报"""
    
    # 1. 检查是否为交易日
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from src.notify import TradingDayChecker
    
    checker = TradingDayChecker()
    today = datetime.now()
    
    if not checker.is_trading_day(today):
        logger.info(f"今日 {today.strftime('%Y-%m-%d')} 非交易日，跳过")
        return None
    
    logger.info(f"生成每日简报: {today.strftime('%Y-%m-%d')}")
    
    # 2. 获取选股信号
    from src.strategies.hybrid_strategy import HybridStrategy
    
    strategy = HybridStrategy()
    signals = strategy.get_buy_signals(
        max_pe=25,
        min_roe=10,
        min_dv_ratio=1,
        max_debt=70,
        min_market_cap=30,
        hybrid_mode='strict'
    )
    
    # 3. 获取收益率数据
    from src.portfolio_optimizer import PortfolioOptimizer
    
    optimizer = PortfolioOptimizer()
    codes = [s.code for s in signals]
    returns = optimizer.get_stock_returns(codes, days=120)
    
    # 4. 生成组合优化
    capital = 1000000  # 100万
    positions = optimizer.optimize(signals, capital, 'risk_parity', returns)
    
    # 5. 构建消息
    report = build_report(today, signals, positions)
    
    return report


def build_report(date: datetime, signals, positions) -> dict:
    """构建简报内容"""
    
    title = f"📈 每日选股简报 - {date.strftime('%Y-%m-%d')}"
    
    # 选股信号
    stock_list = []
    for s in signals[:10]:
        pe = s.indicators.get('pe', 0)
        roe = s.indicators.get('roe', 0)
        stock_list.append((f"{s.code} {s.name}", f"PE={pe:.1f}, ROE={roe:.1f}%"))
    
    # 仓位分配
    allocation = []
    for p in positions[:10]:
        allocation.append((p['name'], f"{p['weight']*100:.1f}%"))
    
    return {
        'title': title,
        'date': date.strftime('%Y-%m-%d'),
        'stock_count': len(signals),
        'stocks': stock_list,
        'allocation': allocation,
        'total_value': sum(p['value'] for p in positions) if positions else 0
    }


def send_report(report: dict):
    """发送简报到飞书"""
    
    webhook = os.environ.get('FEISHU_WEBHOOK_URL')
    
    if not webhook:
        # 尝试读取配置文件
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'feishu.json')
        if os.path.exists(config_path):
            import json
            with open(config_path) as f:
                config = json.load(f)
                webhook = config.get('webhook_url')
    
    if not webhook:
        logger.warning("未配置飞书Webhook，发送模拟消息")
        # 打印到控制台
        print("\n" + "="*50)
        print(f"📈 {report['title']}")
        print("="*50)
        print(f"\n🎯 买入信号 ({report['stock_count']}只):")
        for name, desc in report['stocks'][:5]:
            print(f"  • {name}: {desc}")
        print(f"\n💰 仓位分配 (前5):")
        for name, weight in report['allocation'][:5]:
            print(f"  • {name}: {weight}")
        print("="*50)
        return False
    
    from src.notify import FeishuNotifier
    
    notifier = FeishuNotifier(webhook)
    
    # 构建内容
    content = []
    
    content.append(("📊 买入信号", f"共 {report['stock_count']} 只"))
    for name, desc in report['stocks'][:5]:
        content.append((name, desc))
    
    content.append(("", ""))  # 分隔
    content.append(("💰 仓位分配", "风险平价策略"))
    for name, weight in report['allocation'][:5]:
        content.append((name, weight))
    
    return notifier.send_rich_text(report['title'], content)


def main():
    """主函数"""
    logger.info("开始生成每日简报...")
    
    # 生成简报
    report = generate_daily_report()
    
    if report is None:
        logger.info("非交易日，跳过")
        return
    
    # 发送
    success = send_report(report)
    
    if success:
        logger.info("每日简报发送成功")
    else:
        logger.info("每日简报已生成（未发送）")


if __name__ == "__main__":
    main()
