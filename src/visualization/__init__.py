#!/usr/bin/env python3
"""
回测结果可视化模块
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np
from typing import List, Dict
import os

# 字体配置
FONT_PATH = '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc'
if os.path.exists(FONT_PATH):
    fm.fontManager.addfont(FONT_PATH)
    PROP = fm.FontProperties(fname=FONT_PATH)
else:
    PROP = None

plt.rcParams['axes.unicode_minus'] = False


def plot_strategy_comparison(results: List[Dict], save_path: str = None) -> str:
    """
    绘制策略收益和回撤对比图
    
    Args:
        results: 回测结果列表，每个元素包含:
            - strategy: 策略名
            - total_return: 总收益 (%)
            - annual_return: 年化收益 (%)
            - max_drawdown: 最大回撤 (%)
            - trade_count: 交易次数
        save_path: 保存路径
    
    Returns:
        保存的文件路径
    """
    if not results:
        return ""
    
    strategies = [r.get('strategy', r.get('name', 'Unknown')) for r in results]
    returns = [r.get('total_return', r.get('annual_return', 0)) for r in results]
    drawdowns = [r.get('max_drawdown', 0) for r in results]
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # 收益图
    colors = ['#2ecc71' if r > 0 else '#e74c3c' for r in returns]
    bars1 = axes[0].barh(strategies, returns, color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
    axes[0].axvline(x=0, color='black', linewidth=1)
    axes[0].set_xlabel('收益率 (%)', fontsize=12, fontproperties=PROP)
    axes[0].set_title('策略收益对比', fontsize=14, fontweight='bold', fontproperties=PROP)
    axes[0].set_xlim(min(returns) - 10, max(returns) + 15)
    
    # 设置y轴标签
    if PROP:
        axes[0].set_yticks(range(len(strategies)))
        axes[0].set_yticklabels(strategies, fontproperties=PROP)
    
    for i, v in enumerate(returns):
        axes[0].text(v + (3 if v > 0 else -3), i, f'{v:+.1f}%', 
                    va='center', fontsize=10, fontweight='bold',
                    ha='left' if v > 0 else 'right')
    
    # 回撤图
    colors2 = ['#2ecc71' if d < 30 else '#f39c12' if d < 40 else '#e74c3c' for d in drawdowns]
    bars2 = axes[1].barh(strategies, drawdowns, color=colors2, alpha=0.8, edgecolor='black', linewidth=0.5)
    axes[1].axvline(x=30, color='#f39c12', linewidth=2, linestyle='--', label='警戒线 30%')
    axes[1].axvline(x=40, color='#e74c3c', linewidth=2, linestyle='--', label='危险线 40%')
    axes[1].set_xlabel('最大回撤 (%)', fontsize=12, fontproperties=PROP)
    axes[1].set_title('策略最大回撤', fontsize=14, fontweight='bold', fontproperties=PROP)
    axes[1].legend(loc='lower right', prop=PROP)
    
    if PROP:
        axes[1].set_yticks(range(len(strategies)))
        axes[1].set_yticklabels(strategies, fontproperties=PROP)
    
    for i, v in enumerate(drawdowns):
        axes[1].text(v + 1, i, f'{v:.1f}%', va='center', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    
    if save_path is None:
        save_path = 'backtest_results/strategy_comparison.png'
    
    plt.savefig(save_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    return save_path


def plot_equity_curve(dates: List[str], values: List[float], 
                      strategy_name: str = '', save_path: str = None) -> str:
    """
    绘制资金曲线
    
    Args:
        dates: 日期列表
        values: 资金列表
        strategy_name: 策略名称
        save_path: 保存路径
    
    Returns:
        保存的文件路径
    """
    if not dates or not values:
        return ""
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(range(len(values)), values, linewidth=2, color='#3498db', label='账户净值')
    ax.fill_between(range(len(values)), values, alpha=0.3, color='#3498db')
    
    # 计算回撤
    peak = values[0]
    drawdowns = []
    for v in values:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100 if peak > 0 else 0
        drawdowns.append(dd)
    
    # 绘制回撤
    ax2 = ax.twinx()
    ax2.fill_between(range(len(drawdowns)), drawdowns, alpha=0.2, color='#e74c3c', label='回撤')
    ax2.set_ylabel('回撤 (%)', fontsize=12, fontproperties=PROP, color='#e74c3c')
    
    # 设置x轴标签
    n = len(dates)
    if n > 10:
        step = n // 6
        ax.set_xticks(range(0, n, step))
        ax.set_xticklabels([dates[i] for i in range(0, n, step)], rotation=45)
    
    ax.set_xlabel('时间', fontsize=12, fontproperties=PROP)
    ax.set_ylabel('账户净值', fontsize=12, fontproperties=PROP)
    ax.set_title(f'{strategy_name} 资金曲线' if strategy_name else '资金曲线', 
                fontsize=14, fontweight='bold', fontproperties=PROP)
    ax.legend(loc='upper left', prop=PROP)
    ax2.legend(loc='upper right', prop=PROP)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if save_path is None:
        save_path = 'backtest_results/equity_curve.png'
    
    plt.savefig(save_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    return save_path


def generate_summary_image(results: List[Dict], save_path: str = None) -> str:
    """
    生成汇总图表（包含表格）
    
    Args:
        results: 回测结果
        save_path: 保存路径
    
    Returns:
        保存的文件路径
    """
    if not results:
        return ""
    
    fig, ax = plt.subplots(figsize=(12, 4 + len(results) * 0.5))
    ax.axis('off')
    
    # 表头
    headers = ['策略', '总收益', '年化收益', '最大回撤', '交易次数']
    
    # 数据
    rows = []
    for r in results:
        name = r.get('strategy', r.get('name', 'Unknown'))
        total = f"{r.get('total_return', 0):+.2f}%"
        annual = f"{r.get('annual_return', 0):+.2f}%"
        dd = f"{r.get('max_drawdown', 0):.2f}%"
        trades = str(r.get('trade_count', 0))
        rows.append([name, total, annual, dd, trades])
    
    # 创建表格
    table = ax.table(cellText=rows, colLabels=headers, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1.2, 1.8)
    
    # 设置表头样式
    for i in range(len(headers)):
        table[(0, i)].set_facecolor('#3498db')
        table[(0, i)].set_text_props(color='white', fontweight='bold')
    
    # 设置行样式
    for i in range(1, len(rows) + 1):
        for j in range(len(headers)):
            if i % 2 == 0:
                table[(i, j)].set_facecolor('#f8f9fa')
            
            # 收益列着色
            if j in [1, 2]:
                val = rows[i-1][j].replace('%', '').replace('+', '')
                try:
                    v = float(val)
                    if v > 0:
                        table[(i, j)].set_text_props(color='#2ecc71')
                    elif v < 0:
                        table[(i, j)].set_text_props(color='#e74c3c')
                except:
                    pass
            
            # 回撤列着色
            if j == 3:
                val = rows[i-1][j].replace('%', '')
                try:
                    v = float(val)
                    if v > 40:
                        table[(i, j)].set_text_props(color='#e74c3c')
                    elif v > 30:
                        table[(i, j)].set_text_props(color='#f39c12')
                except:
                    pass
    
    ax.set_title('回测结果汇总', fontsize=16, fontweight='bold', fontproperties=PROP, pad=20)
    
    plt.tight_layout()
    
    if save_path is None:
        save_path = 'backtest_results/summary.png'
    
    plt.savefig(save_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    return save_path


if __name__ == '__main__':
    # 测试
    test_results = [
        {'strategy': '动量策略', 'total_return': 37.66, 'annual_return': 6.60, 'max_drawdown': 29.5, 'trade_count': 1529},
        {'strategy': '突破策略', 'total_return': 60.0, 'annual_return': 10.0, 'max_drawdown': 30.0, 'trade_count': 800},
        {'strategy': '均线策略', 'total_return': -10.13, 'annual_return': -2.11, 'max_drawdown': 36.58, 'trade_count': 967},
        {'strategy': 'MACD策略', 'total_return': -14.92, 'annual_return': -3.18, 'max_drawdown': 36.81, 'trade_count': 1179},
    ]
    
    print("生成策略对比图...")
    path1 = plot_strategy_comparison(test_results)
    print(f"  -> {path1}")
    
    print("生成汇总图...")
    path2 = generate_summary_image(test_results)
    print(f"  -> {path2}")
    
    print("完成!")
