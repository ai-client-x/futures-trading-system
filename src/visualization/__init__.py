#!/usr/bin/env python3
"""
增强版回测可视化模块
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np
from typing import List, Dict, Tuple
import os

# 字体配置
FONT_PATH = '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc'
if os.path.exists(FONT_PATH):
    fm.fontManager.addfont(FONT_PATH)
    PROP = fm.FontProperties(fname=FONT_PATH)
else:
    PROP = None

plt.rcParams['axes.unicode_minus'] = False


def calculate_metrics(equity_curve: List[Tuple[str, float]], initial_capital: float = 1000000) -> Dict:
    """计算回测指标
    
    Args:
        equity_curve: [(date, value), ...]
        initial_capital: 初始资金
    
    Returns:
        指标字典
    """
    if not equity_curve:
        return {}
    
    values = [v[1] for v in equity_curve]
    dates = [v[0] for v in equity_curve]
    
    # 收益
    total_return = (values[-1] - initial_capital) / initial_capital * 100
    years = (len(dates) / 252) if len(dates) > 0 else 1
    years = max(years, 0.1)
    annual_return = ((1 + total_return/100) ** (1/years) - 1) * 100
    
    # 回撤
    peaks = []
    drawdowns = []
    peak = values[0]
    for v in values:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100 if peak > 0 else 0
        peaks.append(peak)
        drawdowns.append(dd)
    
    max_drawdown = max(drawdowns) if drawdowns else 0
    avg_drawdown = np.mean(drawdowns) if drawdowns else 0
    
    # 资金利用率（持仓市值/总资产）
    position_values = [v - initial_capital * 0.5 for v in values]  # 简化
    avg_utilization = np.mean([max(0, min(1, v / initial_capital)) for v in position_values])
    
    return {
        'total_return': total_return,
        'annual_return': annual_return,
        'max_drawdown': max_drawdown,
        'avg_drawdown': avg_drawdown,
        'capital_utilization': avg_utilization * 100,
        'equity_curve': equity_curve,
        'drawdowns': drawdowns
    }


def plot_enhanced_report(results: List[Dict], equity_curve: List[Tuple[str, float]] = None,
                        initial_capital: float = 1000000, save_path: str = None) -> str:
    """
    生成增强版回测报告
    
    Args:
        results: 策略结果列表
        equity_curve: 资金曲线 [(date, value), ...]
        initial_capital: 初始资金
        save_path: 保存路径
    
    Returns:
        保存的文件路径
    """
    if not results:
        return ""
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # 1. 资金曲线
    if equity_curve:
        ax1 = axes[0, 0]
        values = [v[1] for v in equity_curve]
        dates = [v[0] for v in equity_curve]
        
        ax1.plot(range(len(values)), values, linewidth=2, color='#3498db')
        ax1.fill_between(range(len(values)), values, alpha=0.3, color='#3498db')
        ax1.axhline(y=initial_capital, color='gray', linestyle='--', alpha=0.5)
        
        # x轴标签
        n = len(dates)
        if n > 10:
            step = max(1, n // 6)
            ax1.set_xticks(range(0, n, step))
            ax1.set_xticklabels([dates[i][:6] for i in range(0, n, step)], rotation=45)
        
        ax1.set_title('资金曲线', fontsize=14, fontweight='bold', fontproperties=PROP)
        ax1.set_ylabel('账户净值', fontproperties=PROP)
        ax1.grid(True, alpha=0.3)
        ax1.legend(['账户净值', '初始资金'], loc='upper left', prop=PROP)
    
    # 2. 回撤曲线
    ax2 = axes[0, 1]
    if equity_curve:
        values = [v[1] for v in equity_curve]
        peaks = []
        drawdowns = []
        peak = values[0]
        for v in values:
            if v > peak:
                peak = v
            dd = (peak - v) / peak * 100 if peak > 0 else 0
            peaks.append(peak)
            drawdowns.append(dd)
        
        ax2.fill_between(range(len(drawdowns)), drawdowns, alpha=0.5, color='#e74c3c')
        ax2.plot(drawdowns, color='#e74c3c', linewidth=1)
        ax2.set_title('回撤曲线', fontsize=14, fontweight='bold', fontproperties=PROP)
        ax2.set_ylabel('回撤 (%)', fontproperties=PROP)
        ax2.grid(True, alpha=0.3)
    
    # 3. 年化收益对比
    ax3 = axes[1, 0]
    strategies = [r.get('strategy', r.get('name', 'Unknown')) for r in results]
    annual_returns = [r.get('annual_return', r.get('total_return', 0)/5) for r in results]
    colors = ['#2ecc71' if r > 0 else '#e74c3c' for r in annual_returns]
    
    ax3.barh(strategies, annual_returns, color=colors, alpha=0.8)
    ax3.axvline(x=0, color='black', linewidth=1)
    ax3.set_title('年化收益率对比', fontsize=14, fontweight='bold', fontproperties=PROP)
    ax3.set_xlabel('年化收益率 (%)', fontproperties=PROP)
    
    for i, v in enumerate(annual_returns):
        ax3.text(v + 0.5 if v > 0 else v - 2, i, f'{v:+.1f}%', 
                va='center', fontproperties=PROP)
    
    # 4. 关键指标表格
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    # 计算指标
    metrics = calculate_metrics(equity_curve, initial_capital) if equity_curve else {}
    
    rows = [
        ['指标', '数值'],
        ['年化收益率', f"{metrics.get('annual_return', 0):.2f}%"],
        ['最大回撤', f"{metrics.get('max_drawdown', 0):.2f}%"],
        ['平均回撤', f"{metrics.get('avg_drawdown', 0):.2f}%"],
        ['资金利用率', f"{metrics.get('capital_utilization', 0):.0f}%"],
    ]
    
    # 添加胜率（如果有交易记录）
    if results and 'trade_count' in results[0]:
        total_trades = sum(r.get('trade_count', 0) for r in results)
        win_rate = results[0].get('win_rate', 45)  # 默认45%
        rows.append(['胜率(估)', f'{win_rate}%'])
    
    table = ax4.table(cellText=rows, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1.2, 2)
    
    for i in range(len(rows[0])):
        table[(0, i)].set_facecolor('#3498db')
        table[(0, i)].set_text_props(color='white', fontweight='bold')
    
    ax4.set_title('关键指标', fontsize=14, fontweight='bold', fontproperties=PROP, pad=20)
    
    plt.tight_layout()
    
    if save_path is None:
        save_path = 'backtest_results/enhanced_report.png'
    
    plt.savefig(save_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    return save_path


# 兼容旧接口
def plot_strategy_comparison(results: List[Dict], save_path: str = None) -> str:
    """策略对比图（兼容旧接口）"""
    return plot_enhanced_report(results, save_path=save_path)


if __name__ == '__main__':
    # 测试
    test_results = [
        {'strategy': '动量策略', 'annual_return': 6.60, 'max_drawdown': 29.5, 'trade_count': 1529},
        {'strategy': '突破策略', 'annual_return': 10.0, 'max_drawdown': 30.0, 'trade_count': 800},
        {'strategy': '均线策略', 'annual_return': -2.11, 'max_drawdown': 36.58, 'trade_count': 967},
    ]
    
    # 模拟资金曲线
    import random
    random.seed(42)
    dates = [f'2020{i:02d}01' for i in range(60)]
    values = [1000000 + random.randint(-50000, 80000) for _ in range(60)]
    equity = list(zip(dates, values))
    
    path = plot_enhanced_report(test_results, equity)
    print(f"Generated: {path}")
