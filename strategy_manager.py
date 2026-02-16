#!/usr/bin/env python3
"""
策略评估与管理系统
自动评估策略表现，决定是否调整参数或替换策略
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict


@dataclass
class StrategyMetrics:
    """策略指标"""
    name: str
    total_return: float      # 总收益率
    annual_return: float     # 年化收益率
    max_drawdown: float      # 最大回撤
    win_rate: float          # 胜率
    trade_count: int         # 交易次数
    sharpe_ratio: float      # 夏普比率（简化版）
    
    @property
    def score(self) -> float:
        """综合评分（0-100）"""
        # 权重：收益40% + 回撤25% + 胜率20% + 交易效率15%
        return (
            min(self.annual_return, 50) / 50 * 40 +  # 年化收益（最高40分）
            (1 - min(self.max_drawdown, 50) / 50) * 25 +  # 回撤（越低越高）
            self.win_rate / 100 * 20 +  # 胜率
            min(self.trade_count, 100) / 100 * 15  # 交易次数
        )


@dataclass
class StrategyRecord:
    """策略记录"""
    name: str
    created_at: str
    status: str              # active, paused, retired
    param_adjust_count: int  # 参数调整次数
    performance: List[Dict]  # 历史表现
    current_params: Dict     # 当前参数


class StrategyManager:
    """策略管理器"""
    
    def __init__(self, config_path: str = "config/strategy_manager.json"):
        self.config_path = config_path
        self.strategies: Dict[str, StrategyRecord] = {}
        self.load_config()
    
    def load_config(self):
        """加载配置"""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                data = json.load(f)
                for name, record in data.get('strategies', {}).items():
                    self.strategies[name] = StrategyRecord(**record)
    
    def save_config(self):
        """保存配置"""
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        data = {
            'strategies': {name: asdict(record) for name, record in self.strategies.items()},
            'last_update': datetime.now().isoformat()
        }
        with open(self.config_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def register_strategy(self, name: str, params: Dict):
        """注册新策略"""
        self.strategies[name] = StrategyRecord(
            name=name,
            created_at=datetime.now().isoformat(),
            status='active',
            param_adjust_count=0,
            performance=[],
            current_params=params
        )
        self.save_config()
        print(f"✅ 注册新策略: {name}")
    
    def record_performance(self, name: str, metrics: StrategyMetrics):
        """记录策略表现"""
        if name not in self.strategies:
            print(f"⚠️ 策略 {name} 未注册")
            return
        
        self.strategies[name].performance.append({
            'date': datetime.now().isoformat(),
            'total_return': metrics.total_return,
            'annual_return': metrics.annual_return,
            'max_drawdown': metrics.max_drawdown,
            'win_rate': metrics.win_rate,
            'trade_count': metrics.trade_count,
            'score': metrics.score
        })
        self.save_config()
    
    def evaluate_strategy(self, name: str, lookback_periods: int = 3) -> Dict:
        """
        评估策略
        
        返回决策:
        - keep: 保持现状
        - adjust_params: 调整参数
        - replace: 替换策略
        """
        if name not in self.strategies:
            return {'decision': 'unknown', 'reason': '策略未注册'}
        
        record = self.strategies[name]
        perf = record.performance
        
        if len(perf) < lookback_periods:
            return {'decision': 'keep', 'reason': '数据不足，继续观察'}
        
        # 取最近的表现
        recent = perf[-lookback_periods:]
        avg_return = sum(p['annual_return'] for p in recent) / len(recent)
        avg_drawdown = sum(p['max_drawdown'] for p in recent) / len(recent)
        avg_score = sum(p['score'] for p in recent) / len(recent)
        
        # 决策规则
        decision = 'keep'
        reason = ''
        
        if avg_return < -10:
            # 严重亏损
            if record.param_adjust_count < 2:
                decision = 'adjust_params'
                reason = f'连续亏损({avg_return:.1f}%)，尝试调整参数(第{record.param_adjust_count+1}次)'
            else:
                decision = 'replace'
                reason = f'调整{record.param_adjust_count}次后仍亏损，决定替换'
        elif avg_return < 0:
            # 小幅亏损
            if avg_drawdown > 20:
                if record.param_adjust_count < 2:
                    decision = 'adjust_params'
                    reason = f'亏损{avg_return:.1f}%且回撤{avg_drawdown:.1f}%，调整参数'
                else:
                    decision = 'replace'
                    reason = '回撤过大且无法改善'
            else:
                reason = '小幅亏损但风控良好，继续观察'
        elif avg_score < 40:
            # 表现一般
            if record.param_adjust_count < 2:
                decision = 'adjust_params'
                reason = f'综合评分{avg_score:.1f}分，尝试优化参数'
            else:
                reason = '评分一般但可接受，保持观察'
        else:
            reason = f'表现优秀(评分{avg_score:.1f})，继续运行'
        
        return {
            'decision': decision,
            'reason': reason,
            'metrics': {
                'avg_return': avg_return,
                'avg_drawdown': avg_drawdown,
                'avg_score': avg_score
            },
            'adjust_count': record.param_adjust_count
        }
    
    def adjust_params(self, name: str, new_params: Dict):
        """调整策略参数"""
        if name not in self.strategies:
            print(f"⚠️ 策略 {name} 未注册")
            return
        
        record = self.strategies[name]
        old_params = record.current_params.copy()
        record.current_params = new_params
        record.param_adjust_count += 1
        
        print(f"🔧 策略 {name} 参数调整:")
        print(f"   旧参数: {old_params}")
        print(f"   新参数: {new_params}")
        
        self.save_config()
    
    def replace_strategy(self, name: str, new_name: str, new_params: Dict):
        """替换策略"""
        if name in self.strategies:
            self.strategies[name].status = 'retired'
            print(f"🗑️ 策略 {name} 已退役")
        
        self.register_strategy(new_name, new_params)
        print(f"✅ 新策略 {new_name} 已注册")
    
    def get_all_strategies(self) -> List[Dict]:
        """获取所有策略状态"""
        result = []
        for name, record in self.strategies.items():
            latest_perf = record.performance[-1] if record.performance else {}
            result.append({
                'name': name,
                'status': record.status,
                'adjust_count': record.param_adjust_count,
                'score': latest_perf.get('score', 0),
                'annual_return': latest_perf.get('annual_return', 0),
                'current_params': record.current_params
            })
        return result
    
    def generate_report(self) -> str:
        """生成策略评估报告"""
        lines = ["="*60, "📊 策略评估报告", "="*60, ""]
        
        for name, record in self.strategies.items():
            eval_result = self.evaluate_strategy(name)
            
            lines.append(f"策略: {name}")
            lines.append(f"  状态: {record.status}")
            lines.append(f"  调整次数: {record.param_adjust_count}")
            lines.append(f"  决策: {eval_result['decision']}")
            lines.append(f"  原因: {eval_result['reason']}")
            
            if 'metrics' in eval_result:
                m = eval_result['metrics']
                lines.append(f"  平均年化: {m['avg_return']:.2f}%")
                lines.append(f"  平均回撤: {m['avg_drawdown']:.2f}%")
                lines.append(f"  综合评分: {m['avg_score']:.1f}")
            lines.append("")
        
        return "\n".join(lines)


# ============ 示例用法 ============

def example():
    """示例"""
    manager = StrategyManager()
    
    # 注册4个策略
    strategies = {
        '趋势跟踪': {'max_positions': 3, 'stop_loss': -7, 'take_profit': 15},
        '价值投资': {'max_positions': 3, 'stop_loss': -10, 'take_profit': 20},
        '动量策略': {'max_positions': 2, 'stop_loss': -5, 'take_profit': 12},
        '突破策略': {'max_positions': 2, 'stop_loss': -4, 'take_profit': 10}
    }
    
    for name, params in strategies.items():
        manager.register_strategy(name, params)
    
    # 模拟记录表现（实际使用时从回测结果读取）
    mock_results = {
        '趋势跟踪': {'total_return': 36.49, 'annual_return': 18.25, 'max_drawdown': 15.2, 'win_rate': 38.5, 'trade_count': 45, 'sharpe_ratio': 1.2},
        '价值投资': {'total_return': 42.63, 'annual_return': 21.32, 'max_drawdown': 12.5, 'win_rate': 42.0, 'trade_count': 38, 'sharpe_ratio': 1.5},
        '动量策略': {'total_return': 35.18, 'annual_return': 17.59, 'max_drawdown': 18.7, 'win_rate': 35.0, 'trade_count': 52, 'sharpe_ratio': 0.9},
        '突破策略': {'total_return': 49.21, 'annual_return': 24.61, 'max_drawdown': 14.3, 'win_rate': 40.0, 'trade_count': 48, 'sharpe_ratio': 1.6}
    }
    
    for name, metrics in mock_results.items():
        manager.record_performance(name, StrategyMetrics(name=name, **metrics))
    
    # 评估所有策略
    print(manager.generate_report())
    
    # 评估单个策略
    print("\n" + "="*60)
    print("📋 策略 '突破策略' 评估结果:")
    print("="*60)
    result = manager.evaluate_strategy('突破策略')
    print(f"决策: {result['decision']}")
    print(f"原因: {result['reason']}")


if __name__ == "__main__":
    example()
