#!/usr/bin/env python3
"""
配置管理模块
Configuration Management
"""

import os
import yaml
from typing import Any, Dict, Optional
from pathlib import Path


class Config:
    """配置管理类"""
    
    _instance: Optional['Config'] = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._config:
            self.load()
    
    def load(self, config_path: str = None):
        """加载配置文件"""
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__), 
                '..', 'config', 'trading.yaml'
            )
        
        config_path = Path(config_path).resolve()
        
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self._config = yaml.safe_load(f)
        
        return self._config
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置项，支持点号分隔的多级键"""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """设置配置项"""
        keys = key.split('.')
        config = self._config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    @property
    def initial_capital(self) -> float:
        return self.get('initial_capital', 1000000)
    
    @property
    def commission_rate(self) -> float:
        return self.get('commission_rate', 0.0003)
    
    @property
    def stamp_tax(self) -> float:
        return self.get('stamp_tax', 0.001)
    
    @property
    def slippage(self) -> float:
        return self.get('slippage', 0.0005)
    
    @property
    def max_position(self) -> float:
        return self.get('max_position', 0.3)
    
    @property
    def max_loss_per_trade(self) -> float:
        return self.get('max_loss_per_trade', 0.02)
    
    @property
    def max_loss_per_day(self) -> float:
        return self.get('max_loss_per_day', 0.05)
    
    @property
    def stop_loss_pct(self) -> float:
        return self.get('stop_loss_pct', 0.03)
    
    @property
    def take_profit_pct(self) -> float:
        return self.get('take_profit_pct', 0.06)
    
    @property
    def stock_pool(self) -> list:
        return self.get('stock_pool', [])


# 全局配置实例
config = Config()
