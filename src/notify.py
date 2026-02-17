#!/usr/bin/env python3
"""
飞书消息推送模块
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import requests

logger = logging.getLogger(__name__)


class FeishuNotifier:
    """
    飞书消息推送
    """
    
    def __init__(self, webhook_url: str = None):
        """
        初始化
        
        Args:
            webhook_url: 飞书机器人Webhook地址
        """
        # 从环境变量获取
        self.webhook_url = webhook_url or os.environ.get('FEISHU_WEBHOOK_URL')
        self.session = requests.Session()
    
    def send_text(self, text: str) -> bool:
        """
        发送文本消息
        
        Args:
            text: 文本内容
        
        Returns:
            是否成功
        """
        if not self.webhook_url:
            logger.warning("未配置飞书Webhook URL")
            return False
        
        payload = {
            "msg_type": "text",
            "content": {
                "text": text
            }
        }
        
        try:
            response = self.session.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    logger.info("飞书消息发送成功")
                    return True
                else:
                    logger.error(f"飞书API错误: {result}")
            else:
                logger.error(f"HTTP错误: {response.status_code}")
                
        except Exception as e:
            logger.error(f"发送失败: {e}")
        
        return False
    
    def send_rich_text(self, title: str, content: List[tuple]) -> bool:
        """
        发送富文本消息
        
        Args:
            title: 标题
            content: 内容 [(type, text), ...]
                    type: "text", "tag"
        
        Returns:
            是否成功
        """
        if not self.webhook_url:
            logger.warning("未配置飞书Webhook URL")
            return False
        
        # 构建markdown
        md_parts = [f"## {title}\n"]
        
        for item in content:
            if len(item) >= 2:
                md_parts.append(f"- **{item[0]}**: {item[1]}")
            else:
                md_parts.append(f"- {item[0]}")
        
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": title
                    },
                    "template": "blue"
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": "\n".join(md_parts)
                    }
                ]
            }
        }
        
        try:
            response = self.session.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    logger.info("飞书富文本消息发送成功")
                    return True
                    
        except Exception as e:
            logger.error(f"发送失败: {e}")
        
        return False
    
    def send_card(self, title: str, fields: Dict[str, str], 
                  template: str = "blue") -> bool:
        """
        发送卡片消息
        
        Args:
            title: 标题
            fields: 字段 dict
            template: 模板颜色 (blue/green/red/grey)
        
        Returns:
            是否成功
        """
        if not self.webhook_url:
            logger.warning("未配置飞书Webhook URL")
            return False
        
        # 构建元素
        elements = []
        for key, value in fields.items():
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**{key}**: {value}"
                }
            })
        
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": title
                    },
                    "template": template
                },
                "elements": elements
            }
        }
        
        try:
            response = self.session.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"发送失败: {e}")
            return False


class TradingDayChecker:
    """
    交易日判断
    """
    
    def __init__(self):
        self._cache = None
        self._cache_date = None
    
    def is_trading_day(self, date: datetime = None) -> bool:
        """
        判断是否为交易日
        
        Args:
            date: 日期，默认今天
        
        Returns:
            是否交易日
        """
        date = date or datetime.now()
        
        # 先检查缓存
        if self._cache_date == date.strftime('%Y-%m-%d'):
            return self._cache
        
        # 1. 排除周末
        if date.weekday() >= 5:  # 周六=5, 周日=6
            return False
        
        # 2. 检查是否为节假日
        if self._is_holiday(date):
            return False
        
        return True
    
    def _is_holiday(self, date: datetime) -> bool:
        """检查是否为节假日（简化版）"""
        # 固定节假日（农历会有偏差，这里用公历）
        holidays = [
            # 2026年
            '2026-01-01',  # 元旦
            '2026-02-10', '2026-02-11', '2026-02-12', '2026-02-13', '2026-02-14', '2026-02-15', '2026-02-16',  # 春节
            '2026-04-04', '2026-04-05', '2026-04-06',  # 清明
            '2026-05-01', '2026-05-02', '2026-05-03',  # 劳动节
            '2026-06-20', '2026-06-21', '2026-06-22',  # 端午
            '2026-09-25', '2026-09-26', '2026-09-27',  # 中秋
            '2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05', '2026-10-06', '2026-10-07',  # 国庆
        ]
        
        date_str = date.strftime('%Y-%m-%d')
        
        if date_str in holidays:
            return True
        
        return False
    
    def get_next_trading_day(self, date: datetime = None) -> datetime:
        """
        获取下一个交易日
        
        Args:
            date: 起始日期
        
        Returns:
            下一个交易日
        """
        date = date or datetime.now()
        
        while True:
            date = date + timedelta(days=1)
            if self.is_trading_day(date):
                return date
    
    def get_prev_trading_day(self, date: datetime = None) -> datetime:
        """
        获取上一个交易日
        
        Args:
            date: 起始日期
        
        Returns:
            上一个交易日
        """
        date = date or datetime.now()
        
        while True:
            date = date - timedelta(days=1)
            if self.is_trading_day(date):
                return date


def test():
    """测试"""
    checker = TradingDayChecker()
    
    today = datetime.now()
    print(f"今天: {today.strftime('%Y-%m-%d %A')}")
    print(f"是否为交易日: {checker.is_trading_day(today)}")
    print(f"上一个交易日: {checker.get_prev_trading_day(today).strftime('%Y-%m-%d')}")
    print(f"下一个交易日: {checker.get_next_trading_day(today).strftime('%Y-%m-%d')}")


if __name__ == "__main__":
    test()
