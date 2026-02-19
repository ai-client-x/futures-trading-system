#!/usr/bin/env python3
"""
多策略对比回测 - 动态选股池版
模拟实盘：每日/每周检查选股池，动态更新

更新历史:
- 2026-02-18: 添加自适应市场状态功能、分批止盈、金字塔加仓
"""

import os
import sys
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from enum import Enum

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# 自适应市场状态功能 - 2026-02-18 添加
# ============================================================
class MarketRegime(Enum):
    BULL = "bull"      # 牛市
    BEAR = "bear"      # 熊市
    SIDEWAYS = "sideways"  # 震荡市

# 策略参数配置
STRATEGY_PARAMS = {
    MarketRegime.BULL: {'add_on_rise': 0.05, 'max_layers': 2, 'add_ratio': [0.5, 1.0]},
    MarketRegime.BEAR: {'add_on_drop': 0.05, 'max_layers': 2, 'add_ratio': [1.0, 0.5]},
    MarketRegime.SIDEWAYS: {'add_on_drop': 0.08, 'max_layers': 1, 'add_ratio': [0.5]},
}

# 止盈止损参数
TP1, TP2, TP3 = 0.10, 0.15, 0.20
SL = 0.05

# 分批止损参数
SL1, SL2, SL3 = 0.03, 0.05, 0.08  # -3%, -5%, -8%

# ============================================================
# 动态仓位管理 - 2026-02-19 添加
# ============================================================
# 现金储备比例 (保留部分现金作为安全垫)
CASH_RESERVE_RATIO = 0.20  # 保留20%现金

def calculate_max_positions(capital: float) -> int:
    """根据资金量动态计算持仓数量
    
    规则:
    - 10万以下: 1-2只, 每只15-20%
    - 10-50万: 2-3只, 每只15-20%
    - 50-100万: 3-5只, 每只10-15%
    - 100万+: 5-8只, 每只8-12%
    """
    if capital < 100000:
        return 1
    elif capital < 500000:
        return 2
    elif capital < 1000000:
        return 3
    elif capital < 2000000:
        return 5
    else:
        return 6


def calculate_position_size(capital: float, num_positions: int, signal_strength: float = 1.0) -> float:
    """计算单只股票买入金额
    
    Args:
        capital: 当前总资金
        num_positions: 计划持仓数
        signal_strength: 信号强度 (0.5-1.5), 越强买的越多
    
    Returns:
        单只股票买入金额
    """
    # 可用资金 = 总资金 × (1 - 现金储备比例)
    available_capital = capital * (1 - CASH_RESERVE_RATIO)
    
    # 基础每只仓位
    base_per_position = available_capital / max(num_positions, 1)
    
    # 根据信号强度调整
    adjusted = base_per_position * signal_strength
    
    # 上限不超过可用资金的50%
    return min(adjusted, available_capital * 0.5)


def get_signal_strength(df: pd.DataFrame) -> float:
    """计算信号强度 (0.5-1.5)
    
    考虑因素:
    - MA5与MA20的距离 (差距越大越强)
    - 成交量放大
    - 均线多头排列
    """
    if df is None or len(df) < 60:
        return 1.0
    
    close = df['close']
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    
    # MA5与MA20的距离
    if ma20.iloc[-1] > 0:
        distance = (ma5.iloc[-1] - ma20.iloc[-1]) / ma20.iloc[-1]
    else:
        distance = 0
    
    # 距离越大信号越强 (范围: 0.5 - 1.5)
    strength = 1.0 + max(-0.5, min(0.5, distance))
    
    return strength


class TradingCosts:
    """交易成本"""
    COMMISSION = config.commission_rate
    MIN_COMM = 5
    STAMP = config.stamp_tax
    TRANSFER = 0.00002
    SLIPPAGE = config.slippage
    
    @classmethod
    def buy_cost(cls, amount):
        comm = max(amount * cls.COMMISSION, cls.MIN_COMM)
        return comm + amount * cls.TRANSFER + amount * cls.SLIPPAGE
    
    @classmethod
    def sell_cost(cls, amount):
        comm = max(amount * cls.COMMISSION, cls.MIN_COMM)
        return comm + amount * cls.STAMP + amount * cls.TRANSFER + amount * cls.SLIPPAGE


class DynamicBacktest:
    """动态选股池回测"""
    
    def __init__(self):
        self.db_path = DB_PATH
        self.initial_capital = INITIAL_CAPITAL
        
        # 基本面筛选条件
        self.conditions = {
            'max_pe': config.max_pe,
            'min_roe': config.min_roe,
            'min_dv_ratio': config.min_dv_ratio,
            'max_debt': config.max_debt,
            'min_market_cap': config.min_market_cap
        }
        
        logger.info(f"基本面筛选条件: {self.conditions}")
    
    def _conn(self):
        return sqlite3.connect(self.db_path)
    
    def get_stock_pool(self, date: str, industry_diversified: bool = False) -> List[Dict]:
        """
        获取指定日期的选股池
        从数据库动态读取
        
        Args:
            date: 日期
            industry_diversified: 是否行业分散
        """
        conn = self._conn()
        
        if industry_diversified:
            # 行业分散选股
            query = f"""
                SELECT ts_code, name, close, pe, roe, dv_ratio, 
                       debt_to_assets, market_cap, industry
                FROM fundamentals
                WHERE pe > 0 AND pe <= {self.conditions['max_pe']}
                  AND roe >= {self.conditions['min_roe']}
                  AND dv_ratio >= {self.conditions['min_dv_ratio']}
                  AND debt_to_assets <= {self.conditions['max_debt']}
                  AND market_cap >= {self.conditions['min_market_cap']}
                  AND industry IS NOT NULL
                ORDER BY roe DESC
                LIMIT 200
            """
        else:
            query = f"""
                SELECT ts_code, name, close, pe, roe, dv_ratio, 
                       debt_to_assets, market_cap
                FROM fundamentals
                WHERE pe > 0 AND pe <= {self.conditions['max_pe']}
                  AND roe >= {self.conditions['min_roe']}
                  AND dv_ratio >= {self.conditions['min_dv_ratio']}
                  AND debt_to_assets <= {self.conditions['max_debt']}
                  AND market_cap >= {self.conditions['min_market_cap']}
                ORDER BY roe DESC
                LIMIT 50
            """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df is None or len(df) == 0:
            return []
        
        if industry_diversified:
            # 行业分散：每个行业最多2只，最多15个行业
            selected = []
            industries = set()
            
            for _, row in df.iterrows():
                ind = row.get('industry', '')
                cnt = len([s for s in selected if s.get('industry', '') == ind])
                
                if ind and (ind not in industries or cnt < 2):
                    selected.append(row.to_dict())
                    industries.add(ind)
                    if len(industries) >= 15:
                        break
            
            return selected
        
        return df.to_dict('records')
    
    def get_market_regime(self, date: str) -> MarketRegime:
        """根据个股多头比例判断市场状态"""
        conn = self._conn()
        
        # 获取当天有数据的股票
        df = pd.read_sql(f"""
            SELECT ts_code FROM daily 
            WHERE trade_date = '{date}'
        """, conn)
        
        if len(df) < 10:
            conn.close()
            return MarketRegime.SIDEWAYS
        
        # 随机选20只股票计算多头比例
        sample_codes = df.sample(min(20, len(df)), random_state=hash(date) % 1000000)['ts_code'].tolist()
        
        bullish_count = 0
        for code in sample_codes:
            price_df = pd.read_sql(f"""
                SELECT close FROM daily 
                WHERE ts_code = '{code}' AND trade_date <= '{date}'
                ORDER BY trade_date DESC LIMIT 60
            """, conn)
            
            if len(price_df) >= 60:
                ma5 = price_df['close'].iloc[-5:].mean()
                ma20 = price_df['close'].iloc[-20:].mean()
                if ma5 > ma20:
                    bullish_count += 1
        
        conn.close()
        
        ratio = bullish_count / len(sample_codes)
        
        if ratio >= 0.6:
            return MarketRegime.BULL
        elif ratio <= 0.3:
            return MarketRegime.BEAR
        else:
            return MarketRegime.SIDEWAYS
    
    def get_price_series(self, code: str, start: str, end: str) -> Optional[pd.DataFrame]:
        """获取价格序列"""
        conn = self._conn()
        
        df = pd.read_sql(f"""
            SELECT trade_date, open, high, low, close, vol
            FROM daily
            WHERE ts_code = '{code}'
              AND trade_date >= '{start}'
              AND trade_date <= '{end}'
            ORDER BY trade_date
        """, conn)
        
        conn.close()
        
        if df is not None and len(df) > 0:
            df = df.rename(columns={'vol': 'Volume'})
        
        return df
    
    def get_latest_price(self, code: str, date: str) -> Optional[float]:
        """获取指定日期收盘价"""
        conn = self._conn()
        
        df = pd.read_sql(f"""
            SELECT close FROM daily
            WHERE ts_code = '{code}' AND trade_date <= '{date}'
            ORDER BY trade_date DESC LIMIT 1
        """, conn)
        
        conn.close()
        
        return df.iloc[0]['close'] if df is not None and len(df) > 0 else None
    
    # ============ 策略信号 ============
    def check_ma_signal(self, df: pd.DataFrame) -> str:
        """均线策略信号"""
        if df is None or len(df) < 60:
            return 'hold'
        
        close = df['close']
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        
        if len(df) < 2:
            return 'hold'
        
        if ma5.iloc[-1] > ma20.iloc[-1] and ma5.iloc[-2] <= ma20.iloc[-2]:
            return 'buy'
        if ma5.iloc[-1] < ma20.iloc[-1] and ma5.iloc[-2] >= ma20.iloc[-2]:
            return 'sell'
        return 'hold'
    
    def check_macd_signal(self, df: pd.DataFrame) -> str:
        """MACD策略信号"""
        if df is None or len(df) < 60:
            return 'hold'
        
        close = df['close']
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9).mean()
        
        if len(df) < 2:
            return 'hold'
        
        if macd.iloc[-1] > signal.iloc[-1] and macd.iloc[-2] <= signal.iloc[-2]:
            return 'buy'
        if macd.iloc[-1] < signal.iloc[-1] and macd.iloc[-2] >= signal.iloc[-2]:
            return 'sell'
        return 'hold'
    
    def check_momentum_signal(self, df: pd.DataFrame) -> str:
        """动量策略信号"""
        if df is None or len(df) < 30:
            return 'hold'
        
        close = df['close']
        mom = close / close.shift(20) - 1
        
        if mom.iloc[-1] > 0.1:
            return 'buy'
        if mom.iloc[-1] < -0.05:
            return 'sell'
        return 'hold'
    
    def check_breakout_signal(self, df: pd.DataFrame) -> str:
        """突破策略信号 - 均线交叉"""
        if df is None or len(df) < 20:
            return 'hold'
        
        close = df['close']
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        
        if ma5.iloc[-1] > ma20.iloc[-1]:
            return 'buy'
        if ma5.iloc[-1] < ma20.iloc[-1]:
            return 'sell'
        return 'hold'
    
    def run(self, strategy_name: str, signal_func, start_date: str, end_date: str) -> Dict:
        """运行回测 - 支持多持仓"""
        logger.info(f"运行策略: {strategy_name}")
        
        capital = self.initial_capital
        positions = []  # 多持仓列表
        max_positions = calculate_max_positions(capital)  # 动态持仓数
        trades = []
        current_pool = []
        last_pool_date = None
        
        # 最大回撤
        peak_value = self.initial_capital
        max_drawdown = 0
        equity_curve = []  # 资金曲线
        
        # 获取所有交易日
        conn = self._conn()
        trade_dates = pd.read_sql(f"""
            SELECT DISTINCT trade_date FROM daily
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            ORDER BY trade_date
        """, conn)['trade_date'].tolist()
        conn.close()
        
        for i, date in enumerate(trade_dates):
            month = date[:6]
            if month != last_pool_date:
                current_pool = self.get_stock_pool(date)
                last_pool_date = month
            
            # === 市场状态检测 (每周检测一次) ===
            if i % 5 == 0:
                current_regime = self.get_market_regime(date)
            else:
                current_regime = current_regime if 'current_regime' in dir() else MarketRegime.SIDEWAYS
            
            if not current_pool:
                continue
            
            # === 卖出检查 ===
            import logging
            if positions:
                logging.info(f"{date} 检查卖出: 持仓={len(positions)}只")
            
            to_sell = []
            for pos in list(positions):
                df = self.get_price_series(pos['code'], start_date, date)
                if df is not None and len(df) >= 20:
                    signal = signal_func(df)
                    price = df.iloc[-1]['close']
                    
                    # 计算当前收益率
                    curr_ret = (price - pos['cost']) / pos['cost']
                    
                    # 分批止盈检测（每个止盈点只触发一次）
                    sell_qty = 0
                    reason = ''
                    
                    if curr_ret >= TP3:  # 涨20%清仓
                        sell_qty = pos['qty']
                        reason = '止盈20%清仓'
                    elif curr_ret >= TP2 and not pos.get('tp15_triggered', False):
                        sell_qty = int(pos['qty'] * 0.6)
                        reason = '止盈15%卖出60%'
                        pos['tp15_triggered'] = True
                    elif curr_ret >= TP1 and not pos.get('tp10_triggered', False):
                        sell_qty = int(pos['qty'] * 0.3)
                        reason = '止盈10%卖出30%'
                        pos['tp10_triggered'] = True
                    elif curr_ret <= -SL3:  # 止损8%清仓
                        sell_qty = pos['qty']
                        reason = '止损8%清仓'
                    elif curr_ret <= -SL2 and not pos.get('sl5_triggered', False):
                        sell_qty = int(pos['qty'] * 0.6)
                        reason = '止损5%卖出60%'
                        pos['sl5_triggered'] = True
                    elif curr_ret <= -SL1 and not pos.get('sl3_triggered', False):
                        sell_qty = int(pos['qty'] * 0.3)
                        reason = '止损3%卖出30%'
                        pos['sl3_triggered'] = True
                    elif signal == 'sell':  # 死叉卖出
                        sell_qty = pos['qty']
                        reason = '死叉卖出'
                    
                    if sell_qty > 0:
                        logging.info(f"{date} 卖出 {pos['code']}: 价格={price:.2f}, 成本={pos['cost']:.2f}, 原因={reason}")
                        to_sell.append((pos, price, sell_qty, reason))
            
            for pos, price, sell_qty, reason in to_sell:
                revenue = price * sell_qty - TradingCosts.sell_cost(price * sell_qty)
                cost = pos['cost'] * sell_qty
                pnl = revenue - cost
                capital += revenue
                trades.append({'date': date, 'action': 'sell', 'price': price, 'code': pos['code'], 'reason': reason, 'pnl': pnl})
                # 如果只是部分卖出，更新持仓数量
                remaining = pos['qty'] - sell_qty
                if remaining > 0:
                    pos['qty'] = remaining
                else:
                    positions = [p for p in positions if p['code'] != pos['code']]
            
            # === 买入检查 ===
            # 根据资金量动态计算可买入数量
            max_positions = calculate_max_positions(capital)
            
            if len(positions) < max_positions and capital > 0:
                # 获取信号强度
                signal_strength = get_signal_strength(df) if 'df' in dir() and df is not None else 1.0
                
                for stock in current_pool[:15]:
                    if len(positions) >= max_positions:
                        break
                    code = stock['ts_code']
                    if any(p['code'] == code for p in positions):
                        continue
                    df = self.get_price_series(code, start_date, date)
                    if df is not None and len(df) >= 60:
                        signal = signal_func(df)
                        if signal == 'buy':
                            # 计算信号强度
                            strength = get_signal_strength(df)
                            # 计算买入金额
                            alloc = calculate_position_size(capital, max_positions - len(positions), strength)
                            
                            price = df.iloc[-1]['close']
                            qty = int(alloc / price / 100) * 100
                            if qty > 0:
                                cost = price * qty + TradingCosts.buy_cost(price * qty)
                                # 检查是否超过可用资金(保留现金储备)
                                available = capital * (1 - CASH_RESERVE_RATIO)
                                if cost <= available:
                                    capital -= cost
                                    positions.append({
                                        'code': code,
                                        'qty': qty,
                                        'cost': price,
                                        'name': stock.get('name', ''),
                                        'signal_strength': strength  # 记录信号强度
                                    })
                                    trades.append({
                                        'date': date, 'action': 'buy', 'price': price,
                                        'code': code, 'name': stock.get('name', ''),
                                        'signal_strength': strength
                                    })
            
            # === 根据市场状态加仓 ===
            params = STRATEGY_PARAMS[current_regime]
            for pos in positions:
                if pos.get('layers', 1) >= params['max_layers']:
                    continue
                
                df = self.get_price_series(pos['code'], start_date, date)
                if df is not None and len(df) >= 60:
                    curr_price = df.iloc[-1]['close']
                    ret = (curr_price - pos['cost']) / pos['cost']
                    
                    should_add = False
                    if current_regime == MarketRegime.BULL:
                        should_add = ret >= params['add_on_rise']
                    elif current_regime == MarketRegime.BEAR:
                        should_add = ret <= -params['add_on_drop']
                    else:
                        should_add = ret <= -params['add_on_drop']
                    
                    if should_add:
                        layer_idx = pos.get('layers', 1) - 1
                        add_ratio = params['add_ratio'][min(layer_idx, len(params['add_ratio'])-1)]
                        add_qty = int(pos['qty'] * add_ratio)
                        
                        # 检查可用资金(保留现金储备)
                        available = capital * (1 - CASH_RESERVE_RATIO)
                        if add_qty > 0 and curr_price * add_qty * 1.001 <= available:
                            capital -= curr_price * add_qty * 1.001
                            total_cost = pos['cost'] * pos['qty'] + curr_price * add_qty
                            pos['qty'] += add_qty
                            pos['cost'] = total_cost / pos['qty']
                            pos['layers'] = pos.get('layers', 1) + 1
                            # 重置止盈止损触发状态
                            pos['tp10_triggered'] = False
                            pos['tp15_triggered'] = False
                            pos['sl3_triggered'] = False
                            pos['sl5_triggered'] = False
                            trades.append({'date': date, 'action': 'add', 'code': pos['code'], 'reason': f'{current_regime.value}加仓'})
            
            # === 市值计算 ===
            total_value = capital
            for pos in positions:
                price = self.get_latest_price(pos['code'], date)
                if price:
                    total_value += price * pos['qty']
            
            if total_value > peak_value:
                peak_value = total_value
            drawdown = (peak_value - total_value) / peak_value if peak_value > 0 else 0
            if drawdown > max_drawdown:
                max_drawdown = drawdown
            
            # 记录资金曲线
            equity_curve.append((date, total_value))
            
            if (i + 1) % 60 == 0:
                logger.info(f"{date}: 现金={capital:,.0f}, 持仓={len(positions)}只, 总值={total_value:,.0f}, 回撤={drawdown*100:.1f}%")
        
        # 最终清仓
        for pos in list(positions):
            df = self.get_price_series(pos['code'], start_date, end_date)
            if df is not None and len(df) > 0:
                price = df.iloc[-1]['close']
                capital += price * pos['qty'] - TradingCosts.sell_cost(price * pos['qty'])
        
        total_return = (capital - self.initial_capital) / self.initial_capital
        years = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(start_date, '%Y%m%d')).days / 365
        
        # 计算增强统计
        # 1. 胜率
        sells = [t for t in trades if t.get('action') == 'sell']
        if sells:
            wins = sum(1 for t in sells if t.get('pnl', 0) > 0)
            win_rate = wins / len(sells) * 100
        else:
            win_rate = 0
        
        # 2. 平均回撤
        if equity_curve:
            values = [e[1] for e in equity_curve]
            peaks = []
            drawdowns = []
            peak = values[0]
            for v in values:
                if v > peak:
                    peak = v
                dd = (peak - v) / peak * 100 if peak > 0 else 0
                drawdowns.append(dd)
            avg_drawdown = np.mean(drawdowns) if drawdowns else 0
        else:
            avg_drawdown = 0
        
        # 3. 资金利用率
        if equity_curve and len(equity_curve) > 1:
            utils = []
            for i in range(1, len(equity_curve)):
                prev = equity_curve[i-1][1]
                curr = equity_curve[i][1]
                if prev > 0:
                    util = abs((prev - self.initial_capital) / prev)
                    utils.append(util * 100)
            capital_utilization = np.mean(utils) if utils else 0
        else:
            capital_utilization = 0
        
        return {
            'strategy': strategy_name,
            'initial_capital': self.initial_capital,
            'final_assets': capital,
            'total_return': total_return * 100,
            'annual_return': ((1 + total_return) ** (1/years) - 1) * 100 if years > 0 else 0,
            'max_drawdown': max_drawdown * 100,
            'avg_drawdown': avg_drawdown,
            'capital_utilization': capital_utilization,
            'win_rate': win_rate,
            'trade_count': len(trades)
        }



    def run_industry_diversified(self, strategy_name: str, start_date: str, end_date: str) -> Dict:
        """行业分散选股回测"""
        logger.info(f"运行策略: {strategy_name} (行业分散)")
        
        capital = self.initial_capital
        positions = []
        trades = []
        
        conn = self._conn()
        trade_dates = pd.read_sql(f"""
            SELECT DISTINCT trade_date FROM daily
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
            ORDER BY trade_date
        """, conn)['trade_date'].tolist()
        conn.close()
        
        last_month = None
        peak = capital
        max_dd = 0
        
        for i, date in enumerate(trade_dates):
            month = date[:6]
            
            if month != last_month:
                pool = self.get_stock_pool(date, industry_diversified=True)
                last_month = month
            
            if not pool:
                continue
            
            # 卖出
            to_sell = []
            for pos in list(positions):
                df = self.get_price_series(pos['code'], start_date, date)
                if df is not None and len(df) >= 20:
                    signal = self.check_ma_signal(df)
                    price = df.iloc[-1]['close']
                    if (price <= pos['cost'] * 0.97 or price >= pos['cost'] * 1.06 or signal == 'sell'):
                        to_sell.append((pos, price))
            
            for pos, price in to_sell:
                capital += price * pos['qty'] * 0.998
                trades.append({'date': date, 'code': pos['code'], 'action': 'sell'})
                positions = [p for p in positions if p['code'] != pos['code']]
            
            # 买入
            max_pos = calculate_max_positions(capital)
            if len(positions) < max_pos and capital > 0:
                held = set(p['code'] for p in positions)
                candidates = [p for p in pool if p['ts_code'] not in held]
                
                for cand in candidates:
                    if len(positions) >= max_pos:
                        break
                    
                    df = self.get_price_series(cand['ts_code'], start_date, date)
                    if df is not None and len(df) >= 60:
                        signal = self.check_ma_signal(df)
                        if signal == 'buy':
                            price = df.iloc[-1]['close']
                            qty = int(capital / max_pos / price / 100) * 100
                            if qty > 0 and capital > price * qty * 1.001:
                                capital -= price * qty * 1.001
                                positions.append({
                                    'code': cand['ts_code'], 
                                    'qty': qty, 
                                    'cost': price, 
                                    'name': cand.get('name', ''),
                                    'layers': 1,
                                    'tp10_triggered': False,
                                    'tp15_triggered': False,
                                    'sl3_triggered': False,  # -3%止损触发
                                    'sl5_triggered': False,  # -5%止损触发
                                })
                                trades.append({'date': date, 'code': cand['ts_code'], 'action': 'buy'})
            
            # 市值
            total = capital
            for pos in positions:
                price = self.get_latest_price(pos['code'], date)
                if price:
                    total += price * pos['qty']
            
            if total > peak:
                peak = total
            dd = (peak - total) / peak if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd
        
        # 清仓
        for pos in positions:
            price = self.get_latest_price(pos['code'], end_date)
            if price:
                capital += price * pos['qty'] * 0.998
        
        total_return = (capital - self.initial_capital) / self.initial_capital
        
        return {
            'strategy': strategy_name,
            'total_return': total_return * 100,
            'annual_return': total_return * 100 / 5,
            'max_drawdown': max_dd * 100,
            'trade_count': len(trades)
        }


def main():
    print("="*70)
    print("📊 多策略回测 (动态选股池)")
    print("="*70)
    print(f"回测期间: {BACKTEST_START} ~ {BACKTEST_END}")
    print(f"初始资金: ¥{INITIAL_CAPITAL:,}")
    print(f"基本面条件: PE≤{config.max_pe}, ROE≥{config.min_roe}%, 股息≥{config.min_dv_ratio}%, 负债≤{config.max_debt}%, 市值≥{config.min_market_cap}亿")
    print(f"风控: 止损{config.stop_loss_pct*100}%, 止盈{config.take_profit_pct*100}%")
    print("="*70)
    
    engine = DynamicBacktest()
    
    # 测试各个策略
    # 添加行业分散策略
    strategies = [
        ("均线策略", engine.check_ma_signal),
        ("MACD策略", engine.check_macd_signal),
        ("动量策略", engine.check_momentum_signal),
        ("突破策略", engine.check_breakout_signal),
        ("行业分散", "industry_diversified"),  # 特殊标记
    ]
    
    results = {'backtest': {}}
    
    for name, func in strategies:
        if isinstance(func, str) and func == "industry_diversified":
            result = engine.run_industry_diversified(name, BACKTEST_START, BACKTEST_END)
        else:
            result = engine.run(name, func, BACKTEST_START, BACKTEST_END)
        results['backtest'][name] = result
    
    # 打印结果
    print("\n" + "="*70)
    print("📈 回测结果")
    print("="*70)
    
    total_return = 0
    for name, result in results['backtest'].items():
        print(f"  {result['strategy']:10s}: 收益={result['total_return']:>7.2f}%, 年化={result['annual_return']:>7.2f}%, 回撤={result['max_drawdown']:.2f}%, 交易={result['trade_count']}")
        total_return += result['total_return']
    
    avg_return = total_return / len(results['backtest'])
    print(f"  {'综合策略':10s}: 收益={avg_return:>7.2f}%")
    
    print("\n" + "="*70)
    
    # 保存
    filepath = f"backtest_results/dynamic_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    os.makedirs('backtest_results', exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"已保存: {filepath}")


if __name__ == "__main__":
    main()
