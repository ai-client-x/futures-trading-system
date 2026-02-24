# 股票量化交易系统

A股量化交易系统，支持实盘交易和历史回测。

## 目录结构

```
├── run_trading.py          # 实盘交易入口
├── daily_report.py         # 每日股票报告
├── morning_report.py       # 早报
├── requirements.txt        # Python依赖
├── src/
│   ├── unified_backtest.py # 回测入口（与实盘流程一致）
│   ├── config.py           # 配置
│   ├── market_regime.py    # 市场环境识别
│   ├── signal_strength.py  # 26策略评分
│   ├── fundamentals/       # 基本面筛选
│   ├── strategies/         # 26个交易策略
│   ├── engines/            # 交易/回测引擎
│   ├── risk/               # 风险管理
│   └── data/               # 数据获取
├── scripts/                # 数据脚本
│   ├── download_fundamentals_history.py
│   ├── download_missing_data.py
│   ├── fix_amount_data.py
│   └── ...
├── data/                   # 数据目录
│   └── stocks.db           # 股票数据库
└── config/                 # 配置文件
```

## 快速开始

### 安装依赖

```bash
pip install -r requirements.txt
```

### 实盘交易

```bash
# 开盘前准备
python run_trading.py --mode pre

# 盘中监控（待实现）
python run_trading.py --mode live
```

### 回测

```bash
# 2019年Q4回测
python src/unified_backtest.py --start 20191001 --end 20191231

# 2016-2019完整回测
python src/unified_backtest.py --start 20160101 --end 20191231
```

### 每日报告

```bash
python daily_report.py
```

## 交易流程（与实盘一致）

### 开盘前流程

```
1. 获取活跃股票列表 (60天日均成交额 >= 3000万)
2. 基本面筛选 (PE<25, ROE>10%, 股息率>1%, 负债<70%, 市值>30亿)
3. 识别市场环境 (牛市/熊市/震荡市) - 每天检测
   3.1. 环境变化时卖出不适用新环境的持仓
4. 检查持仓情况
   4.1. 满仓(>90%) -> 不操作
   4.2. 未满仓 ->
       4.2.1. 选择1个最优策略
       4.2.2. 计算信号分数，排序
       4.2.3. 每有10%未持仓，买入1只
```

### 开盘时流程

```
实盘: tick数据检查
- 卖出: 止盈/止损/策略信号（T+1限制）
- 加仓: 满足加仓条件

回测: 日线数据检查
- 卖出: 最高/低价满足卖出条件（T+1限制）
- 加仓: 满足加仓条件
```

## 26个策略（按市场环境分组）

| 市场 | 策略数 | 策略列表 |
|------|--------|----------|
| **牛市** | 14个 | 成交量突破、MACD+成交量、MACD策略、突破前高、均线发散、量价齐升、RSI趋势、趋势过滤、均线策略、均线交叉强度、收盘站均线、成交量+均线、突破确认、平台突破 |
| **熊市** | 6个 | 动量反转、威廉指标、RSI逆势、双底形态、缩量回调、MACD背离 |
| **震荡市** | 6个 | 布林带、RSI+均线、布林带+RSI、支撑阻力、波动率突破、均线收复 |

## 参数配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| 止盈 | 20% | 达到20%收益自动卖出 |
| 止损 | 10% | 亏损10%自动卖出 |
| 信号阈值 | 40分 | 低于40分不买入 |
| 最大持仓 | 10只 | 最多同时持有10只 |
| 单只仓位 | 10% | 每只股票10%仓位 |

## 数据库

数据存储在 `data/stocks.db`，包含：

- `daily` - 股票日线数据 (2015-2025)
- `fundamentals` - 当前基本面数据
- `fundamentals_history` - 历史基本面数据 (2015-2019)
