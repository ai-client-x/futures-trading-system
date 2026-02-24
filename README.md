# 股票量化交易系统

A股量化交易系统，支持实盘交易和历史回测。

## 目录结构

```
├── run_trading.py          # 实盘交易入口
├── daily_report.py         # 每日股票报告
├── morning_report.py       # 早报
├── requirements.txt        # Python依赖
├── src/
│   ├── unified_backtest.py # 回测入口
│   ├── run_trading.py       # 实盘交易
│   ├── config.py            # 配置
│   ├── market_regime.py     # 市场环境识别
│   ├── signal_strength.py  # 26策略评分
│   ├── fundamentals/        # 基本面筛选
│   ├── strategies/          # 26个交易策略
│   ├── engines/             # 交易/回测引擎
│   ├── risk/                # 风险管理
│   └── data/                # 数据获取
├── scripts/                 # 数据脚本
│   ├── download_fundamentals_history.py  # 下载历史基本面
│   ├── download_missing_data.py           # 补充历史日线
│   ├── fix_amount_data.py                 # 修复成交额数据
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
python run_trading.py
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
# 生成当日候选股票报告
python daily_report.py
```

## 策略说明

系统包含26个技术指标策略，根据市场环境动态选择：

- **牛市** (14个)：成交量突破、MACD金叉、突破前高、均线发散等
- **熊市** (6个)：动量反转、威廉指标超卖、RSI逆势等
- **震荡市** (6个)：布林带、RSI+均线、支撑阻力等

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

- `stocks` - 股票日线数据 (2015-2025)
- `fundamentals` - 当前基本面数据
- `fundamentals_history` - 历史基本面数据 (2015-2019)
