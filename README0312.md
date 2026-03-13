# 超卖反弹策略 · 量化研究项目

> A股全市场数据入库 → 因子工程 → IC验证 → 策略回测 → 实盘

---

## 项目结构

```
QuantResearch/
│
├── main.py                     【统一入口】命令行调用各模块
├── config.py                   【全局配置】路径、参数统一管理
├── README.md                   本文件
│
├── data/                       数据层
│   ├── tushare_fetcher.py      【数据获取】Tushare API 封装
│   ├── fetch_pipeline.py       【入库流水线】五张表拉取+入库总控
│   ├── raw_to_parquet.py       【格式转换】SQLite → Parquet 分区存储
│   └── check_data.py           【数据校验】三层校核，问题股票自动剔除
│
├── factor/                     因子层
│   ├── build_wide_table.py     【宽表构建】三表合并 + 前复权
│   ├── compute_factors.py      【技术指标】TA-Lib 计算 MACD/KDJ/RSI/BOLL 等
│   ├── generate_signals.py     【形态信号】构造 0/1 事件信号
│   └── export_qlib.py          【Qlib同步】signals层 → Qlib .day.bin
│
├── research/                   研究层
│   └── factor_eval.py          【因子评估】IC 分析，输出交互式仪表板
│
├── backtest/                   回测层（待开发）
│   └── backtest_portfolio.py   【策略回测】vectorbt 引擎
│
├── utils/                      工具层
│   └── data_manager.py         【数据管理】SQLite 读写封装
│
├── config/
│   └── tushare_token.txt       Tushare Token（不提交 git）
│
├── data/
│   ├── quant_data.db           SQLite 原始数据库
│   ├── parquet/                原始 Parquet 分区
│   │   ├── daily/              日线行情
│   │   ├── adj_factor/         复权因子
│   │   └── daily_basic/        每日指标
│   └── factor/                 因子层 Parquet
│       ├── wide/               宽表（25列）
│       ├── factors/            技术指标（36列）
│       └── signals/            形态信号（39列）← 回测直接读这里
│
└── my_cn_data/                 Qlib 数据目录
    ├── calendars/day.txt
    ├── instruments/all.txt
    └── features/sz000001/
```

---

## 快速开始

```bash
conda activate qlib_env
cd D:\Desktop\LiangHuaSSS\QuantResearch

python main.py fetch      # 数据入库（首次或增量更新）
python main.py parquet    # SQLite → Parquet
python main.py check      # 数据三层校核
python main.py wide       # 构建宽表
python main.py factors    # 计算技术指标
python main.py signals    # 构造形态信号
python main.py qlib       # 同步到 Qlib
python main.py ic         # 因子 IC 分析

# 或一键全跑
python main.py all
```

---

## 完整数据流程

```
┌──────────────────────────────────────────────────────────────────┐
│  STEP 1  数据获取与入库                fetch_pipeline.py          │
│                                                                  │
│    trade_cal    交易日历    一次性全量，几秒                        │
│    stock_basic  股票信息    一次性全量，几秒                        │
│    daily_h      日线行情    并发拉取，约7分钟                       │
│    adj_factor   复权因子    并发拉取，约5分钟                       │
│    daily_basic  每日指标    并发拉取，约7分钟                       │
│                                                                  │
│  输出：quant_data.db                                              │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  STEP 2  格式转换                      raw_to_parquet.py         │
│                                                                  │
│  SQLite → Parquet，按股票分区存储                                  │
│  输出：data/parquet/{daily,adj_factor,daily_basic}/              │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  STEP 3  数据三层校核                  check_data.py             │
│                                                                  │
│  第一层：存在性校核   三张表是否都有对应文件夹                        │
│  第二层：日期对齐校核  daily 有的每一天，其他表必须都有               │
│  第三层：数值合理性   收盘价为正，复权因子合理，市值为正              │
│                                                                  │
│  问题股票文件夹自动删除，校核报告输出到 三表校核.csv                 │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  STEP 4  宽表构建                      build_wide_table.py       │
│                                                                  │
│  合并 daily + adj_factor + daily_basic，完成前复权                │
│  前复权公式：adj_close = close × (adj_factor / 最新adj_factor)   │
│                                                                  │
│  输出：data/factor/wide/（25列）                                  │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  STEP 5  技术指标计算（约15秒）         compute_factors.py        │
│                                                                  │
│  工具：TA-Lib，结果与通达信/同花顺对齐                              │
│                                                                  │
│  MACD   macd_dif / macd_dea / macd_bar    (12, 26, 9)          │
│  KDJ    kdj_k / kdj_d / kdj_j            (9, 3, 3)             │
│  RSI    rsi_6 / rsi_12                                          │
│  BOLL   boll_upper / boll_mid / boll_lower (20, 2)             │
│  动量   mom_5 / mom_10 / mom_20                                  │
│  均线   ma_5 / ma_10 / ma_20 / ma_60                            │
│  波动率 atr_14                                                    │
│                                                                  │
│  输出：data/factor/factors/（36列）                               │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  STEP 6  形态信号构造（约9秒）          generate_signals.py       │
│                                                                  │
│  kdj_cross     KDJ低位金叉                                        │
│    最近3天内：K上穿D 且 K<=24 且 D<=24                            │
│                                                                  │
│  rsi_drop      RSI近期触底                                        │
│    近5日RSI最低点 <= 近10日RSI最低点                               │
│                                                                  │
│  macd_shrink   MACD负值缩量                                       │
│    连续3天递增 且 当天值 < 0.1                                     │
│                                                                  │
│  输出：data/factor/signals/（39列）← 回测直接读这里               │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  STEP 7  同步到 Qlib                   export_qlib.py            │
│                                                                  │
│  signals层所有字段 → Qlib .day.bin 格式                           │
│  代码转换：000001.SZ → sz000001                                   │
│  factor 固定写 1.0（已前复权）                                     │
│                                                                  │
│  输出：my_cn_data/features/                                      │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  STEP 8  因子 IC 分析                  factor_eval.py            │
│                                                                  │
│  Spearman IC，2020-2026，1455个交易日截面                         │
│  输出：ic_dashboard.html（交互式仪表板）                           │
└──────────────────────────────────────────────────────────────────┘
                              ↓（待开发）
┌──────────────────────────────────────────────────────────────────┐
│  STEP 9  策略回测                      backtest_portfolio.py     │
│                                                                  │
│  买入：kdj_cross=1 AND rsi_drop=1 AND macd_shrink=1             │
│        AND 10亿 < total_mv < 25亿                                │
│  止损：持仓 > 3天 且亏损 > 4%                                     │
│  超时：持仓 ≥ 10天 且最大盈利 < 8%                                │
│  止盈：最大盈利 ≥ 8% 后回撤 > 3%                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 因子研究结论（2020-2026，Spearman IC）

| 因子 | IC均值 | ICIR | IC>0胜率 | 有效性 | 策略角色 |
|------|--------|------|----------|--------|----------|
| 小市值 | 0.0550 | 0.994 | 84.1% | ✅ 强效 | 核心选股因子 |
| RSI6超卖 | 0.0346 | 0.509 | 71.5% | ✅ 有效 | 辅助筛选 |
| KDJ J值 | 0.0210 | 0.323 | 63.8% | ⚠️ 弱效 | 参考 |
| MACD柱 | -0.0161 | -0.228 | 40.3% | ❌ 数值无效 | 形态过滤器 |
| KDJ金叉 | -0.0060 | -0.100 | 45.7% | ❌ 数值无效 | 形态过滤器 |

> MACD 和 KDJ 作为数值因子统计无效，但作为**形态过滤器**使用有意义。
> 不参与选股评分，在小市值候选池中作为择时条件筛选买入时机。

---

## 开发进度

```
数据工程层    ████████████  100%  五张表入库 + 宽表 + 因子 + Qlib 全部完成
因子研究层    ████████░░░░   65%  IC分析完成，回测待开发
策略回测层    ░░░░░░░░░░░░    0%  下一步：vectorbt 回测
实盘层        ░░░░░░░░░░░░    0%  待规划
```

---

## 环境信息

```
Python    3.9.25
TA-Lib    0.6.8
环境名     qlib_env

Qlib 初始化：
import qlib
qlib.init(provider_uri='D:\\Desktop\\LiangHuaSSS\\QuantResearch\\my_cn_data')
```
