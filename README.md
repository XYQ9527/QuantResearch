# 超卖反弹策略 · 量化研究项目

> A股全市场数据入库 → 因子工程 → IC验证 → 策略回测 → 实盘

---

## 项目结构

```
pythonProject/
│
├── README.md                   ← 本文件
│
├── ── 数据层 ──────────────────────────────────────────
├── tushare_fetcher.py          【数据获取】Tushare API 封装，拉取五张表
├── data_manager.py             【数据管理】SQLite 读写封装，统一 CRUD 接口
├── get_data.py                 【流水线】协调拉取+写入，五张表入库总控
├── data_to_parquet.py          【格式转换】SQLite → Parquet（输出 wide 层）
├── check_data.py               【数据校验】校验完整性、对齐、缺失值
│
├── ── 因子层 ──────────────────────────────────────────
├── build_wide_table.py         【宽表构建】行情+基本面合并，完成前复权
├── factors_compute.py          【初级因子】TA-Lib 计算 MACD/KDJ/RSI/BOLL
├── factor_engineer.py          【加工因子】构造形态信号 0/1
├── perquet_to_qlibdata.py      【Qlib同步】signals层 → Qlib .day.bin
│
├── ── 研究层 ──────────────────────────────────────────
├── step2_factor_eval.py        【因子评估】IC 分析，输出交互式仪表板
├── ic_dashboard.html           【IC报告】因子分析结果（自动生成）
│
├── ── 回测层（待开发）──────────────────────────────────
├── backtest_vectorbt.py        【事件回测】事件触发策略，vectorbt 引擎
├── backtest_qlib.py            【评分回测】滚动 Top10，Qlib 引擎
│
├── config.py                   【全局配置】Token、路径、参数
│
├── data/
│   ├── quant_data.db           SQLite 原始数据库
│   └── parquet/
│       ├── wide/               宽表（25列）
│       ├── factors/            初级因子（36列）
│       └── signals/            最终宽表（39列）← 回测直接读这里
│
└── my_cn_data/                 Qlib 数据目录
    ├── calendars/day.txt       交易日历（2465天）
    ├── instruments/all.txt     股票列表（5507只）
    └── features/sz000001/      各股票特征 .day.bin
```

---

## 完整数据流程

```
┌──────────────────────────────────────────────────────────────────┐
│  STEP 1  数据获取与入库                                            │
│                                                                  │
│  tushare_fetcher.py   API封装，定义每张表的拉取逻辑               │
│  data_manager.py      SQLite封装，提供 save/query 接口            │
│         ↑ 被调用                                                  │
│  get_data.py          总控脚本，按顺序协调以上两个模块完成入库      │
│                                                                  │
│  入库顺序：                                                        │
│    Step1  trade_cal    交易日历    一次性全量，几秒                 │
│    Step2  stock_basic  股票信息    一次性全量，几秒                 │
│    Step3  daily_h      日线行情    并发拉取，约7分钟                │
│    Step4  adj_factor   复权因子    并发拉取，约5分钟                │
│    Step5  daily_basic  每日指标    并发拉取，约7分钟                │
│                                                                  │
│  输出：quant_data.db（SQLite）                                    │
│    daily_h       ~641万行                                        │
│    adj_factor    ~641万行                                        │
│    daily_basic   ~1300万行                                       │
│    trade_cal     2465天                                          │
│    stock_basic   5507只                                          │
└──────────────────────────────────────────────────────────────────┘
                              ↓ data_to_parquet.py
┌──────────────────────────────────────────────────────────────────┐
│  STEP 2  格式转换 + 数据校验                                       │
│                                                                  │
│  data_to_parquet.py   SQLite → Parquet，按股票分区存储            │
│  check_data.py        校验数据完整性、日期对齐、缺失值检查          │
│                                                                  │
│  校验通过后再进行后续步骤，数据有问题先修复                          │
└──────────────────────────────────────────────────────────────────┘
                              ↓ build_wide_table.py
┌──────────────────────────────────────────────────────────────────┐
│  STEP 3  宽表构建（第二层：wide）                                   │
│                                                                  │
│  build_wide_table.py                                             │
│    合并 daily_h + adj_factor + daily_basic                       │
│    完成前复权（adj_close = close × adj_factor）                   │
│    按股票代码分区存储                                               │
│                                                                  │
│  输出：data/parquet/wide/ts_code=xxx/                            │
│  25列：前复权价格 + 原始价格 + 基本面 + 换手率 + 股本              │
│  规模：5186只 × 平均2000行 ≈ 1000万行                             │
└──────────────────────────────────────────────────────────────────┘
                              ↓ factors_compute.py（约15秒）
┌──────────────────────────────────────────────────────────────────┐
│  STEP 4  初级因子计算（第三层：factors）                            │
│                                                                  │
│  factors_compute.py                                              │
│    工具：TA-Lib（C底层，结果与通达信/同花顺对齐）                   │
│    并发：20进程                                                    │
│                                                                  │
│  新增11列技术指标：                                                │
│    MACD   macd_dif / macd_dea / macd_bar    参数(12,26,9)       │
│    KDJ    kdj_k / kdj_d / kdj_j             参数(9,3,3)         │
│    RSI    rsi_6 / rsi_12                                        │
│    BOLL   boll_upper / boll_mid / boll_lower 参数(20,2)         │
│                                                                  │
│  输出：data/parquet/factors/ts_code=xxx/（36列）                  │
└──────────────────────────────────────────────────────────────────┘
                              ↓ factor_engineer.py（约9秒）
┌──────────────────────────────────────────────────────────────────┐
│  STEP 5  加工因子（最终宽表：signals）                              │
│                                                                  │
│  factor_engineer.py                                              │
│    基于初级因子，构造策略所需的形态信号（0/1）                       │
│    并发：20进程                                                    │
│                                                                  │
│  新增3列形态信号：                                                 │
│                                                                  │
│  macd_shrink    MACD缩量信号                                      │
│    macd_bar < 0 且连续3天递增（负值区柱子在收缩）                   │
│    触发频率约 19%                                                  │
│                                                                  │
│  kdj_cross      KDJ低位金叉信号                                   │
│    今天K>D 且昨天K<D 且K<50（低位才有效）                          │
│    触发频率约 8.5%                                                 │
│                                                                  │
│  rsi_oversold   RSI超卖信号                                       │
│    rsi_6 < 30                                                    │
│    触发频率约 15%                                                  │
│                                                                  │
│  输出：data/parquet/signals/ts_code=xxx/（39列）← 回测直接读这里   │
└──────────────────────────────────────────────────────────────────┘
                              ↓ perquet_to_qlibdata.py
┌──────────────────────────────────────────────────────────────────┐
│  STEP 6  同步到 Qlib                                              │
│                                                                  │
│  perquet_to_qlibdata.py                                          │
│    signals层所有字段 → Qlib .day.bin 格式                         │
│    代码转换：000001.SZ → sz000001                                 │
│    factor=1.0（已前复权，Qlib不需要再复权）                         │
│                                                                  │
│  输出：my_cn_data/features/sz000001/*.day.bin                    │
│  Qlib可直接读取所有字段（$close / $macd_bar / $kdj_cross 等）      │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│  STEP 7  因子分析                                                  │
│                                                                  │
│  step2_factor_eval.py                                            │
│    Spearman IC 分析，1455个交易日截面                              │
│    输出 ic_dashboard.html（交互式仪表板）                          │
└──────────────────────────────────────────────────────────────────┘
                              ↓（待开发）
┌──────────────────────────────────────────────────────────────────┐
│  STEP 8  策略回测                                                  │
│                                                                  │
│  backtest_vectorbt.py   事件触发回测                              │
│    买入：macd_shrink=1 AND kdj_cross=1 AND 市值在范围内            │
│    出场：止损/止盈/超时三条件                                       │
│                                                                  │
│  backtest_qlib.py       滚动Top10回测                             │
│    每期在满足形态条件的股票里，按小市值+RSI评分取Top10持有           │
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
> 它们不参与选股评分，在小市值候选池中作为择时条件筛选买入时机。

---

## 策略逻辑

### 买入条件（三个同时满足）
```
1. 市值筛选     10亿 < total_mv < 32亿
2. KDJ低位金叉  kdj_cross = 1
3. MACD负值缩量 macd_shrink = 1
```

### 出场条件
```
止损：持仓 > 3天 且 亏损 > 4%
超时：持仓 ≥ 10天 且 最大盈利 < 8%
止盈：最大盈利 ≥ 8% 后回撤 > 3%
```

---

## 快速开始

```bash
conda activate qlib_env

# STEP 1  数据入库（首次或每日增量）
python get_data.py

# STEP 2  格式转换
python data_to_parquet.py

# STEP 3  数据校验（确认无误再往下走）
python check_data.py

# STEP 4  宽表构建
python build_wide_table.py

# STEP 5  初级因子（约15秒）
python factors_compute.py

# STEP 6  加工因子（约9秒）
python factor_engineer.py

# STEP 7  同步Qlib
python perquet_to_qlibdata.py

# STEP 8  因子分析
python step2_factor_eval.py
```

---

## 开发进度

```
数据工程层    ████████████  100%  五张表入库 + 宽表 + 因子 + Qlib 全部完成
因子研究层    ████████░░░░   65%  IC分析完成，回测待开发
策略回测层    ░░░░░░░░░░░░    0%  下一步：vectorbt + Qlib 双路回测
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
qlib.init(provider_uri='D:\\Desktop\\LiangHuaSSS\\pythonProject\\my_cn_data')
```
