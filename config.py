"""
全局配置 config.py

所有路径、表结构、合并策略统一在这里管理
新增数据表只需修改 TABLE_CONFIG 和 MERGE_CONFIG，其他文件不用动
"""
import os
import sys

# 确保项目根目录在 Python 路径里
# 所有子目录脚本都可以直接 from config import xxx
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# ── Token ────────────────────────────────────────────────────────
# tushare token 存在 config/ 目录下，不提交到 git
TOKEN_PATH = os.path.join(ROOT_DIR, "config", "tushare_token.txt")

# ── 数据库 ───────────────────────────────────────────────────────
DB_PATH = os.path.join(ROOT_DIR, "datapipeline", "quant_data.db")

# ── 原始 Parquet 层（datapipeline/ 目录下）──────────────────────────────
# raw_to_parquet.py 的输出目录
# 结构：datapipeline/parquet/{表名}/ts_code=xxx/part-0.parquet
PARQUET_ROOT = os.path.join(ROOT_DIR, "datapipeline", "parquet")
DAILY_DIR    = os.path.join(PARQUET_ROOT, "daily")
ADJ_DIR      = os.path.join(PARQUET_ROOT, "adj_factor")
BASIC_DIR    = os.path.join(PARQUET_ROOT, "daily_basic")
REPORT_PATH  = os.path.join(PARQUET_ROOT, "三表校核.csv")

# ── 因子层（factor/ 目录下）─────────────────────────────────────
# 三层递进：宽表 → 技术指标 → 形态信号
FACTOR_ROOT  = os.path.join(ROOT_DIR, "datapipeline", "factor")
WIDE_DIR     = os.path.join(FACTOR_ROOT, "wide")          # build_wide_table.py 输出
FACTORS_DIR  = os.path.join(FACTOR_ROOT, "factors")       # compute_factors.py 输出
SIGNALS_DIR  = os.path.join(FACTOR_ROOT, "signals")       # generate_signals.py 输出

# ── Qlib 目录 ────────────────────────────────────────────────────
# export_qlib.py 的输出目录
QLIB_DIR  = os.path.join(ROOT_DIR, "my_cn_data")
QLIB_CAL  = os.path.join(QLIB_DIR, "calendars")
QLIB_INST = os.path.join(QLIB_DIR, "instruments")
QLIB_FEAT = os.path.join(QLIB_DIR, "features")

# ── 回测输出目录 ─────────────────────────────────────────────────
BACKTEST_DIR = os.path.join(ROOT_DIR, "datapipeline", "backtest")

# ── 原始表配置 ───────────────────────────────────────────────────
# raw_to_parquet.py 读取此配置，决定每张表导出到哪个子目录
# 新增表只需在这里加一行，raw_to_parquet.py 不用改
TABLE_CONFIG = {
    "daily_h": {
        "date_col":   "trade_date",
        "output_dir": "daily",
    },
    "adj_factor": {
        "date_col":   "trade_date",
        "output_dir": "adj_factor",
    },
    "daily_basic": {
        "date_col":   "trade_date",
        "output_dir": "daily_basic",
    },
    # ── 未来新增表在下面取消注释 ──────────────────
    # "chip_peak": {
    #     "date_col":   "trade_date",
    #     "output_dir": "chip_peak",
    # },
    # "holder_count": {       # 股东数量，季度披露
    #     "date_col":   "trade_date",
    #     "output_dir": "holder_count",
    # },
    # "stk_limit": {          # 涨跌停价格
    #     "date_col":   "trade_date",
    #     "output_dir": "stk_limit",
    # },
}

# ── 宽表合并策略 ─────────────────────────────────────────────────
# build_wide_table.py 读取此配置，决定如何合并各原始表
# fill 说明：
#   None  → 不填充（每个交易日都有数据，如复权因子）
#   ffill → 前向填充（稀疏表，用上一条有效值填充，如股东数量）
MERGE_CONFIG = {
    "adj_factor": {
        "how":  "left",
        "fill": None,
    },
    "daily_basic": {
        "how":  "left",
        "fill": None,
    },
    # ── 未来新增表的合并策略 ──────────────────────
    # "holder_count": {
    #     "how":  "left",
    #     "fill": "ffill",    # 季度披露，前向填充
    # },
    # "stk_limit": {
    #     "how":  "left",
    #     "fill": None,
    # },
}