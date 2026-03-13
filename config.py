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


# ================================================================== #
#  Token
# ================================================================== #

# tushare token 存在 config/ 目录下，不提交到 git
TOKEN_PATH = os.path.join(ROOT_DIR, "config", "tushare_token.txt")


# ================================================================== #
#  数据库路径   datas/quant_data.db
# ================================================================== #

DB_PATH = os.path.join(ROOT_DIR, "datas", "quant_data.db")


# ================================================================== #
#  原始 Parquet 层   datas/parquet/
#  结构：{表名}/ts_code=xxx/part-0.parquet
# ================================================================== #

PARQUET_ROOT = os.path.join(ROOT_DIR, "datas", "parquet")

DAILY_DIR   = os.path.join(PARQUET_ROOT, "daily")       # 日线行情
ADJ_DIR     = os.path.join(PARQUET_ROOT, "adj_factor")  # 复权因子
BASIC_DIR   = os.path.join(PARQUET_ROOT, "daily_basic") # 每日指标
INDEX_DIR   = os.path.join(PARQUET_ROOT, "index_daily") # 指数日线

REPORT_PATH = os.path.join(PARQUET_ROOT, "四表校核.csv")


# ================================================================== #
#  因子层   datas/factor/
#  三层递进：宽表 → 技术指标 → 形态信号
# ================================================================== #

FACTOR_ROOT = os.path.join(ROOT_DIR, "datas", "factor")

WIDE_DIR    = os.path.join(FACTOR_ROOT, "step1_wide")    # build_wide_table.py 输出
FACTORS_DIR = os.path.join(FACTOR_ROOT, "step2_factors") # compute_factors.py  输出
SIGNALS_DIR = os.path.join(FACTOR_ROOT, "step3_signals") # generate_signals.py 输出


# ================================================================== #
#  Qlib 目录   qlib_data/
# ================================================================== #

QLIB_DIR  = os.path.join(ROOT_DIR, "qlib_data")
QLIB_CAL  = os.path.join(QLIB_DIR, "calendars")
QLIB_INST = os.path.join(QLIB_DIR, "instruments")
QLIB_FEAT = os.path.join(QLIB_DIR, "features")


# ================================================================== #
#  回测输出目录   backtest/
# ================================================================== #

BACKTEST_DIR = os.path.join(ROOT_DIR, "backtest")


# ================================================================== #
#  指数配置
#
#  用途：
#    - 计算超额收益（策略净值 vs 基准指数净值）
#    - 市场环境过滤（大盘趋势判断）
#    - Beta/Alpha 归因分析
#
#  基准选择说明：
#    策略买入市值区间 10亿~25亿 → 小市值风格
#    主基准用中证1000（000852.SH），与持仓风格匹配
#    参考基准用沪深300（000300.SH），作为市场整体参照
# ================================================================== #

INDEX_LIST = [
    ("000001.SH", "上证指数"),  # 市场整体情绪
    ("399001.SZ", "深证成指"),  # 深市情绪
    ("000300.SH", "沪深300"),   # 大盘参考基准
    ("000852.SH", "中证1000"),  # ★ 主基准，与持仓小市值风格匹配
    ("399006.SZ", "创业板指"),  # 成长股风向标
]


# ================================================================== #
#  原始表配置
#
#  raw_to_parquet.py 读取此配置，决定每张表导出到哪个子目录
#  新增表只需在这里加一行，raw_to_parquet.py 不用改
# ================================================================== #

TABLE_CONFIG = {

    # ── 股票三张核心表 ────────────────────────────────────────────
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

    # ── 指数日线 ──────────────────────────────────────────────────
    # 按 ts_code 分区存储，结构与股票表一致
    # 无需复权，直接存原始点位
    "index_daily": {
        "date_col":   "trade_date",
        "output_dir": "index_daily",
    },

    # ── 未来新增表在下面取消注释 ──────────────────────────────────
    # "stk_limit": {          # 涨跌停价格，回测成交假设必备
    #     "date_col":   "trade_date",
    #     "output_dir": "stk_limit",
    # },
    # "chip_peak": {
    #     "date_col":   "trade_date",
    #     "output_dir": "chip_peak",
    # },
    # "holder_count": {       # 股东数量，季度披露
    #     "date_col":   "trade_date",
    #     "output_dir": "holder_count",
    # },
}


# ================================================================== #
#  宽表合并策略
#
#  build_wide_table.py 读取此配置，决定如何合并各原始表
#  fill 说明：
#    None  → 不填充（每个交易日都有数据，如复权因子）
#    ffill → 前向填充（稀疏表，用上一条有效值填充，如股东数量）
#
#  注：index_daily 是独立的市场基准表，不合并进个股宽表
# ================================================================== #

MERGE_CONFIG = {
    "adj_factor": {
        "how":  "left",
        "fill": None,
    },
    "daily_basic": {
        "how":  "left",
        "fill": None,
    },
    # ── 未来新增表的合并策略 ──────────────────────────────────────
    # "stk_limit": {
    #     "how":  "left",
    #     "fill": None,
    # },
    # "holder_count": {
    #     "how":  "left",
    #     "fill": "ffill",    # 季度披露，前向填充
    # },
}
