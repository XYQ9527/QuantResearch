"""
数据库管理模块 data_manager.py

所有表结构在 TABLE_SCHEMAS 里统一定义
新增表、加字段、改类型只改 TABLE_SCHEMAS，其他代码不动

当前管理六张表：
    daily_h      日线行情
    adj_factor   复权因子
    daily_basic  每日指标（市值/PE/PB/换手率）
    trade_cal    交易日历
    stock_basic  股票基础信息
    index_daily  指数日线（新增）

核心接口：
    db.save('index_daily', df)   ← 存任意表，统一入口
    db.fetch('index_daily', ...) ← 查任意表，统一入口
"""
import os
import sys
import sqlite3
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_PATH


# ================================================================== #
#  ★ 核心配置：所有表结构在这里统一定义
#
#  字段格式：(列名, SQLite类型, 注释)
#  primary_keys:  联合主键，决定去重逻辑
#  on_conflict:   主键冲突策略
#                 IGNORE  → 已存在跳过（行情/指标等原始数据）
#                 REPLACE → 已存在覆盖（stock_basic 状态会变化）
# ================================================================== #

TABLE_SCHEMAS = {

    # ── 日线行情 ──────────────────────────────────────────────────
    'daily_h': {
        'columns': [
            ('ts_code',    'TEXT', '股票代码'),
            ('trade_date', 'TEXT', '交易日期'),
            ('open',       'REAL', '开盘价'),
            ('high',       'REAL', '最高价'),
            ('low',        'REAL', '最低价'),
            ('close',      'REAL', '收盘价'),
            ('vol',        'REAL', '成交量（手）'),
            ('amount',     'REAL', '成交额（千元）'),
        ],
        'primary_keys': ['ts_code', 'trade_date'],
        'on_conflict':  'IGNORE',
    },

    # ── 复权因子 ──────────────────────────────────────────────────
    'adj_factor': {
        'columns': [
            ('ts_code',    'TEXT', '股票代码'),
            ('trade_date', 'TEXT', '交易日期'),
            ('adj_factor', 'REAL', '复权因子'),
        ],
        'primary_keys': ['ts_code', 'trade_date'],
        'on_conflict':  'IGNORE',
    },

    # ── 每日指标 ──────────────────────────────────────────────────
    # pe_ttm / dv_ttm 亏损或不分红时为 NULL，属正常现象
    'daily_basic': {
        'columns': [
            ('ts_code',          'TEXT', '股票代码'),
            ('trade_date',       'TEXT', '交易日期'),
            ('total_mv',         'REAL', '总市值（万元）'),
            ('circ_mv',          'REAL', '流通市值（万元）'),
            ('pe_ttm',           'REAL', '市盈率TTM，亏损为NULL'),
            ('pb',               'REAL', '市净率'),
            ('ps_ttm',           'REAL', '市销率TTM'),
            ('turnover_rate',    'REAL', '换手率(%)'),
            ('turnover_rate_f',  'REAL', '换手率-自由流通股(%)'),
            ('volume_ratio',     'REAL', '量比'),
            ('dv_ttm',           'REAL', '股息率TTM，不分红为NULL'),
            ('total_share',      'REAL', '总股本（万股）'),
            ('float_share',      'REAL', '流通股本（万股）'),
            ('free_share',       'REAL', '自由流通股本（万股）'),
        ],
        'primary_keys': ['ts_code', 'trade_date'],
        'on_conflict':  'IGNORE',
    },

    # ── 交易日历 ──────────────────────────────────────────────────
    # A股三所日历完全同步，只存上交所（SSE）
    'trade_cal': {
        'columns': [
            ('exchange',      'TEXT',    '交易所'),
            ('cal_date',      'TEXT',    '日历日期'),
            ('is_open',       'INTEGER', '是否交易日 1=是 0=否'),
            ('pretrade_date', 'TEXT',    '上一交易日'),
        ],
        'primary_keys': ['exchange', 'cal_date'],
        'on_conflict':  'IGNORE',
    },

    # ── 股票基础信息 ──────────────────────────────────────────────
    # list_date:   防止回测买入未来才上市的股票（未来函数）
    # delist_date: 在市股票为空字符串
    # is_st:       接口无此字段，从名称判断后写入
    'stock_basic': {
        'columns': [
            ('ts_code',      'TEXT',    '股票代码'),
            ('name',         'TEXT',    '股票名称'),
            ('market',       'TEXT',    '市场类型 主板/创业板/科创板'),
            ('exchange',     'TEXT',    '交易所'),
            ('industry',     'TEXT',    '所属行业'),
            ('area',         'TEXT',    '地域'),
            ('list_date',    'TEXT',    '上市日期'),
            ('delist_date',  'TEXT',    '退市日期，在市为空字符串'),
            ('list_status',  'TEXT',    'L上市 D退市 P暂停'),
            ('is_st',        'INTEGER', '是否ST 1=是 0=否'),
        ],
        'primary_keys': ['ts_code'],
        'on_conflict':  'REPLACE',
    },

    # ── 指数日线 ──────────────────────────────────────────────────
    # 指数不存在复权问题，直接存原始点位
    # pct_chg: 当日涨跌幅(%)    change: 当日点位变动
    # 用途：计算超额收益、市场环境过滤、Beta/Alpha 归因
    'index_daily': {
        'columns': [
            ('ts_code',    'TEXT', '指数代码'),
            ('trade_date', 'TEXT', '交易日期'),
            ('open',       'REAL', '开盘点位'),
            ('high',       'REAL', '最高点位'),
            ('low',        'REAL', '最低点位'),
            ('close',      'REAL', '收盘点位'),
            ('vol',        'REAL', '成交量'),
            ('amount',     'REAL', '成交额'),
            ('pct_chg',    'REAL', '涨跌幅(%)'),
            ('change',     'REAL', '点位变动'),
        ],
        'primary_keys': ['ts_code', 'trade_date'],
        'on_conflict':  'IGNORE',
    },
}


# ================================================================== #
#  内部工具：从 TABLE_SCHEMAS 自动生成 SQL
# ================================================================== #

def _build_create_sql(table_name: str) -> str:
    """根据 TABLE_SCHEMAS 生成 CREATE TABLE SQL"""
    schema   = TABLE_SCHEMAS[table_name]
    col_defs = [f"{col} {dtype}" for col, dtype, _ in schema['columns']]
    pk_str   = f", PRIMARY KEY ({', '.join(schema['primary_keys'])})"
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)}{pk_str})"


def _col_names(table_name: str) -> list:
    """返回表的列名列表（顺序与 TABLE_SCHEMAS 一致）"""
    return [col for col, _, _ in TABLE_SCHEMAS[table_name]['columns']]


def _build_insert_sql(table_name: str) -> str:
    """根据 TABLE_SCHEMAS 生成 INSERT SQL"""
    schema       = TABLE_SCHEMAS[table_name]
    cols         = _col_names(table_name)
    on_conflict  = schema['on_conflict']
    placeholders = ', '.join(['?'] * len(cols))
    col_str      = ', '.join(cols)
    return f"INSERT OR {on_conflict} INTO {table_name} ({col_str}) VALUES ({placeholders})"


# ================================================================== #
#  StockDataManager
# ================================================================== #

class StockDataManager:
    """
    本地数据库管理模块

    表结构统一在 TABLE_SCHEMAS 里定义，新增表/加字段只改那里。

    核心接口：
        db.save('index_daily', df)              ← 存任意表
        db.fetch('index_daily', ts_code='...')  ← 查任意表
        db.get_trade_dates(start, end)          ← 交易日列表
        db.get_tradeable_stocks(date)           ← 可交易股票池
        db.get_index(ts_code, start, end)       ← 查指数，快捷接口
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        """根据 TABLE_SCHEMAS 初始化所有表（已存在则跳过）"""
        with self._get_connection() as conn:
            for table_name in TABLE_SCHEMAS:
                conn.execute(_build_create_sql(table_name))
            conn.commit()
        print(
            f"✅ 数据库已就绪：{self.db_path}\n"
            f"   共 {len(TABLE_SCHEMAS)} 张表："
            f"{', '.join(TABLE_SCHEMAS)}"
        )

    # ------------------------------------------------------------------ #
    #  统一存库入口
    # ------------------------------------------------------------------ #

    def save(self, table_name: str, df: pd.DataFrame) -> int:
        """
        存库统一入口，所有表都走这里

        Args:
            table_name: TABLE_SCHEMAS 里定义的表名
            df:         原始 DataFrame

        Returns:
            实际写入行数

        用法示例：
            db.save('daily_h',     df_daily)
            db.save('adj_factor',  df_adj)
            db.save('daily_basic', df_basic)
            db.save('trade_cal',   df_cal)
            db.save('stock_basic', df_stock)
            db.save('index_daily', df_index)
        """
        if df is None or df.empty:
            return 0

        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"未知表名 '{table_name}'，"
                f"请先在 TABLE_SCHEMAS 里定义。"
                f"当前已有：{list(TABLE_SCHEMAS)}"
            )

        df = df.copy()

        # stock_basic 特殊处理：补充接口不返回的衍生字段
        if table_name == 'stock_basic':
            df['delist_date'] = df['delist_date'].fillna('')
            df['is_st'] = df['name'].str.contains('ST', na=False).astype(int)

        # 按 TABLE_SCHEMAS 定义的列顺序过滤，多余列自动忽略
        cols      = [c for c in _col_names(table_name) if c in df.columns]
        df_sorted = df[cols].sort_values(TABLE_SCHEMAS[table_name]['primary_keys'])

        sql  = _build_insert_sql(table_name)
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(sql, df_sorted.itertuples(index=False, name=None))
            conn.commit()
            print(f"✅ [{table_name}] 写入 {len(df_sorted)} 行")
            return len(df_sorted)
        except Exception as e:
            conn.rollback()
            print(f"❌ [{table_name}] 写入失败，已回滚: {e}")
            return 0
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    #  统一查询入口
    # ------------------------------------------------------------------ #

    def fetch(
        self,
        table_name:  str,
        ts_code:     str  = None,
        start_date:  str  = None,
        end_date:    str  = None,
    ) -> pd.DataFrame:
        """
        查询统一入口，支持按代码和日期范围过滤

        用法示例：
            db.fetch('daily_h',     ts_code='000001.SZ',
                     start_date='20160101', end_date='20260310')
            db.fetch('index_daily', ts_code='000300.SH')
            db.fetch('trade_cal')
        """
        query  = f"SELECT * FROM {table_name} WHERE 1=1"
        params = []

        if ts_code:
            query += " AND ts_code = ?"
            params.append(ts_code)
        if start_date:
            query += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND trade_date <= ?"
            params.append(end_date)

        query += " ORDER BY trade_date ASC"

        with self._get_connection() as conn:
            return pd.read_sql(query, conn, params=params)

    # ------------------------------------------------------------------ #
    #  常用快捷查询
    # ------------------------------------------------------------------ #

    def get_trade_dates(
        self,
        start_date: str,
        end_date:   str,
        exchange:   str = 'SSE',
    ) -> list:
        """返回指定区间内所有交易日列表"""
        sql = """
            SELECT cal_date FROM trade_cal
            WHERE exchange = ? AND is_open = 1
              AND cal_date >= ? AND cal_date <= ?
            ORDER BY cal_date
        """
        with self._get_connection() as conn:
            return [
                r[0] for r in
                conn.execute(sql, (exchange, start_date, end_date)).fetchall()
            ]

    def get_tradeable_stocks(self, date: str) -> list:
        """
        返回指定日期的可交易股票列表

        过滤条件：已上市 + 未退市 + 非ST
        """
        sql = """
            SELECT ts_code FROM stock_basic
            WHERE list_date <= ?
              AND (delist_date = '' OR delist_date > ?)
              AND is_st = 0
        """
        with self._get_connection() as conn:
            return [r[0] for r in conn.execute(sql, (date, date)).fetchall()]

    def get_index(
        self,
        ts_code:    str,
        start_date: str = None,
        end_date:   str = None,
    ) -> pd.DataFrame:
        """
        查询指数日线数据，快捷接口

        用法示例：
            # 拉取中证1000全段数据（主基准）
            csi1000 = db.get_index('000852.SH', '20160101', '20260310')

            # 拉取沪深300（参考基准）
            hs300 = db.get_index('000300.SH', '20160101', '20260310')
        """
        return self.fetch(
            'index_daily',
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )


# ================================================================== #
#  独立工具函数
# ================================================================== #

def merge_features(base_df: pd.DataFrame, feature_dfs: list) -> pd.DataFrame:
    """
    将特征表与基表左连接对齐，按股票前向填充低频数据

    Args:
        base_df:     行情表，作为时间轴基准
        feature_dfs: 特征 DataFrame 列表，需含 ts_code 和 trade_date
    """
    final_df = base_df.copy()
    for f_df in feature_dfs:
        final_df = pd.merge(
            final_df, f_df,
            on=['trade_date', 'ts_code'],
            how='left',
        )
    final_df = final_df.sort_values(['ts_code', 'trade_date'])
    final_df = final_df.groupby('ts_code').ffill()
    return final_df


# ================================================================== #
#  调试入口
# ================================================================== #

if __name__ == '__main__':
    db = StockDataManager()

    # 查看所有表结构
    print("\n当前表结构：")
    for t in TABLE_SCHEMAS:
        cols = _col_names(t)
        pks  = TABLE_SCHEMAS[t]['primary_keys']
        print(f"  {t:<15} {len(cols)} 列  主键: {pks}")

    # 查询示例：个股
    df = db.fetch(
        'adj_factor', ts_code='000001.SZ',
        start_date='20160101', end_date='20260310',
    )
    print(f"\nadj_factor 查询结果: {df.shape}")
    print(df.head())

    # 查询示例：指数（快捷接口）
    idx = db.get_index('000852.SH', '20160101', '20260310')
    print(f"\n中证1000 查询结果: {idx.shape}")
    print(idx.tail())
