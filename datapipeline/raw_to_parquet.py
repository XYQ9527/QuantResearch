"""
原始层导出器 raw_to_parquet.py

将 SQLite 各表独立导出成 Parquet，按 ts_code 分区存储。
TABLE_CONFIG 里配置了哪些表，这里就导出哪些表，新增表不用改这个文件。

输出结构：
    datas/parquet/
        daily/
            ts_code=000001.SZ/
                part-0.parquet
        adj_factor/
            ts_code=000001.SZ/
                part-0.parquet
        daily_basic/
            ts_code=000001.SZ/
                part-0.parquet
        index_daily/
            ts_code=000001.SH/
                part-0.parquet
"""
import os
import sys
import time
import sqlite3

import pandas          as pd
import pyarrow         as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_PATH, PARQUET_ROOT, TABLE_CONFIG


# ================================================================== #
#  内部工具
# ================================================================== #

def _read_table(table_name: str, date_col: str) -> pd.DataFrame:
    """
    从 SQLite 读取一张表

    处理细节：
      1. 丢弃 Tushare 内部行号列 Rec（如果存在）
      2. 日期列统一转为 datetime 格式
      3. 按 ts_code + 日期排序，保证分区内有序
    """
    conn: sqlite3.Connection = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()

    # 丢弃 Tushare 内部行号，不让它污染 Parquet
    df = df.drop(columns=['Rec'], errors='ignore')

    # 日期列统一格式
    df[date_col] = pd.to_datetime(df[date_col], format='%Y%m%d')

    # 按 ts_code + 日期排序
    df = df.sort_values(['ts_code', date_col]).reset_index(drop=True)

    return df


# ================================================================== #
#  单表导出
# ================================================================== #

def export_table(table_name: str):
    """
    导出单张表：pyarrow 批量分区写入

    Args:
        table_name: TABLE_CONFIG 里定义的表名
    """
    if table_name not in TABLE_CONFIG:
        raise ValueError(
            f"表 '{table_name}' 未在 TABLE_CONFIG 中配置，"
            f"当前已有：{list(TABLE_CONFIG)}"
        )

    cfg        = TABLE_CONFIG[table_name]
    date_col   = cfg['date_col']
    output_dir = os.path.join(PARQUET_ROOT, cfg['output_dir'])

    print(f"[{table_name}] 开始导出...")
    t0 = time.time()

    df = _read_table(table_name, date_col)

    if df.empty:
        print(f"[{table_name}] ⚠️  表为空，跳过\n")
        return

    n_codes = df['ts_code'].nunique()
    print(f"[{table_name}] {len(df):,} 条 | {n_codes} 个代码")

    os.makedirs(output_dir, exist_ok=True)

    pq.write_to_dataset(
        pa.Table.from_pandas(df=df, preserve_index=False),
        root_path=output_dir,
        partition_cols=['ts_code'],
        existing_data_behavior='overwrite_or_ignore',
        use_threads=True,
    )

    elapsed = time.time() - t0
    print(f"[{table_name}] ✅ 完成 → {output_dir}  ({elapsed:.1f}s)\n")


# ================================================================== #
#  全量导出入口
# ================================================================== #

def export_all():
    """
    一次性导出 TABLE_CONFIG 里的所有表

    新增表只需在 config.py 的 TABLE_CONFIG 里加一行，
    这里不用改。
    """
    print('=' * 55)
    print(f"原始层导出：共 {len(TABLE_CONFIG)} 张表")
    print(f"数据库：{DB_PATH}")
    print(f"输出目录：{PARQUET_ROOT}")
    print(f"导出列表：{list(TABLE_CONFIG)}")
    print('=' * 55 + '\n')

    t0 = time.time()

    for table_name in TABLE_CONFIG:
        export_table(table_name)

    elapsed = time.time() - t0
    print('=' * 55)
    print(f"✅ 全部导出完成 | 总耗时：{elapsed:.1f}s")
    print('=' * 55)


# ================================================================== #
#  调试入口
# ================================================================== #

if __name__ == '__main__':
    export_all()
