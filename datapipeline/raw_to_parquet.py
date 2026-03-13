"""
原始层导出器：SQLite 各表独立导出成 Parquet
使用 pyarrow 批量分区写入，按股票分区存储

输出结构：
    datapipeline/parquet/raw/
        daily/
            ts_code=000001.SZ/
                part-0.parquet
        adj_factor/
            ts_code=000001.SZ/
                part-0.parquet
        chip_peak/               ← 未来新增表同理
"""
import os
import time
import sqlite3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from config import DB_PATH, PARQUET_ROOT, TABLE_CONFIG
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(PARQUET_ROOT)


def _read_table(table_name: str, date_col: str) -> pd.DataFrame:
    """
    从 SQLite 读取一张表，统一日期格式

    Tushare 接口会返回 Rec 内部行号列，SQLite 里可能已存入
    在这里统一 drop，不让它污染 parquet 文件
    """
    conn: sqlite3.Connection = sqlite3.connect(DB_PATH)  # ← 加类型注解
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    df = df.drop(columns=['Rec'], errors='ignore')  # 丢弃 Tushare 内部行号
    df[date_col] = pd.to_datetime(df[date_col], format="%Y%m%d")
    df = df.sort_values(["ts_code", date_col]).reset_index(drop=True)
    return df


def export_table(table_name: str):
    """导出单张表：pyarrow 批量分区写入"""
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"表 '{table_name}' 未在 TABLE_CONFIG 中配置")

    cfg = TABLE_CONFIG[table_name]
    date_col = cfg["date_col"]
    output_dir = os.path.join(RAW_DIR, cfg["output_dir"])

    print(f"[{table_name}] 开始导出...")
    t0 = time.time()

    df = _read_table(table_name, date_col)
    if df.empty:
        print(f"[{table_name}] ⚠️  表为空，跳过\n")
        return

    print(f"[{table_name}] {len(df):,} 条，{df['ts_code'].nunique()} 只股票")

    os.makedirs(output_dir, exist_ok=True)
    table = pa.Table.from_pandas(df=df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=output_dir,
        partition_cols=["ts_code"],
        existing_data_behavior="overwrite_or_ignore",
        use_threads=True,
    )

    elapsed = time.time() - t0
    print(f"[{table_name}] ✅ 完成 → {output_dir}  ({elapsed:.1f}s)\n")


def export_all():
    """一次性导出所有表，每张表独立存储"""
    print("=" * 55)
    print(f"原始层导出：共 {len(TABLE_CONFIG)} 张表")
    print("=" * 55 + "\n")

    t0 = time.time()
    for table_name in TABLE_CONFIG:
        export_table(table_name)

    elapsed = time.time() - t0
    print("=" * 55)
    print(f"✅ 全部导出完成，总耗时：{elapsed:.1f}s")
    print(f"   输出目录：{RAW_DIR}")
    print("=" * 55)


if __name__ == "__main__":
    export_all()
