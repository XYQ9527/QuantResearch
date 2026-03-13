"""
数据校核模块 check_data.py

针对股票三张核心表执行三层校核，任何一层有问题直接 DROP。
只有三层全部通过的股票才保留，目录天然干净供 build_wide_table.py 使用。

注：index_daily 是独立的市场基准表，不参与校核。

第一层：存在性校核    三张表是否都有对应文件夹
第二层：日期对齐校核  daily 有的每一天，adj_factor 和 daily_basic 必须都有
第三层：数值合理性校核 收盘价为正，复权因子在合理范围，总市值为正

输出：
    四表校核.csv  → 问题股票明细（原因记录在 note 列）
    DROP 股票的 parquet 文件夹直接删除，build_wide_table.py 扫目录即可
"""
import os
import sys
import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas          as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DAILY_DIR, ADJ_DIR, BASIC_DIR, REPORT_PATH

logger = logging.getLogger(__name__)

# 三张表统一管理，存在性校核和 DROP 删除时都用这里
ALL_DIRS = {
    'daily':      DAILY_DIR,
    'adj_factor': ADJ_DIR,
    'daily_basic': BASIC_DIR,
}


# ================================================================== #
#  第二层 + 第三层：单只股票校核
# ================================================================== #

def _validate_single(ts_code: str) -> dict:
    """
    对单只股票执行第二层和第三层校核

    第二层：日期对齐
        daily 有的每一天，adj_factor 和 daily_basic 必须都有
        adj_factor / daily_basic 比 daily 多的日期属正常现象，不报错

    第三层：数值合理性
        收盘价必须全部 > 0
        复权因子必须在合理范围 0.01 ~ 1000
        total_mv 必须全部 > 0（市值为 0 或负是数据源问题）
        pe_ttm / dv_ttm 等允许为 NULL 的字段不做校核
    """
    result = {
        'ts_code':          ts_code,
        'daily_days':       0,
        'adj_days':         0,
        'basic_days':       0,
        'missing_in_adj':   0,
        'missing_in_basic': 0,
        'price_anomaly':    0,
        'factor_anomaly':   0,
        'mv_anomaly':       0,
        'status':           'PASS',
        'note':             '',
    }

    try:
        # 只读需要的列，减少 IO
        df_daily = pq.read_table(
            os.path.join(DAILY_DIR, f'ts_code={ts_code}'),
            columns=['trade_date', 'close'],
        ).to_pandas()

        df_adj = pq.read_table(
            os.path.join(ADJ_DIR, f'ts_code={ts_code}'),
            columns=['trade_date', 'adj_factor'],
        ).to_pandas()

        df_basic = pq.read_table(
            os.path.join(BASIC_DIR, f'ts_code={ts_code}'),
            columns=['trade_date', 'total_mv'],
        ).to_pandas()

        daily_dates = set(df_daily['trade_date'])
        adj_dates   = set(df_adj['trade_date'])
        basic_dates = set(df_basic['trade_date'])

        result['daily_days'] = len(daily_dates)
        result['adj_days']   = len(adj_dates)
        result['basic_days'] = len(basic_dates)

        problems = []

        # ── 第二层：日期对齐 ──────────────────────────────────────

        missing_adj = daily_dates - adj_dates
        result['missing_in_adj'] = len(missing_adj)
        if missing_adj:
            problems.append(f"复权因子缺失{len(missing_adj)}天")

        missing_basic = daily_dates - basic_dates
        result['missing_in_basic'] = len(missing_basic)
        if missing_basic:
            problems.append(f"每日指标缺失{len(missing_basic)}天")

        # ── 第三层：数值合理性 ────────────────────────────────────

        price_anomaly = int((df_daily['close'] <= 0).sum())
        result['price_anomaly'] = price_anomaly
        if price_anomaly:
            problems.append(f"价格异常{price_anomaly}天")

        factor_anomaly = int(
            ((df_adj['adj_factor'] <= 0.01) | (df_adj['adj_factor'] > 1000)).sum()
        )
        result['factor_anomaly'] = factor_anomaly
        if factor_anomaly:
            problems.append(f"因子异常{factor_anomaly}天")

        mv_anomaly = int((df_basic['total_mv'].dropna() <= 0).sum())
        result['mv_anomaly'] = mv_anomaly
        if mv_anomaly:
            problems.append(f"市值异常{mv_anomaly}天")

        # ── 综合判定 ──────────────────────────────────────────────
        if problems:
            result['status'] = 'DROP'
            result['note']   = '；'.join(problems)
        else:
            result['note'] = '完全对齐'

    except Exception as e:
        result['status'] = 'DROP'
        result['note']   = f'读取失败: {e}'

    return result


# ================================================================== #
#  主校核入口
# ================================================================== #

def run_validation(max_workers: int = 20) -> list:
    """
    对全市场所有股票执行三层完整校核

    Args:
        max_workers: 并发线程数，校核是纯 IO 操作，多线程效果显著

    Returns:
        valid_codes: 三层全部通过的股票代码列表
    """
    t0 = time.time()

    # ── 第一层：存在性校核 ────────────────────────────────────────
    logger.info("第一层：存在性校核（三张表）...")

    code_sets = {}
    for name, base_dir in ALL_DIRS.items():
        code_sets[name] = {
            f.replace('ts_code=', '')
            for f in os.listdir(base_dir)
            if os.path.isdir(os.path.join(base_dir, f))
        }
        logger.info(f"  {name}: {len(code_sets[name])} 只")

    # 三张表都有才进入下一层校核
    all_three = (
        code_sets['daily'] &
        code_sets['adj_factor'] &
        code_sets['daily_basic']
    )
    all_codes = (
        code_sets['daily'] |
        code_sets['adj_factor'] |
        code_sets['daily_basic']
    )

    # 缺任意一张表的股票直接 DROP
    drop_records = []
    for code in all_codes - all_three:
        missing_tables = [name for name, s in code_sets.items() if code not in s]
        drop_records.append({
            'ts_code':          code,
            'daily_days':       -1,
            'adj_days':         -1,
            'basic_days':       -1,
            'missing_in_adj':   -1,
            'missing_in_basic': -1,
            'price_anomaly':    0,
            'factor_anomaly':   0,
            'mv_anomaly':       0,
            'status':           'DROP',
            'note':             f"缺少表：{', '.join(missing_tables)}",
        })

    logger.info(
        f"  三表齐全: {len(all_three)} 只 | "
        f"缺表直接DROP: {len(all_codes - all_three)} 只"
    )

    # ── 第二层 + 第三层：并发校核 ─────────────────────────────────
    logger.info("第二层 + 第三层：日期对齐 + 数值合理性校核...")
    logger.info(f"  并发线程数: {max_workers} | 待校核: {len(all_three)} 只")

    results   = []
    completed = 0
    total     = len(all_three)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {
            executor.submit(_validate_single, code): code
            for code in all_three
        }
        for future in as_completed(future_to_code):
            completed += 1
            results.append(future.result())
            if completed % 500 == 0 or completed == total:
                elapsed = time.time() - t0
                logger.info(
                    f"  进度: {completed}/{total} "
                    f"({completed / total * 100:.1f}%) | "
                    f"速率: {completed / elapsed:.0f}只/秒 | "
                    f"耗时: {elapsed:.1f}s"
                )

    # ── 汇总结果 ──────────────────────────────────────────────────
    report_df   = pd.DataFrame(results + drop_records)
    pass_df     = report_df[report_df['status'] == 'PASS']
    drop_df     = report_df[report_df['status'] == 'DROP']
    valid_codes = sorted(pass_df['ts_code'].tolist())

    # ── 删除 DROP 股票的文件夹 ────────────────────────────────────
    failed_delete = []
    if not drop_df.empty:
        logger.info(f"删除 {len(drop_df)} 只问题股票的文件夹...")
        for ts_code in drop_df['ts_code']:
            for base_dir in ALL_DIRS.values():
                folder = os.path.join(base_dir, f'ts_code={ts_code}')
                if os.path.exists(folder):
                    try:
                        shutil.rmtree(folder)
                    except Exception as e:
                        failed_delete.append(f"{folder}: {e}")

    # ── 保存问题股票报告 ──────────────────────────────────────────
    drop_df.to_csv(REPORT_PATH, index=False, encoding='utf-8-sig')

    # ── 打印最终摘要 ──────────────────────────────────────────────
    elapsed = time.time() - t0
    logger.info(f"\n{'=' * 50}")
    logger.info(f"三层校核完成 | 总耗时: {elapsed:.1f}s")
    logger.info(f"{'=' * 50}")
    logger.info(f"✅ PASS（保留）: {len(valid_codes):>6} 只")
    logger.info(f"❌ DROP（删除）: {len(drop_df):>6} 只")
    if not drop_df.empty:
        for note, cnt in drop_df['note'].value_counts().items():
            logger.info(f"   └ {note}: {cnt} 只")
    logger.info(f"{'=' * 50}")

    if not drop_df.empty:
        logger.warning(f"\n问题股票明细（已删除文件夹，详见 {REPORT_PATH}）：")
        for _, row in drop_df.sort_values('note').iterrows():
            logger.warning(f"  ❌ {row['ts_code']}  {row['note']}")

    if failed_delete:
        logger.error("\n以下文件夹删除失败，请手动处理：")
        for f in failed_delete:
            logger.error(f"  {f}")

    return valid_codes


# ================================================================== #
#  供 build_wide_table.py 调用：直接扫目录
# ================================================================== #

def get_valid_codes() -> list:
    """
    返回当前 daily 目录下所有股票代码

    校核运行后问题股票文件夹已删除，目录天然干净，
    直接扫目录即可，无需读额外的列表文件。
    """
    codes = sorted([
        f.replace('ts_code=', '')
        for f in os.listdir(DAILY_DIR)
        if os.path.isdir(os.path.join(DAILY_DIR, f))
    ])
    logger.info(f"扫描到可用股票: {len(codes)} 只")
    return codes


# ================================================================== #
#  调试入口
# ================================================================== #

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )
    valid_codes = run_validation(max_workers=20)
    print(f"\n最终可用股票数量: {len(valid_codes)} 只")
