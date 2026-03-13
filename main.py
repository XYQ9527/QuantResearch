"""
统一入口 main.py

用法：
    python main.py fetch       拉取数据入库
    python main.py parquet     SQLite 导出为 parquet
    python main.py check       数据三层校核
    python main.py wide        构建宽表（合并+前复权）
    python main.py factors     计算技术指标
    python main.py signals     构造形态信号
    python main.py qlib        同步到 Qlib 格式
    python main.py ic          因子 IC 分析
    python main.py all         全量跑一遍（按顺序）
"""
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)

COMMANDS = {
    'fetch':   '拉取数据入库',
    'parquet': 'SQLite 导出为 parquet',
    'check':   '数据三层校核',
    'wide':    '构建宽表',
    'factors': '计算技术指标',
    'signals': '构造形态信号',
    'qlib':    '同步到 Qlib',
    'ic':      '因子 IC 分析',
    'all':     '全量跑一遍',
}


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'help'

    if cmd == 'fetch':
        from datapipeline.fetch_pipeline import (
            run_trade_cal_pipeline,
            run_stock_basic_pipeline,
            run_daily_pipeline,
            run_adj_factor_pipeline,
            run_daily_basic_pipeline,
        )
        run_trade_cal_pipeline(start_date='20160101', end_date='20301231')
        run_stock_basic_pipeline()
        run_daily_pipeline(start_date='20160101', end_date='20260302')
        run_adj_factor_pipeline(start_date='20160101', end_date='20260302')
        run_daily_basic_pipeline(start_date='20160101', end_date='20260302')

    elif cmd == 'parquet':
        from datapipeline.raw_to_parquet import export_all
        export_all()

    elif cmd == 'check':
        from datapipeline.check_data import run_validation
        valid_codes = run_validation(max_workers=20)
        print(f"\n最终可用股票：{len(valid_codes)} 只")

    elif cmd == 'wide':
        from factor.build_wide_table import build_wide
        build_wide(max_workers=20)

    elif cmd == 'factors':
        from factor.compute_factors import run
        run(max_workers=20)

    elif cmd == 'signals':
        from factor.generate_signals import run
        run(max_workers=20)

    elif cmd == 'qlib':
        from factor.export_qlib import run_dump
        run_dump(max_workers=20)

    elif cmd == 'ic':
        from research.factor_eval import run
        run()

    elif cmd == 'all':
        # 按流水线顺序全量跑一遍
        from datapipeline.fetch_pipeline import (
            run_trade_cal_pipeline, run_stock_basic_pipeline,
            run_daily_pipeline, run_adj_factor_pipeline,
            run_daily_basic_pipeline,
        )
        from datapipeline.raw_to_parquet import export_all
        from datapipeline.check_data import run_validation
        from factor.build_wide_table import build_wide
        from factor.compute_factors import run as run_factors
        from factor.generate_signals import run as run_signals
        from factor.export_qlib import run_dump

        run_trade_cal_pipeline(start_date='20160101', end_date='20301231')
        run_stock_basic_pipeline()
        run_daily_pipeline(start_date='20160101', end_date='20260302')
        run_adj_factor_pipeline(start_date='20160101', end_date='20260302')
        run_daily_basic_pipeline(start_date='20160101', end_date='20260302')
        export_all()
        run_validation(max_workers=20)
        build_wide(max_workers=20)
        run_factors(max_workers=20)
        run_signals(max_workers=20)
        run_dump(max_workers=20)

    else:
        print("\n用法：python main.py <命令>\n")
        print("可用命令：")
        for c, desc in COMMANDS.items():
            print(f"  {c:<10} {desc}")
        print()


if __name__ == '__main__':
    main()