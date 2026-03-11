import pandas as pd
import sys
import pytz
import time
import ccxt
import math
import numpy as np
import datetime as dt

from datetime import date, timedelta, datetime, tzinfo, timezone
from datetime import datetime
from loguru import logger
from itertools import cycle

from core.orchestrator import DataSourceConfig
from core.datacenter import DataCenterSrv
from core.algo_strat import AlgoStrategy
from strategy.strat_method import CreateSignal
from core.execution import SignalExecution


# start algo sequence
def algo_seq(BET_SIZE):

    start_time = dt.datetime.now(dt.UTC)
    logger.info(f'Starting algo_seq at (UTC) {start_time.strftime("%Y-%m-%d %H:%M:%S")}\n')

    # 1. Load strategy configuration
    ds = DataSourceConfig()
    ds.create_folder()
    strat_df = ds.load_info_dict()
    logger.info(f'Loaded #{len(strat_df)} rows of strategy configuration\n')

    # 2. Build request / data frame
    dcs = DataCenterSrv(strat_df)
    dcs.create_df()
    logger.info('Data cleaning and update data complete\n')

    # 3. Collect market data
    algo = AlgoStrategy(strat_df)
    algo.data_collect()
    logger.info('Data collection completed\n')

    # 4. Generate trading signals
    gen_signal = CreateSignal(strat_df)
    signal_df = gen_signal.split_sub()
    logger.info(f'Generated {len(signal_df)} signals\n')

    # 5. Execute signals with per-symbol bet sizes
    signal_exec = SignalExecution(signal_df, BET_SIZE)
    signal_exec.create_market_order()
    logger.info(f'Executed market orders with bet_size mapping: {BET_SIZE}\n')

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f'algo_seq finished at (UTC) {end_time_str} (duration: {round(duration, 1)} sec) \n')


if __name__ == '__main__':
    BET_SIZE = {'BTC': 0.001, 'ETH': 0.01, 'SOL': 0.2, 'SUI': 20, 'DOGE': 100}
    algo_seq(BET_SIZE)