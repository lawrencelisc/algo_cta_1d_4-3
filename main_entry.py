import schedule
import time
import json
import requests

import pandas as pd
import datetime as dt

from datetime import date, timedelta, datetime, tzinfo, timezone
from loguru import logger
from io import StringIO

from core.orchestrator import DataSourceConfig
from core.datacenter import DataCenterSrv
from core.algo_strat import AlgoStrategy
from strategy.strat_method import CreateSignal
from core.execution import SignalExecution


def gn_10m_status():
    dt_until = datetime.now(timezone.utc)
    unix_until = int(dt_until.timestamp())
    unix_since = unix_until - (60 * 60)

    hr_start_dt = dt_until.replace(minute=0, second=0, microsecond=0)
    unix_hr_start = int(hr_start_dt.timestamp())

    # Load API config once
    gn_api = DataSourceConfig.load_gn_api_config()
    gn_api_value: str = gn_api.get('GN_API')
    if not gn_api_value:
        logger.error('GN_API key not found in config.')
        return False

    # 0. Load strategy configuration
    ds = DataSourceConfig()
    strat_df = ds.load_info_dict()

    session = requests.Session()
    session.headers.update({'Accept': 'application/json'})

    update_status = []

    for _, row in strat_df.iterrows():
        name: str = str(row['name'])
        symbol: str = str(row['symbol'])
        endpoint_url: str = str(row['url'])
        resolution: str = '10m'

        params = {
            'a': symbol,
            's': unix_since,
            'u': unix_until,
            'api_key': gn_api_value,
            'i': resolution
        }

        try:
            resp = session.get(endpoint_url, params=params, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f'HTTP error for {symbol} at {endpoint_url}: {e}')
            update_status.append(False)  # ✅ 記錄失敗
            continue

        text = resp.text.strip()

        try:
            df_raw = pd.read_json(StringIO(text))
            if df_raw.empty or 't' not in df_raw.columns:
                logger.warning(f'Empty dataframe or missing "t" column for {symbol}')
                update_status.append(False)  # ✅ 記錄失敗
                continue

            last_dt = df_raw['t'].iloc[-1]
            is_updated = last_dt >= unix_hr_start
            update_status.append(is_updated)  # ✅ 記錄狀態

            # ✅ 詳細日誌
            last_dt_str = datetime.fromtimestamp(last_dt, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            hr_start_str = datetime.fromtimestamp(unix_hr_start, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f'{name}: last_dt={last_dt_str}, hr_start={hr_start_str}, updated={is_updated}')

        except Exception as e:
            logger.error(f'Error processing data for {symbol}: {e}')
            update_status.append(False)  # ✅ 記錄失敗
            continue

    # ✅ 只有當 ALL symbols 都更新時才返回 True
    all_updated = all(update_status) if update_status else False

    logger.info(f'Update status: {sum(update_status)}/{len(update_status)} endpoint updated')
    logger.info(f'gn_10m_status() returning: {all_updated}')

    return all_updated


def scheduler(bet_size):
    start_time = datetime.now(timezone.utc)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f'Starting algo_seq at (UTC) {start_time_str}\n')

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
    logger.info(f'Executed market orders with bet_size mapping: {BET_SIZE} \n')

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f'algo_seq finished at (UTC) {end_time_str} (duration: {round(duration, 1)} sec) \n')


if __name__ == '__main__':
    BET_SIZE = {'BTC': 0.002, 'ETH': 0.02, 'SOL': 0.2, 'SUI': 10, 'DOGE': 100}
    each_time: str = '09:15' # ---> modify 9:15 utc

    logger.info('Starting unified scheduler + algo program')


    def check_n_run():
        max_attempts = 16
        scheduler_executed = False

        # Get current time when function is triggered
        trigger_time = datetime.now(timezone.utc)
        logger.info(f'check_n_run() triggered at {trigger_time.strftime("%Y-%m-%d %H:%M:%S")} UTC')

        # Calculate the NEXT 10-minute interval from trigger time
        minutes = trigger_time.minute
        next_10min = ((minutes // 10) + 1) * 10

        if next_10min >= 60:
            start_time = (trigger_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        else:
            start_time = trigger_time.replace(minute=next_10min, second=0, microsecond=0)

        start_timestamp = int(start_time.timestamp())
        logger.info(f'First check will be at {start_time.strftime("%H:%M:%S")} UTC')

        for attempt in range(max_attempts):
            # Calculate target time (every 5 minutes = 300 seconds)
            target_timestamp = start_timestamp + (attempt * 300)
            target_datetime = datetime.fromtimestamp(target_timestamp, tz=timezone.utc)

            logger.info(
                f'Waiting until {target_datetime.strftime("%H:%M:%S")} UTC (attempt {attempt + 1}/{max_attempts})')

            # Wait until target time
            while True:
                current_timestamp = int(time.time())
                if current_timestamp >= target_timestamp:
                    break
                time.sleep(1)

            logger.info(f'Checking gn_10m_status at {target_datetime.strftime("%H:%M:%S")} UTC')

            if gn_10m_status():
                logger.info(f'All endpoints updated at {target_datetime.strftime("%H:%M:%S")} UTC, running scheduler')
                scheduler(BET_SIZE)
                scheduler_executed = True
                break
            else:
                logger.info(f'Not all endpoints updated at {target_datetime.strftime("%H:%M:%S")} UTC')

        if not scheduler_executed:
            logger.warning(f'Max attempts ({max_attempts}) reached, gn_10m_status() never returned True')


    schedule.every().day.at(each_time).do(check_n_run)
    current_time = datetime.now(timezone.utc)

    logger.info(f'Scheduler configured to run daily at {each_time} UTC')
    logger.info(f'Current time: {current_time.strftime("%Y-%m-%d %H:%M:%S")} UTC')

    scheduled_time = current_time.replace(
        hour=int(each_time.split(':')[0]),
        minute=int(each_time.split(':')[1]),
        second=0,
        microsecond=0
    )

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning('KeyboardInterrupt received; program terminated.')