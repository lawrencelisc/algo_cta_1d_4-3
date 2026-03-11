import os
import gc
import ast
import json
import pytz
import requests
import pandas as pd
import warnings
import ccxt

from io import StringIO
from loguru import logger
from pathlib import Path
from datetime import datetime, timezone

from core.orchestrator import DataSourceConfig
from ccxt.base.exchange import Exchange



class DataCenterSrv:
    warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")
    dict_output_key = 'o'
    data_folder_GN = Path(__file__).parent.parent / 'data' / 'GrassNodeData'


    def __init__(self, strat_df: pd.DataFrame):
        self.strat_df = strat_df


    def get_exchange_trade(self, symbol: str):
        market_symbol = f'{symbol}/USDT:USDT'
        try:
            bybit_cfg = DataSourceConfig()
            bybit_api = bybit_cfg.load_bybit_api_config(symbol)
            self.bybit = ccxt.bybit({
                'apiKey': bybit_api[symbol + '_1D_API_KEY'],
                'secret': bybit_api[symbol + '_1D_SECRET_KEY'],
                'enableRateLimit': True,
                'options': {'default': 'swap'},
            })
            self.markets = self.bybit.load_markets()
        except Exception as e:
            logger.exception("Failed to load exchange info for %s: %s", symbol, e)
            raise
        try:
            market = self.markets[market_symbol]
            return market
        except KeyError:
            logger.error("No matching market for %s", symbol)
            return None


    def create_df(self):
        # Validations
        if self.strat_df is None or self.strat_df.empty:
            logger.error('strat_df is empty or None. Provide a non-empty DataFrame.')
            return

        required_cols = {'name', 'symbol', 'url', 'endpt_col'}
        missing = required_cols - set(self.strat_df.columns)
        if missing:
            logger.error(f'strat_df missing required columns: {missing}')
            return

        # Load API config once
        gn_api = DataSourceConfig.load_gn_api_config()
        gn_api_value: str = gn_api.get('GN_API')
        if not gn_api_value:
            logger.error('GN_API key not found in config.')
            return

        # Time window
        since_iso = '2020-01-01 00:00:00+00:00'
        dt_since = datetime.strptime(since_iso, '%Y-%m-%d %H:%M:%S%z')
        dt_until = datetime.now(timezone.utc)

        unix_since_default = int(dt_since.timestamp())
        unix_until = int(dt_until.timestamp())

        dict_output = self.dict_output_key
        session = requests.Session()
        session.headers.update({'Accept': 'application/json'})


        def data_cleaning_dict(x):
            # Normalizes cell to dict or None
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            if isinstance(x, dict):
                return x
            strip_data = str(x).strip()
            for parser in (json.loads, ast.literal_eval):
                try:
                    parsed = parser(strip_data)
                    return parsed if isinstance(parsed, dict) else None
                except Exception:
                    continue
            return None


        def gn_create_df(endpoint_url: str, symbol: str, unix_since: int, unix_until_local: int, resolution: str):
            params = {
                'a': symbol,
                's': unix_since,
                'u': unix_until_local,
                'api_key': gn_api_value,
                'i': resolution
            }

            try:
                resp = session.get(endpoint_url, params=params, timeout=60)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.error(f'HTTP error for {symbol} at {endpoint_url}: {e}')
                return pd.DataFrame()

            # If server returns empty or non-JSON content
            text = resp.text.strip()
            if not text:
                logger.warning(f'Empty response for {symbol}.')
                return pd.DataFrame()

            try:
                df_raw = pd.read_json(StringIO(text), convert_dates=['t'])
            except ValueError as e:
                logger.error(f'JSON decode error for {symbol}: {e}')
                return pd.DataFrame()

            if df_raw.empty:
                return pd.DataFrame()

            if dict_output in df_raw.columns:
                df_raw[dict_output] = df_raw[dict_output].apply(data_cleaning_dict)
                # Some rows may be None after cleaning; drop them
                df_raw = df_raw.dropna(subset=[dict_output])
                if df_raw.empty:
                    return pd.DataFrame()
                result_df = pd.json_normalize(df_raw[dict_output])
                df2 = pd.concat([df_raw.drop(columns=[dict_output]), result_df], axis=1)
            else:
                df2 = df_raw

            # Set index to timestamp if present
            if 't' in df2.columns:
                df2['t'] = pd.to_datetime(df2['t'], utc=True)
                df2 = df2.set_index('t')
            df2.index.name = 'date'
            return df2.sort_index()


        # load data from GN glassnode datasource for each strategy
        for _, row in self.strat_df.iterrows():
            combined_df = pd.DataFrame()

            name: str = str(row['name'])
            symbol: str = str(row['symbol'])
            endpoint_url: str = str(row['url'])
            endpt_col: str = str(row['endpt_col'])
            resolution: str = '1h'

            filename: str = f'{name}_{symbol}.csv'
            filename_ap: str = f'{name}_{symbol}_ap.csv'
            file_path = self.data_folder_GN / filename
            file_path_ap = self.data_folder_GN / filename_ap

            # Determine fetch_since based on existing file
            if not file_path.exists():
                # Fresh full download
                df_new = gn_create_df(endpoint_url, symbol, unix_since_default, unix_until, resolution)
                if df_new.empty:
                    logger.warning(f'No data returned for {symbol}; skipping write.')

                # For CSV, write with date index in UTC ISO
                try:
                    df_new.to_csv(file_path)
                    logger.info(f'Created file with {len(df_new)} row(s) for {symbol}: {filename}')
                except Exception as e:
                    logger.error(f'Failed to save CSV for {symbol}: {e}')

            # Update existing file
            try:
                existing_df = pd.read_csv(file_path, index_col=0)
            except Exception as e:
                logger.error(f'Failed to read existing CSV {filename}: {e}')
                existing_df = pd.DataFrame()

            if existing_df.empty:
                # Treat as fresh
                df_new = gn_create_df(endpoint_url, symbol, unix_since_default, unix_until, gn_create_df)
                existing_df = df_new.copy()
                if df_new.empty:
                    logger.warning(f'No data returned for {symbol}; skipping write.')
                try:
                    df_new.to_csv(file_path)
                    logger.info(f'Overwrite empty file with {len(df_new)} rows for {symbol}: {filename}')
                except Exception as e:
                    logger.error(f'Failed to save CSV for {symbol}: {e}')


            existing_df.index = pd.to_datetime(existing_df.index, utc=True)
            existing_df.index.name = 'date'
            existing_df = existing_df.sort_index()
            latest_ts = existing_df.index[-1]
            unix_latest_ts = int(latest_ts.timestamp())
            until_ts = pd.Timestamp.now(tz='UTC').floor('min')

            # Fetch from the next day after latest timestamp
            fetch_since = int((latest_ts + pd.Timedelta(hours=1)).timestamp())
            unix_diff = int(unix_until - fetch_since)
            unix_gn_get_since = unix_latest_ts

            if True:
                df_new = gn_create_df(endpoint_url, symbol, fetch_since, unix_until, resolution)
                combined_df = pd.concat(
                    [existing_df.dropna(how='all', axis=1),
                     df_new.dropna(how='all', axis=1)],
                    axis=0
                )
                latest_ts_combine = combined_df.index[-1]
                unix_latest_ts_combine = int(latest_ts_combine.timestamp())
                unix_gn_get_since = unix_latest_ts_combine
                try:
                    combined_df.to_csv(file_path)
                    logger.info(f'Updated {filename}: +{len(combined_df) - len(existing_df)} new rows.')
                except Exception as e:
                    logger.error(f'Failed to update CSV for {symbol}: {e}')
            else:
                logger.info(f'No new data available for "{filename}".')

            resolution = '10m'
            df_10m_new = gn_create_df(endpoint_url, symbol, unix_gn_get_since, unix_until, resolution)
            df_1h_new = df_10m_new.resample('h').last()
            df_1h_new = df_1h_new.iloc[1:]

            if combined_df.empty:
                gn_combined_df = pd.concat(
                    [existing_df.dropna(how='all', axis=1),
                     df_1h_new.dropna(how='all', axis=1)],
                    axis=0
                )
            else:
                gn_combined_df = pd.concat(
                    [combined_df.dropna(how='all', axis=1),
                     df_1h_new.dropna(how='all', axis=1)],
                    axis=0
                )

                try:
                    gn_combined_df.to_csv(file_path_ap)
                    logger.info(
                        f'Modify the existing file with {len(df_new)} (hourly) gn row(s) + '
                        f'{len(df_1h_new)} (10min>hourly) gn row(s) for {symbol}: {filename_ap}'
                                )
                except Exception as e:
                    logger.error(f'Failed to save CSV for {symbol}: {e}')

        gc.collect
        return