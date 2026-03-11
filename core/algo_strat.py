import gc
import pandas as pd
import numpy as np
from loguru import logger
from pathlib import Path


class AlgoStrategy:
    data_folder_GN = Path(__file__).parent.parent / 'data' / 'GrassNodeData'

    res_list = ['24h']

    def __init__(self, strat_df: pd.DataFrame):
        self.strat_df = strat_df
        self._p_data_cache = {}


    def data_collect(self):

        for res in self.res_list:
            sub_folder = res
            strat_folder = Path(__file__).parent.parent / 'data' / 'StratData' / sub_folder
            strat_folder.mkdir(parents=True, exist_ok=True)

            required_cols = {'name', 'symbol', 'endpt_col', 'strat'}
            missing = required_cols - set(self.strat_df.columns)
            if missing:
                logger.error(f'strat_df missing required columns: {missing}')
                return

            p_strat_df = self.strat_df[
                (self.strat_df['name'].str.startswith('GN')) |
                (self.strat_df['endpt_col'] == 'close')
                ]

            np_strat_df = self.strat_df[
                ~((self.strat_df['name'].str.startswith('GN')) |
                  (self.strat_df['endpt_col'] == 'ohlc'))
            ].reset_index(drop=True)

            np_by_symbol = np_strat_df.groupby('symbol')

            try:
                for _, p_row in p_strat_df.iterrows():
                    p_name = str(p_row['name'])
                    p_symbol = str(p_row['symbol'])
                    p_endpt_col = str(p_row['endpt_col'])
                    p_filename = f'{p_name}_{p_symbol}_ap.csv'
                    p_file_path = self.data_folder_GN / p_filename

                    p_df = pd.read_csv(p_file_path, index_col=0)
                    p_df.index = pd.to_datetime(p_df.index)
                    p_df = p_df.rename(columns={
                        'c': 'close',
                        'h': 'high',
                        'l': 'low',
                        'o': 'open'
                    })

                    p_df_xh = p_df.resample(res, offset='0h').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last'
                    })

                    result_df = p_df_xh.copy()

                    output_filename = f'{p_name}_{res}_{p_symbol}.csv'
                    output_path = strat_folder / output_filename
                    result_df.to_csv(output_path, date_format='%Y-%m-%d %H:%M:%S')
                    logger.info(f'File saved ({output_filename}) with {len(result_df)} rows')

                logger.info(f'Aggregation ....... completed\n')
                self._p_data_cache.clear()
                gc.collect()

            except Exception as e:
                logger.error(f'Data collection failed: {e}')
                self._p_data_cache.clear()
                raise