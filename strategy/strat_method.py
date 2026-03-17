import pandas as pd
import numpy as np
import pytz
import sys
import os

from pathlib import Path
from loguru import logger
from datetime import datetime


class CreateSignal:
    # constant
    strat_list = ['zscore', 'sma_candle']
    strat_folder = Path(__file__).parent.parent / 'data' / 'StratData'
    signal_folder = Path(__file__).parent.parent / 'data' / 'Signal'
    signal_filename = 'signal_table.csv'
    signal_path = signal_folder / signal_filename


    def __init__(self, strat_df: pd.DataFrame):
        self.strat_df = strat_df


    def _zscore_pos_loop(self, z: np.ndarray, mode: str, enter_thres: float,
                              exit_thres: float, confirm_bars: int, min_hold: int,
                              cooldown: int):
        n = len(z)
        pos = np.zeros(n, dtype=int)

        bar_pos = 0
        cooldown_num = 0
        up_count = 0
        dn_count = 0

        allow_long = mode in ('long', 'long_short')
        allow_short = mode in ('short', 'long_short')

        for i in range(n):
            zi = z[i]

            if np.isnan(zi):
                pos[i] = pos[i - 1] if i > 0 else 0
                continue

            prev_pos = pos[i - 1] if i > 0 else 0
            curr_pos = prev_pos

            # decrement cooldown
            if cooldown_num > 0:
                cooldown_num -= 1

            # update bars-in-position counter based on prev_pos
            if prev_pos != 0:
                bar_pos += 1
            else:
                bar_pos = 0

            can_exit = (bar_pos >= min_hold)

            # During position or cooldown, don't accumulate confirmation counts
            if prev_pos != 0 or cooldown_num > 0:
                up_count = 0
                dn_count = 0
            else:
                up_cond = (zi >= +abs(enter_thres))
                dn_cond = (zi <= -abs(enter_thres))
                up_count = up_count + 1 if up_cond else 0
                dn_count = dn_count + 1 if dn_cond else 0

            # exits
            if prev_pos == +1:
                if can_exit and zi <= +abs(exit_thres):
                    curr_pos = 0
                    if cooldown > 0:
                        cooldown_num = cooldown
                    bar_pos = 0
            elif prev_pos == -1:
                if can_exit and zi >= -abs(exit_thres):
                    curr_pos = 0
                    if cooldown > 0:
                        cooldown_num = cooldown
                    bar_pos = 0

            # entries
            if (curr_pos == 0) and (cooldown_num == 0):
                if allow_long and up_count >= confirm_bars:
                    curr_pos = +1
                    bar_pos = 0
                    up_count = dn_count = 0
                elif allow_short and dn_count >= confirm_bars:
                    curr_pos = -1
                    bar_pos = 0
                    up_count = dn_count = 0

            pos[i] = curr_pos
        return pos


    def strat_zscore(self, row):
        # ================== get param from su_table.csv
        name: str = str(row['name'])
        symbol: str = str(row['symbol'])
        endpt_col: str = str(row['endpt_col'])
        strat: str = str(row['strat'])
        mode: str = str(row['mode'])
        rol: int = int(row['rol'])

        enter_thres: float = float(row['thres'])
        exit_thres: float = float(row['exit_thres'])
        confirm_bars: int = int(row['confirm_bars'])
        min_hold: int = int(row['min_hold'])
        cooldown: int = int(row['cooldown'])

        strat_filename: str = f'{name}_{endpt_col}_{symbol}.csv'
        file_path = self.strat_folder / strat_filename
        df = pd.read_csv(file_path, index_col=0)
        df[endpt_col] = df[endpt_col].astype('float64')

        # ================== strategy calculation
        df['ma'] = df[endpt_col].rolling(rol).mean().astype('float64')
        df['std'] = df[endpt_col].rolling(rol).std().astype('float64')
        df[strat] = ((df[endpt_col] - df['ma']) / df['std']).astype('float64')

        z = df[strat].to_numpy(dtype=float)

        # call the loop function
        pos = self._zscore_pos_loop(z, mode, enter_thres, exit_thres,
                                         confirm_bars, min_hold, cooldown)

        df['pos'] = pos
        df.to_csv(file_path)
        signal = int(df['pos'].iloc[-1])

        return signal

    # ==========================================
    # Generate signal table if strategy is ma & candle length
    # ==========================================

    def strat_ma_candle(self, row):

        signal: int = 0
        name: str = str(row['name'])
        symbol: str = str(row['symbol'])
        endpt_col: str = str(row['endpt_col'])
        res: str = str(row['res'])
        strat: str = str(row['strat'])
        mode: str = str(row['mode'])
        candle_len: int = int(row['candle_len'])
        rol: int = int(row['rol'])
        num_std: int = int(row['num_std'])

        # 定義 5x ATR 狂暴系名單 (放寬至 50 bars)
        wild_coins = ['DOGE', 'SUI']
        # 定義 3x ATR 穩陣系名單 (收緊至 20 bars)
        stable_coins = ['BTC', 'ETH', 'SOL']

        if symbol in wild_coins:
            dynamic_cycle = 50
        elif symbol in stable_coins:
            dynamic_cycle = 20
        else:
            # 如果唔喺名單上面，用返 CSV 設定檔個數做保底
            dynamic_cycle = int(row['cycle'])

        strat_filename = str(row['name'] + '_' + row['res'] + '_' + row['symbol'] + '.csv')
        file_path = self.strat_folder / res / strat_filename

        df = pd.read_csv(file_path, index_col=0)
        df[endpt_col] = df[endpt_col].astype('float64')
        df[endpt_col] = df[endpt_col].replace('', np.nan)

        df['candle'] = (df['close'] / df['open'] - 1).astype('float64')
        df['pct'] = df[endpt_col].pct_change().astype('float64')
        df['sma'] = df[endpt_col].rolling(rol).mean().astype('float64')
        df['std'] = df[endpt_col].rolling(rol).std().astype('float64')
        df['std_raito'] = ((df['sma'] - df[endpt_col]) / df['std']).astype('float64')

        sma_dir, candle_dir = mode.split('&')

        if candle_dir == 'positive':
            df['candle_pos'] = df['candle'] > (candle_len * 0.01)
        elif candle_dir == 'negative':
            df['candle_pos'] = df['candle'] < (-1 * candle_len * 0.01)

        if sma_dir == 'above':
            df['sma_pos'] = (df[endpt_col] > df['sma']) & (df['std_raito'] < -1 * num_std)
        elif sma_dir == 'below':
            df['sma_pos'] = (df[endpt_col] < df['sma']) & (df['std_raito'] > num_std)

        if candle_dir == 'positive':
            if 'sma_pos' in df.columns:
                df['pos'] = np.where(df['candle_pos'] & df['sma_pos'], 1, 0)
            else:
                df['pos'] = np.where(df['candle_pos'], 1, 0)
        elif candle_dir == 'negative':
            if 'sma_pos' in df.columns:
                df['pos'] = np.where(df['candle_pos'] & df['sma_pos'], -1, 0)
            else:
                df['pos'] = np.where(df['candle_pos'], -1, 0)

        blocks = (df['pos'] != df['pos'].shift()).cumsum()
        cum_counts = df.groupby(blocks).cumcount() + 1
        df['pos'] = np.where((df['pos'] != 0) & (cum_counts >= cycle), 0, df['pos'])

        df.to_csv(file_path)
        signal = df['pos'].iloc[-1]

        return signal


    def split_sub(self):
        count = 0
        new_signal_df = pd.DataFrame()
        combine_signal_df = pd.DataFrame(
            {
                'name': pd.Series(dtype='string'),
                'symbol': pd.Series(dtype='string'),
                'saved_csv': pd.Series(dtype='string'),
                'signal': pd.Series(dtype='int64'),
            }
        )

        strategy_funcs = {
            self.strat_list[0]: self.strat_zscore,                      # 0 - zscore
            self.strat_list[1]: self.strat_ma_candle,                   # 1 - ma & candle
        }

        for _, row in self.strat_df.iterrows():
            func = strategy_funcs.get(row['strat'])
            if func:
                func(row)
                count += 1
                str_saved_csv = str(row['name'] + '_' + row['res'] + '_' + row['symbol'] + '.csv')
                strat_path = str(self.strat_folder / row['res'] / str_saved_csv)

                df = pd.read_csv(strat_path, index_col=0)
                lastest_date = str(df.index[-1])

                new_row = {
                    'date': lastest_date,
                    'name': str(row['name']),
                    'symbol': str(row['symbol']),
                    'saved_csv': str_saved_csv,
                    'signal': str(df.iloc[-1]['pos'])
                }

                new_row_df = pd.DataFrame([new_row])
                new_row_df['date'] = pd.to_datetime(new_row_df['date'])
                new_row_df = new_row_df.set_index('date')

                frames = [df for df in [combine_signal_df, new_row_df] if df is not None and not df.empty]
                if frames:
                    combine_signal_df = pd.concat(frames, axis=0)
                else:
                    combine_signal_df = pd.DataFrame()

                # Try to load existing CSV if it exists
                if os.path.exists(self.signal_path):
                    try:
                        existing_signal_df = pd.read_csv(self.signal_path)
                    except pd.errors.EmptyDataError:
                        existing_signal_df = pd.DataFrame()
                        logger.error(f'Failed to read existing CSV {self.signal_filename}: {e}')
                else:
                    existing_signal_df = pd.DataFrame()

        # If the CSV is empty or missing, create it
        combine_signal_df = combine_signal_df.reset_index()
        combine_signal_df['date'] = combine_signal_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        if existing_signal_df.empty:
            combine_signal_df.to_csv(self.signal_path, index=False)
        else:
            combined_df = pd.concat([existing_signal_df, combine_signal_df], ignore_index=True)
            combined_df.to_csv(self.signal_path, index=False)
        logger.info(f'Updated {self.signal_filename}: +{count} new rows.')

        return combine_signal_df