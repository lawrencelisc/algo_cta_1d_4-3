import time
import ccxt
import os
import gc
import sys
import requests
import pandas as pd
from loguru import logger

from threading import Thread
from queue import Queue

from pathlib import Path
from datetime import datetime, timezone
from ccxt.base.exchange import Exchange

from core.orchestrator import DataSourceConfig
from utils.trade_record import TradeRecord
from utils.tg_wrapper import SendTGBot


class TelegramNotifier:
    """統一管理所有 Telegram 通知的類"""

    def __init__(self):
        self.tg = SendTGBot()
        self.queue = Queue()
        self.worker_thread = None
        self._start_worker()

    def _start_worker(self):
        """啟動後台工作線程"""
        if self.worker_thread is None or not self.worker_thread.is_alive():
            self.worker_thread = Thread(target=self._worker, daemon=True)
            self.worker_thread.start()
            logger.info('Telegram worker started')

    def _worker(self):
        """後台線程處理消息隊列"""
        while True:
            message_data = self.queue.get()

            if message_data is None:  # 停止信號
                logger.info('Telegram worker stopped')
                break

            txt_msg = message_data['message']
            context = message_data['context']

            # 簡單重試 2 次
            for attempt in range(1, 3):
                try:
                    success = self.tg.send_df_msg(txt_msg, timeout=20)

                    if success:
                        logger.info(f'✓ TG sent ({context})' + (f' [retry {attempt}]' if attempt > 1 else ''))
                        break
                    elif attempt < 2:
                        logger.warning(f'TG retry {attempt}/2 ({context})')
                        time.sleep(2)
                    else:
                        logger.warning(f'TG failed after 2 attempts ({context})')

                except Exception as e:
                    if attempt < 2:
                        logger.warning(f'TG error, retry {attempt}/2: {type(e).__name__}')
                        time.sleep(2)
                    else:
                        logger.error(f'TG error after 2 attempts ({context}): {e}')

            time.sleep(1)  # 避免限流
            self.queue.task_done()

    def send(self, txt_msg: str, context: str = ""):
        """異步發送消息（立即返回）"""
        self._start_worker()
        self.queue.put({'message': txt_msg, 'context': context})
        logger.info(f'TG queued ({context}), size: {self.queue.qsize()}')

    def wait(self, timeout: int = 60):
        """等待所有消息發送完成"""
        queue_size = self.queue.qsize()
        if queue_size == 0:
            logger.info('✓ No pending TG notifications')
            return True

        logger.info(f'Waiting for {queue_size} TG notifications (timeout: {timeout}s)...')
        try:
            self.queue.join()
            logger.info('All TG notifications sent')
            return True
        except Exception as e:
            logger.error(f'Error waiting for TG: {e}')
            return False

    def stop(self):
        """停止工作線程"""
        if self.worker_thread and self.worker_thread.is_alive():
            self.queue.put(None)
            self.worker_thread.join(timeout=5)
            logger.info('TG worker stopped')


class SignalExecution:

    # initialization
    strat_folder = Path(__file__).parent.parent / 'data' / 'StratData'
    signal_folder = Path(__file__).parent.parent / 'data' / 'Signal'

    prev_signal_filename = 'prev_signal_table.csv'
    signal_filename = 'signal_table.csv'
    signal_plus_filename = 'signal_table_plus.csv'

    prev_signal_path = signal_folder / prev_signal_filename
    signal_path = signal_folder / signal_filename
    signal_plus_path = signal_folder / signal_plus_filename

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    def __init__(self, signal_df: pd.DataFrame, bet_size: dict):
        self.signal_df = signal_df
        self.bet_size = bet_size
        self.tg_notifier = TelegramNotifier()  # 統一的 TG 管理器
        self.load_risk_para_dict()


    # get bybit api via ccxt
    def get_exchange_info(self, symbol: str):
        try:
            bybit_cfg = DataSourceConfig()
            bybit_api = bybit_cfg.load_bybit_api_config(symbol)
            self.bybit = ccxt.bybit({
                'apiKey': bybit_api[symbol + '_1D_API_KEY'],
                'secret': bybit_api[symbol + '_1D_SECRET_KEY'],
                'options': {'adjustForTimeDifference': True},
            })
            self.markets = self.bybit.load_markets()
        except Exception as e:
            logger.exception('Failed to load exchange info for %s: %s', symbol, e)
            raise

        market_symbol = f'{symbol}/USDT:USDT'
        market = self.markets.get(market_symbol)

        if market is None:
            logger.error('No matching market for %s', symbol)

        gc.collect()
        return market


    def get_pos_status(self, symbol: str):
        product_symbol = f'{symbol}USDT'

        market = self.get_exchange_info(symbol)
        position_info_dict: dict = self.bybit.fetch_positions(product_symbol)[0]['info']
        current_leverage = float(position_info_dict.get('leverage', 0))

        side: str = position_info_dict.get('side')
        pos_size: float = abs(float(position_info_dict.get('size')))
        markPrice: str = position_info_dict.get('markPrice')
        balance: float = self.bybit.fetch_balance()
        avg_price: float = position_info_dict.get('avgPrice')
        liq_price: float = position_info_dict.get('liqPrice')

        created_time_unix: float = position_info_dict.get('createdTime')
        created_time_s: int = int(created_time_unix) // 1000
        dt = datetime.utcfromtimestamp(created_time_s)
        created_time = dt.strftime('%y-%m-%d %H:%M')

        position_value: float = position_info_dict.get('positionValue')
        unrealised_pnl: float = position_info_dict.get('unrealisedPnl')
        cum_realised_pnl: float = position_info_dict.get('cumRealisedPnl')

        usdt_bal: float = float(balance['USDT']['total'])

        logger.info(f'Product symbol ({product_symbol}), '
                    f'current price (USDT): {markPrice}. '
                    f'account balance (USDT): {str(usdt_bal)}')

        time.sleep(0.05)

        pos_status = {
            'product_symbol': product_symbol,
            'leverage': current_leverage,
            'side': side,
            'pos_size': pos_size,
            'usdt_bal': usdt_bal,
            'markPrice': markPrice,
            'avg_price': avg_price,
            'liq_price': liq_price,
            'created_time': created_time,
            'position_value': position_value,
            'unrealised_pnl': unrealised_pnl,
            'cum_realised_pnl': cum_realised_pnl
        }

        gc.collect()
        return pos_status


    # ==========================================
    # Final check for order adjustment according to signal generate for each day
    # ==========================================

    def pos_adj(self):
        """倉位調整"""
        tg = SendTGBot()
        trade = TradeRecord(self.signal_df)
        df = self.signal_df.copy()
        bid_df = self.bet_size.copy()
        df['signal'] = df['signal'].astype(int)

        for symbol in df['symbol'].unique():
            symbol_para = self.risk_dict.get(symbol, {'leverage': 2,
                                                      'tp_pct': 60.0,
                                                      'sl_pct': 16.0,
                                                      'trail_sl_pct': 12.0,
                                                      'activation_pct': 10.0}
                                             )
            signal_sum = df.loc[df['symbol'] == symbol, 'signal'].sum()
            target_coin_qty = round(float(bid_df.get(symbol, 0) * signal_sum), 5)

            pos_status = self.get_pos_status(symbol)
            actual_coin_qty: float = float(pos_status.get('pos_size', 0))
            side = pos_status['side']
            mark_price = float(pos_status.get('markPrice', 0))

            if side == 'Sell':
                actual_coin_qty = actual_coin_qty * -1

            adj_coin_qty = target_coin_qty - actual_coin_qty
            adj_usdt_value = abs(adj_coin_qty * mark_price)
            TOLERANCE_USDT = 10.0  # 如果差額價值少於 $10 USD，就忽略不計

            if adj_usdt_value > TOLERANCE_USDT:
                adj_value = abs(adj_coin_qty)
                logger.info(
                    f'[{symbol} Pos Adj] Tgt: {target_coin_qty} | '
                    f'Cur: {actual_coin_qty} | '
                    f'Adj: {adj_coin_qty} (~${adj_usdt_value:.2f})'
                )

                if adj_coin_qty > 0:
                    logger.info(f'trade.long adjustment ({adj_value})')
                    record_df = trade.trade_long(
                        symbol=symbol,
                        total_bet=adj_value,
                        tp_pct=symbol_para['tp_pct'],
                        sl_pct=symbol_para['sl_pct'],
                        trail_sl_pct=symbol_para['trail_sl_pct'],
                        activation_pct=symbol_para['activation_pct'],
                        leverage=int(symbol_para['leverage'])
                    )
                elif adj_coin_qty < 0:
                    logger.info(f'trade.short adjustment ({adj_value})')
                    record_df = trade.trade_short(
                        symbol=symbol,
                        total_bet=adj_value,
                        tp_pct=symbol_para['tp_pct'],
                        sl_pct=symbol_para['sl_pct'],
                        trail_sl_pct=symbol_para['trail_sl_pct'],
                        activation_pct=symbol_para['activation_pct'],
                        leverage=int(symbol_para['leverage'])
                    )

                print(record_df)

                # 更新狀態並發送 TG 通知
                time.sleep(1)
                new_pos_status: dict = self.get_pos_status(symbol)
                txt_msg: str = self.tg_notifier.tg.paradict_to_txt('pos_status (ADJ)', new_pos_status)
                self.tg_notifier.send(txt_msg, f"pos_adj - {symbol}")
            else:
                logger.info(f'{symbol} Position aligned (value difference '
                            f'${adj_usdt_value:.2f} is acceptable), no adj required')



    def prev_signal_df(self):
        signal_df = self.signal_df

        if os.path.exists(self.prev_signal_path):
            try:
                prev_signal_df = pd.read_csv(self.prev_signal_path)
            except Exception as e:
                logger.error(f'Failed to read {self.prev_signal_filename}: {e}')
                prev_signal_df = signal_df.copy()
                prev_signal_df['signal'] = 0
        else:
            prev_signal_df = signal_df.copy()
            prev_signal_df['signal'] = 0

        signal_df.to_csv(self.prev_signal_path, index=False)
        signal_df_s1 = prev_signal_df.copy().reset_index()
        signal_df_s1.rename(columns={'date': 'date_s1', 'signal': 'signal_s1'}, inplace=True)
        signal_df_s1 = signal_df_s1.drop(columns=['name', 'symbol', 'saved_csv'])

        gc.collect()
        return signal_df_s1


    # ==========================================
    # Load (symbol_params_1d.csv) becoming risk_para
    # ==========================================

    def load_risk_para_dict(self):
        try:
            project_root = os.path.dirname(os.path.dirname(__file__))
            csv_path = os.path.join(project_root, 'config', 'symbol_params_1d.csv')
            risk_para_df = pd.read_csv(csv_path)
            self.risk_dict = risk_para_df.set_index('symbol').to_dict('index')
        except FileNotFoundError:
            logger.error('symbol_params_1d.csv.')
            risk_para_df = pd.DataFrame()
            self.risk_dict = {}

        gc.collect()
        return risk_para_df


    def create_market_order(self):
        tg = SendTGBot()
        signal_df = self.signal_df
        trade = TradeRecord(self.signal_df)

        signal_df_s1 = self.prev_signal_df()

        result_signal_df = pd.concat([signal_df.reset_index(), signal_df_s1], axis=1)
        result_signal_df.drop(columns=['index', 'index'], inplace=True)
        result_signal_df = result_signal_df[['date', 'date_s1', 'name', 'symbol', 'saved_csv', 'signal', 'signal_s1']]
        result_signal_df['date'] = pd.to_datetime(result_signal_df['date'])
        result_signal_df['date_s1'] = pd.to_datetime(result_signal_df['date_s1'])
        result_signal_df['signal_plus'] = (result_signal_df['signal_s1'].astype(str) +
                                           result_signal_df['signal'].astype(str))

        print()
        print('===================== result_signal_df =====================')
        print(result_signal_df)

        txt_msg = tg.result_signal_df_to_txt(result_signal_df)
        self.tg_notifier.send(txt_msg, "result_signal_df")

        file_exists = os.path.isfile(self.signal_plus_path)
        result_signal_df.to_csv(
            self.signal_plus_path,
            mode='a',
            index=False,
            header=not file_exists
        )

        # mapping from signal_plus to human‑readable bucket
        signal_map = {
            '11': 'L/L', '10': 'L/0', '1-1': 'L/S',
            '01': '0/L', '00': '0/0', '0-1': '0/S',
            '-11': 'S/L', '-10': 'S/0', '-1-1': 'S/S'
        }

        cols = ['L/L', 'S/L', '0/L', 'L/0', '0/0', 'S/0', '0/S', 'L/S', 'S/S']

        exec_list_df = (
            result_signal_df
            .assign(signal_bulk=lambda d: d['signal_plus'].map(signal_map))
            .assign(signal_bulk=lambda d: pd.Categorical(d['signal_bulk'], categories=cols, ordered=False))
            .pivot_table(
                index='symbol',
                columns='signal_bulk',
                values='signal',
                aggfunc='count',
                fill_value=0,
                observed=False
            )
            .reindex(columns=cols, fill_value=0)
            .rename_axis('index', axis=1)
            .reset_index()
        )

        print()
        print('===================== exec_list_df =====================')
        print(exec_list_df)

        trade = TradeRecord(self.signal_df)
        _10m_traded = trade._10m_traded()
        print('10 min excess? ', _10m_traded)

        if True:
            for _, row in exec_list_df.iterrows():
                symbol: str = row['symbol']
                total_bet: float = 0
                bet_size = float(self.bet_size.get(symbol, 0))

                symbol_para = self.risk_dict.get(symbol, {'leverage': 2,
                                                          'tp_pct': 60.0,
                                                          'sl_pct': 16.0,
                                                          'trail_sl_pct': 12.0,
                                                          'activation_pct': 10.0}
                                                 )

                leverage = int(symbol_para['leverage'])
                tp_pct = float(symbol_para['tp_pct'])
                sl_pct = float(symbol_para['sl_pct'])
                trail_sl_pct = float(symbol_para['trail_sl_pct'])
                activation_pct = float(symbol_para['activation_pct'])


                # row['L/0'] = 1                # < for test
                # row['0/L'] = 1                # < for test
                # row['S/0'] = 1                # < for test
                # row['0/S'] = 1                # < for test

                # Trading signals
                if int(row['L/0']) > 0:
                    total_bet = int(row['L/0']) * bet_size
                    record_df = trade.close_long(symbol,
                                                  total_bet,
                                                  tp_pct,
                                                  sl_pct,
                                                  trail_sl_pct,
                                                  activation_pct
                                                  )
                    print(record_df)
                    if record_df is not None:
                        after_signal_df = result_signal_df[
                            (result_signal_df['symbol'] == symbol) &
                            (result_signal_df['signal_plus'] == '10')
                            ]
                        trade.trade_record_combine(after_signal_df, record_df)
                    else:
                        logger.error(f'{symbol} L/0 order making fail, skip recording')

                if int(row['0/L']) > 0:
                    total_bet = int(row['0/L']) * bet_size
                    record_df = trade.trade_long(symbol,
                                                  total_bet,
                                                  tp_pct,
                                                  sl_pct,
                                                  trail_sl_pct,
                                                  activation_pct,
                                                  leverage
                                                  )
                    print(record_df)
                    if record_df is not None:
                        after_signal_df = result_signal_df[
                            (result_signal_df['symbol'] == symbol) &
                            (result_signal_df['signal_plus'] == '01')
                            ]
                        trade.trade_record_combine(after_signal_df, record_df)
                    else:
                        logger.error(f'{symbol} 0/L order making fail, skip recording')

                if int(row['0/S']) > 0:
                    total_bet = int(row['0/S']) * bet_size
                    record_df = trade.trade_short(symbol,
                                                  total_bet,
                                                  tp_pct,
                                                  sl_pct,
                                                  trail_sl_pct,
                                                  activation_pct,
                                                  leverage
                                                  )
                    print(record_df)
                    if record_df is not None:
                        after_signal_df = result_signal_df[
                            (result_signal_df['symbol'] == symbol) &
                            (result_signal_df['signal_plus'] == '0-1')
                            ]
                        trade.trade_record_combine(after_signal_df, record_df)
                    else:
                        logger.error(f'{symbol} 0/S order making fail, skip recording')

                if int(row['S/0']) > 0:
                    total_bet = int(row['S/0']) * bet_size
                    record_df = trade.close_short(symbol,
                                                  total_bet,
                                                  tp_pct,
                                                  sl_pct,
                                                  trail_sl_pct,
                                                  activation_pct
                                                  )
                    print(record_df)
                    if record_df is not None:
                        after_signal_df = result_signal_df[
                            (result_signal_df['symbol'] == symbol) &
                            (result_signal_df['signal_plus'] == '-10')
                            ]
                        trade.trade_record_combine(after_signal_df, record_df)
                    else:
                        logger.error(f'{symbol} S/0 order making fail, skip recording')

                pos_status: dict = self.get_pos_status(symbol)
                time.sleep(5)
                txt_msg = tg.paradict_to_txt('pos_status (AFTER)', pos_status)
                self.tg_notifier.send(txt_msg, f"pos_status AFTER - {symbol}")

        # 檢查調整
        self.pos_adj()
        time.sleep(5)

        # 等待所有通知發送完成
        self.tg_notifier.wait(timeout=60)

        gc.collect()