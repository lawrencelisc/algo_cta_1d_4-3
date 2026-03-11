import os
import time
import ccxt
import gc
import csv
import sys

import pandas as pd
import numpy as np

from pathlib import Path
from loguru import logger
from datetime import datetime, timezone
from ccxt.base.exchange import Exchange
from core.orchestrator import DataSourceConfig


class TradeRecord:

    def __init__(self, signal_df: pd.DataFrame):
        self.signal_df = signal_df
        trade_folder = Path(__file__).parent.parent / 'data' / 'Trade'
        trade_folder.mkdir(parents=True, exist_ok=True)

        trade_filename: str = 'bybit_trade_record.csv'
        trade_hist_filename: str = 'bybit_trade_hist.csv'
        after_signal_filename: str = 'after_signal_df.csv'
        return_filename: str = 'return_df.csv'

        self.file_path_trade = trade_folder / trade_filename
        self.file_path_trade_hist = trade_folder / trade_hist_filename
        self.file_path_si = trade_folder / after_signal_filename
        self.file_path_re = trade_folder / return_filename

        signal_folder = Path(__file__).parent.parent / 'data' / 'Signal'
        prev_signal_filename = 'prev_signal_table.csv'
        signal_filename = 'signal_table.csv'
        self.prev_signal_path = signal_folder / prev_signal_filename
        self.signal_path = signal_folder / signal_filename

        self.file_path_trade.touch(exist_ok=True)
        self.file_path_trade_hist.touch(exist_ok=True)
        self.file_path_si.touch(exist_ok=True)
        self.file_path_re.touch(exist_ok=True)

        self.col_order = [
            'timestamp',
            'datetime',
            'symbol',
            'order',
            'type',
            'side',
            'takerOrMaker',
            'price',
            'amount',
            'cost',
            'info.symbol',
            'info.orderType',
            'info.underlyingPrice',
            'info.orderLinkId',
            'info.orderId',
            'info.stopOrderType',
            'info.execTime',
            'info.feeCurrency',
            'info.createType',
            'info.execFeeV2',
            'info.feeRate',
            'info.tradeIv',
            'info.blockTradeId',
            'info.markPrice',
            'info.execPrice',
            'info.markIv',
            'info.orderQty',
            'info.orderPrice',
            'info.execValue',
            'info.closedSize',
            'info.execType',
            'info.seq',
            'info.side',
            'info.indexPrice',
            'info.leavesQty',
            'info.isMaker',
            'info.execFee',
            'info.execId',
            'info.marketUnit',
            'info.execQty',
            'info.extraFees',
            'info.nextPageCursor',
            'fee.currency',
            'fee.cost',
            'fee.rate',
            'fees.currency',
            'fees.cost',
            'fees.rate'
        ]

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

    def _10m_traded(self):
        unix_now_ts: float = float(datetime.now(timezone.utc).timestamp())
        if os.path.exists(self.prev_signal_path):
            try:
                df = pd.read_csv(self.prev_signal_path)
            except pd.errors.EmptyDataError as e:
                df = pd.DataFrame()
                logger.error(f'Failed to read existing CSV {self.prev_signal_path}: {e}')
                return False
            except Exception as e:
                logger.error(f'Error reading {self.prev_signal_path}: {e}')
                return False
        else:
            df = pd.DataFrame()

        if df.empty:
            return False

        prev_dates = sorted(df['date'].unique())
        prev_last_date = prev_dates[-1]
        unix_prev_last_ts: float = float((pd.to_datetime(prev_last_date)).timestamp())
        unix_diff_ts: float = (unix_now_ts - unix_prev_last_ts)

        return (unix_diff_ts > 10 * 60)

    def trade_record_combine(self, after_signal_df: pd.DataFrame, record_df: pd.DataFrame):
        def is_file_empty(path):
            return os.path.getsize(path) == 0

        file_si_empty = is_file_empty(self.file_path_si)
        after_signal_df.to_csv(
            self.file_path_si,
            mode='a',
            index=False,
            header=file_si_empty
        )

        file_re_empty = is_file_empty(self.file_path_re)
        record_df.to_csv(
            self.file_path_re,
            mode='a',
            index=False,
            header=file_si_empty
        )
        strat_hist_df = after_signal_df.copy()

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        if (len(record_df) > 1):
            out = {}
            for col in record_df.columns:
                col_val = record_df[col]
                col_num = pd.to_numeric(col_val, errors='coerce')

                is_numeric = pd.api.types.is_numeric_dtype(col_val) or not col_num.isna().all()
                first_val = col_val.iloc[0] if len(col_val) else np.nan
                all_same = col_val.eq(first_val).all()

                if all_same:
                    out[col] = first_val
                elif is_numeric:
                    out[col] = col_val.sum(skipna=True)
                else:
                    out[col] = first_val

            rec_df = pd.DataFrame([out])
            num_cols = rec_df.select_dtypes(include=[np.number]).columns
            rec_df[num_cols] = rec_df[num_cols].replace(0, np.nan)
        else:
            rec_df = record_df.iloc[[0]].copy()
        print(rec_df)
        rec_dict: dict = rec_df.to_dict('records')[0]
        row_count: int = len(strat_hist_df)

        if row_count == 0:
            logger.warning('after_signal_df (row_count=0), no data is required appended')
            return

        strat_hist_df['order_id'] = rec_dict['order']
        strat_hist_df['t_timestamp'] = rec_dict['timestamp']
        strat_hist_df['t_datetime'] = rec_dict['datetime']
        strat_hist_df['t_type'] = rec_dict['type']
        strat_hist_df['side'] = rec_dict['side']
        strat_hist_df['takerOrMaker'] = rec_dict['takerOrMaker']
        strat_hist_df['price'] = rec_dict['price']
        strat_hist_df['amount'] = float(rec_dict['amount']) / row_count
        strat_hist_df['cost'] = float(rec_dict['cost']) / row_count
        strat_hist_df['product_symbol'] = rec_dict['info.symbol']
        strat_hist_df['feeCurrency'] = rec_dict['fee.currency']
        strat_hist_df['fee.cost'] = float(rec_dict['fee.cost']) / row_count
        strat_hist_df['fee.rate'] = rec_dict['fee.rate']
        strat_hist_df.drop('date_s1', axis=1, inplace=True)
        strat_hist_df.drop('signal_s1', axis=1, inplace=True)
        strat_hist_df.drop('signal_plus', axis=1, inplace=True)

        print(strat_hist_df)
        write_header = not os.path.exists(self.file_path_trade_hist) or \
                       os.path.getsize(self.file_path_trade_hist) == 0
        strat_hist_df.to_csv(self.file_path_trade_hist, mode='a', header=write_header, index=False)

    def record_to_df(self, trade_param_list: any):
        row = []
        for t_element in trade_param_list:
            base = {t_name: t_data for t_name, t_data in t_element.items() if t_name not in ("info", "fee", "fees")}
            info = {f"info.{t_name}": t_data for t_name, t_data in (t_element.get("info") or {}).items()}
            single_fee = {f"fee.{t_name}": t_data for t_name, t_data in (t_element.get("fee") or {}).items()}

            fees_list = t_element.get("fees") or [{}]  # ensure at least one row
            for f in fees_list:
                feeitem = {f"fees.{t_name}": f.get(t_name) for t_name in ("currency", "cost", "rate")}
                row.append({**base, **info, **single_fee, **feeitem})
        return row

    # ==========================================
    # Helper Function: 動態決定價格小數點位數 (防止 Bybit 精度報錯)
    # ==========================================

    def _get_price_precision(self, price: float) -> int:
        """根據幣價級別回傳適合的小數點位數"""
        if price >= 1000:
            return 1  # BTC, ETH 等大幣
        elif price >= 10:
            return 3  # SOL 等中價幣
        elif price >= 1:
            return 4  # SUI 等低價幣
        else:
            return 5  # DOGE 等極低價幣

    # ==========================================
    # Helper Function: Clear legacy protection orders (TP/SL/Trailing) for a specified direction
    # ==========================================
    def _clear_pos_protection(self, symbol: str, position_idx: int):
        """Clear all stop orders and trailing stop for a position"""

        # 1. Cancel all conditional TP/SL orders
        product_symbol = f'{symbol}USDT'
        try:
            response = self.bybit.private_get_v5_order_realtime({
                'category': 'linear',
                'symbol': product_symbol,
                'orderFilter': 'StopOrder'
            })

            orders = response.get('result', {}).get('list', [])
            for order in orders:
                if int(order.get('positionIdx', 0)) == position_idx:
                    try:
                        self.bybit.private_post_v5_order_cancel({
                            'category': 'linear',
                            'symbol': product_symbol,
                            'orderId': order['orderId']
                        })
                        logger.info(f"canceled stop order: {order['orderId']}")
                    except Exception as e:
                        if '110001' in str(e):
                            continue
                        logger.warning(f"Failed to cancel order {order['orderId']}: {e}")

        except Exception as e:
            logger.error(f'Error fetching stop orders: {e}')

        # 2. Clear trailing stop on position
        try:
            self.bybit.private_post_v5_position_trading_stop({
                'category': 'linear',
                'symbol': product_symbol,
                'positionIdx': position_idx,
                'trailingStop': '0'
            })
            logger.info(f'Cleared trailing stop (PositionIdx: {position_idx})')
        except Exception as e:
            ignored_errors = ('not modified', '34040')
            if any(err in str(e).lower() for err in ignored_errors):
                pass
            else:
                logger.error(f'Failed to clear trailing stop: {e}')

    # ==========================================
    # 平多單 / 減多單邏輯 (Logic for Close Long)
    # ==========================================
    def close_long(self, symbol: str,
                   total_bet: float,
                   tp_pct: float = None,
                   sl_pct: float = None,
                   trail_sl_pct: float = None,
                   activation_pct: float = None
                   ):

        self.get_exchange_trade(symbol)
        product_symbol = f'{symbol}USDT'
        position_idx = 1  # 1 代表 Hedge Mode 下的多單

        # 1. 抓取目前倉位狀態
        market = self.get_exchange_trade(symbol)
        pos_res = self.bybit.private_get_v5_position_list({'category': 'linear', 'symbol': product_symbol})
        long_pos = next((p for p in pos_res['result']['list'] if int(p['positionIdx']) == position_idx), None)

        if not long_pos or float(long_pos['size']) == 0:
            logger.info(f'No long position found for {symbol}, no need to reduce/close position.')
            return

        current_size = float(long_pos['size'])
        leverage = float(long_pos['leverage'])

        # 🛑 隱患一修正：防呆處理 avgPrice 空值
        raw_avg_price = long_pos.get('avgPrice', 0)
        avg_price = float(raw_avg_price) if raw_avg_price else 0.0

        close_amount = total_bet

        if close_amount <= 0 or close_amount > current_size:
            logger.info(f'Invalid close amount ({close_amount}). Current pos: {current_size}')
            return

        remaining_size = round(current_size - close_amount, 4)

        logger.info(f'Target pos: {total_bet} | Current pos: {current_size}')
        logger.info(f'Preparing to close {close_amount} {symbol} '
                    f'long pos (Expected remaining after close: {remaining_size})')

        # 2. 發送市價單平倉 (ReduceOnly 確保只減倉)
        try:
            order = self.bybit.create_order(
                symbol=product_symbol,
                type='market',
                side='sell',  # 多單平倉要 Sell
                amount=close_amount,
                price=None,
                params={
                    'category': 'linear',
                    'positionIdx': position_idx,
                    'reduceOnly': True,
                    'timeInForce': 'IOC'
                }
            )
            logger.info(f"Long pos closed successfully, OrderID: {order.get('id', 'N/A')}")
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Failed to close pos: {e}")
            return

        # 3. 砍掉重練：清理舊的所有防護網
        self._clear_pos_protection(symbol, position_idx)

        # 4. 如果手上還有剩餘倉位，重新掛上防護網 (Reset)
        if remaining_size > 0:
            if avg_price == 0:
                logger.warning(f"Warning: avgPrice is 0 for {symbol}. Cannot recalculate TP/SL.")
                return

            logger.info(f'Re-establishing protections for the remaining {remaining_size} long contracts...')

            # 取得動態小數點精度
            tick_decimals = self._get_price_precision(avg_price)

            # (A) 獨立 TP & Hard SL (雙標計算，除以槓桿)
            tp_sl_params = {
                'category': 'linear',
                'symbol': product_symbol,
                'positionIdx': position_idx,
                'tpslMode': 'Partial',
                'tpSize': str(remaining_size),
                'slSize': str(remaining_size),
            }

            has_tp_sl = False
            if tp_pct is not None:
                tp_price = round(avg_price * (1 + tp_pct / 100), tick_decimals)
                tp_sl_params['takeProfit'] = str(tp_price)
                has_tp_sl = True

            if sl_pct is not None:
                real_sl_price_pct = sl_pct / leverage
                sl_price = round(avg_price * (1 - real_sl_price_pct / 100), tick_decimals)
                tp_sl_params['stopLoss'] = str(sl_price)
                has_tp_sl = True

            if has_tp_sl:
                self.bybit.private_post_v5_position_trading_stop(tp_sl_params)
                logger.info(f'new Partial TP ({tp_price}) / SL ({sl_price}) config')

            # (B) 重新設定移動止損 (距離除以槓桿)
            if trail_sl_pct is not None:
                real_trail_pct = trail_sl_pct / leverage
                new_trail_dist = round(avg_price * (real_trail_pct / 100), tick_decimals)
                new_activation = round(avg_price * (1 + activation_pct / 100), tick_decimals) if activation_pct else 0

                trailing_params = {
                    'category': 'linear',
                    'symbol': product_symbol,
                    'positionIdx': position_idx,
                    'trailingStop': str(new_trail_dist),
                }
                if new_activation > 0:
                    trailing_params['activePrice'] = str(new_activation)

                self.bybit.private_post_v5_position_trading_stop(trailing_params)
                logger.info(f'new Trailing Stop ({new_trail_dist}) config')
        else:
            logger.info(f'order completely closed')

    # ==========================================
    # 平空單 / 減空單邏輯 (Logic for Close Short)
    # ==========================================
    def close_short(self, symbol: str,
                    total_bet: float,
                    tp_pct: float = None,
                    sl_pct: float = None,
                    trail_sl_pct: float = None,
                    activation_pct: float = None
                    ):

        self.get_exchange_trade(symbol)
        product_symbol = f'{symbol}USDT'
        position_idx = 2  # 2 代表 Hedge Mode 下的空單

        # 1. 抓取目前倉位狀態
        market = self.get_exchange_trade(symbol)
        pos_res = self.bybit.private_get_v5_position_list({'category': 'linear', 'symbol': product_symbol})
        short_pos = next((p for p in pos_res['result']['list'] if int(p['positionIdx']) == position_idx), None)

        if not short_pos or float(short_pos['size']) == 0:
            logger.info(f'No short position found for {symbol}, no need to reduce/close position.')
            return

        current_size = float(short_pos['size'])
        leverage = float(short_pos['leverage'])

        # 🛑 隱患一修正：防呆處理 avgPrice 空值
        raw_avg_price = short_pos.get('avgPrice', 0)
        avg_price = float(raw_avg_price) if raw_avg_price else 0.0

        close_amount = total_bet

        if close_amount <= 0 or close_amount > current_size:
            logger.info(f'Invalid close amount ({close_amount}). Current pos: {current_size}')
            return

        remaining_size = round(current_size - close_amount, 4)

        logger.info(f'Target pos: {total_bet} | Current pos: {current_size}')
        logger.info(f'Preparing to close {close_amount} {symbol} '
                    f'short pos (Expected remaining after close: {remaining_size})')

        # 2. 發送市價單平倉 (ReduceOnly 確保只減倉)
        try:
            order = self.bybit.create_order(
                symbol=product_symbol,
                type='market',
                side='buy',  # 空單平倉要 Buy
                amount=close_amount,
                price=None,
                params={
                    'category': 'linear',
                    'positionIdx': position_idx,
                    'reduceOnly': True,
                    'timeInForce': 'IOC'
                }
            )
            logger.info(f"Short pos closed successfully, OrderID: {order.get('id', 'N/A')}")
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Failed to close pos: {e}")
            return

        # 3. 砍掉重練：清理舊的所有防護網
        self._clear_pos_protection(symbol, position_idx)

        # 4. 如果手上還有剩餘倉位，重新掛上防護網 (Reset)
        if remaining_size > 0:
            if avg_price == 0:
                logger.warning(f"Warning: avgPrice is 0 for {symbol}. Cannot recalculate TP/SL.")
                return

            logger.info(f'Re-establishing protections for the remaining {remaining_size} short contracts...')

            # 取得動態小數點精度
            tick_decimals = self._get_price_precision(avg_price)

            # (A) 獨立 TP & Hard SL (雙標計算，除以槓桿)
            tp_sl_params = {
                'category': 'linear',
                'symbol': product_symbol,
                'positionIdx': position_idx,
                'tpslMode': 'Partial',
                'tpSize': str(remaining_size),
                'slSize': str(remaining_size),
            }

            has_tp_sl = False
            if tp_pct is not None:
                tp_price = round(avg_price * (1 - tp_pct / 100), tick_decimals)
                tp_sl_params['takeProfit'] = str(tp_price)
                has_tp_sl = True

            if sl_pct is not None:
                real_sl_price_pct = sl_pct / leverage
                sl_price = round(avg_price * (1 + real_sl_price_pct / 100), tick_decimals)
                tp_sl_params['stopLoss'] = str(sl_price)
                has_tp_sl = True

            if has_tp_sl:
                self.bybit.private_post_v5_position_trading_stop(tp_sl_params)
                logger.info(f'new Partial TP ({tp_price}) / SL ({sl_price}) config')

            # (B) 重新設定移動止損 (距離除以槓桿)
            if trail_sl_pct is not None:
                real_trail_pct = trail_sl_pct / leverage
                new_trail_dist = round(avg_price * (real_trail_pct / 100), tick_decimals)
                new_activation = round(avg_price * (1 - activation_pct / 100), tick_decimals) if activation_pct else 0

                trailing_params = {
                    'category': 'linear',
                    'symbol': product_symbol,
                    'positionIdx': position_idx,
                    'trailingStop': str(new_trail_dist),
                }
                if new_activation > 0:
                    trailing_params['activePrice'] = str(new_activation)

                self.bybit.private_post_v5_position_trading_stop(trailing_params)
                logger.info(f'new Trailing Stop ({new_trail_dist}) config')
        else:
            logger.info(f'order completely closed')

    # ==========================================
    # 開多單 / 開多單邏輯 (Logic for Open Long)
    # ==========================================
    def trade_long(self, symbol: str,
                   total_bet: float,
                   tp_pct: float = None,
                   sl_pct: float = None,
                   trail_sl_pct: float = None,
                   activation_pct: float = None,
                   leverage: int = 1):

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        self.get_exchange_trade(symbol)
        lever = float(leverage)
        market_symbol = f'{symbol}/USDT:USDT'
        product_symbol = f'{symbol}USDT'

        # ⚠️ 設定為 Hedge Mode (1=多單)
        params = {
            'category': 'linear',
            'positionIdx': 1,  # 1=long, 2=short, 0=one-way
            'timeInForce': 'IOC',
        }

        pos_res = self.bybit.private_get_v5_position_list({'category': 'linear', 'symbol': product_symbol})
        long_pos = next((p for p in pos_res.get('result', {}).get('list', []) if int(p.get('positionIdx', 0)) == 1),
                        None)

        current_leverage = float(long_pos.get('leverage', 0)) if long_pos else 0
        if current_leverage != lever:
            try:
                self.bybit.set_leverage(leverage, market_symbol)
                logger.info(f'Leverage successfully set to {leverage}x')
            except ccxt.BadRequest as e:
                if 'not modified' in str(e).lower() or '110043' in str(e):
                    pass
                else:
                    logger.warning(f'Failed to set leverage: {e}')
            except Exception as e:
                logger.warning(f'Unexpected error when setting leverage: {e}')

        try:
            order = self.bybit.create_order(
                symbol=product_symbol,
                type='market',
                side='buy',
                amount=total_bet,
                price=None,
                params=params
            )
            time.sleep(1)
            logger.info(f"Long opened, orderId: {order.get('id', order)}")
        except ccxt.BaseError as e:
            logger.error(f'Create order failed: {e}')
            if hasattr(e, 'body'): logger.error(f'Exchange body: {e.body}')
            if hasattr(e, 'headers'): logger.error(f'Exchange headers: {e.headers}')
            return False

        order_id = order.get('id', order)

        # Fetch trade details
        trade_param_list = self.bybit.fetch_order_trades(order_id, product_symbol)
        # print(trade_param_list)

        if trade_param_list:
            filled_amount = sum(float(trade['amount']) for trade in trade_param_list)
            if filled_amount == 0:
                logger.warning('Filled amount is 0, cannot calculate entry price.')
                return False

            total_cost = sum(float(trade['price']) * float(trade['amount']) for trade in trade_param_list)
            entry_price = round(total_cost / filled_amount, 4)

            logger.info(f'Average Entry price: {entry_price}')
            logger.info(f'Filled amount: {filled_amount}')

            print(f"\n{'=' * 60}")
            print(f"🎯 TP/SL Parameters Check (HEDGE MODE | LEVERAGE: {leverage}x)")
            print(f"{'=' * 60}")
            print(f"tp_pct: {tp_pct} (Not divided by leverage)")
            print(f"sl_pct: {sl_pct} (Divided by leverage)")
            print(f"trail_sl_pct: {trail_sl_pct} (Divided by leverage)")
            print(f"activation_pct: {activation_pct if 'activation_pct' in locals() else 'Not defined'}")
            print(f"Entry Price: {entry_price}")
            print(f"Product Symbol: {product_symbol}")
            print(f"Filled Amount (This order): {filled_amount}")
            print(f"{'=' * 60}\n")

            # 取得動態小數點精度
            tick_decimals = self._get_price_precision(entry_price)

            # ==================== 1. 獨立 TP & Hard SL (Partial Mode) ====================
            tp_sl_params = {
                'category': 'linear',
                'symbol': product_symbol,
                'positionIdx': 1,  # 1=long
                'tpslMode': 'Partial',  # 開啟部分止盈止損
                'tpSize': str(filled_amount),  # 綁定這筆單的數量
                'slSize': str(filled_amount),  # 綁定這筆單的數量
            }

            has_tp_or_sl = False

            if tp_pct is not None:
                # 【獲利最大化】：TP 不除以槓桿。
                tp_price = round(entry_price * (1 + tp_pct / 100), tick_decimals)
                tp_sl_params['takeProfit'] = str(tp_price)
                tp_sl_params['tpTriggerBy'] = 'LastPrice'
                logger.info(f'Setting Partial TP at {tp_price} (BTC +{tp_pct}%, Est. ROE +{tp_pct * lever}%)')
                has_tp_or_sl = True

            if sl_pct is not None:
                # 【風險最小化】：SL 除以槓桿。
                real_sl_price_pct = sl_pct / lever
                sl_price = round(entry_price * (1 - real_sl_price_pct / 100), tick_decimals)
                tp_sl_params['stopLoss'] = str(sl_price)
                tp_sl_params['slTriggerBy'] = 'LastPrice'
                logger.info(f'Preparing Partial Hard SL at {sl_price} (BTC -{real_sl_price_pct}%, '
                            f'Max ROE Loss Capped at -{sl_pct}%)')
                has_tp_or_sl = True

            if has_tp_or_sl:
                try:
                    result = self.bybit.private_post_v5_position_trading_stop(tp_sl_params)
                    if str(result.get('retCode')) == '0':
                        logger.info('Partial TP/SL successfully deployed on exchange.')
                    else:
                        logger.warning(f'Unexpected TP/SL Response: {result}')
                except Exception as e:
                    logger.error(f'TP/SL Error: {type(e).__name__}: {e}')

            # ==================== 2. 加權平均 Trailing Stop ====================
            if trail_sl_pct is not None:
                try:
                    pos_res = self.bybit.private_get_v5_position_list({'category': 'linear', 'symbol': product_symbol})
                    long_pos = next((p for p in pos_res['result']['list'] if int(p['positionIdx']) == 1), None)

                    if long_pos:
                        total_qty = float(long_pos['size'])
                        new_qty = filled_amount
                        old_qty = total_qty - new_qty

                        # 【風險最小化】：Trailing Stop 距離除以槓桿。
                        real_trail_pct = trail_sl_pct / lever
                        new_trail_dist = entry_price * (real_trail_pct / 100)

                        # 激活價 (Activation) 通常看標的物真實漲幅，不除以槓桿。
                        new_activation = entry_price * (1 + activation_pct / 100) if activation_pct is not None else 0

                        if old_qty > 0 and long_pos.get('trailingStop') and float(long_pos.get('trailingStop')) > 0:
                            # 🛑 隱患二修正：確保 trailingStop 距離為絕對值正數
                            old_trail_dist = abs(float(long_pos['trailingStop']))
                            old_activation = float(long_pos.get('activePrice', 0))

                            final_trail_dist = round(
                                ((old_qty * old_trail_dist) + (new_qty * new_trail_dist)) / total_qty, tick_decimals)

                            if old_activation > 0 and new_activation > 0:
                                final_activation = round(
                                    ((old_qty * old_activation) + (new_qty * new_activation)) / total_qty,
                                    tick_decimals)
                            else:
                                final_activation = round(new_activation, tick_decimals)
                            logger.info(f'Averaging in triggered! Merged position size: {total_qty}')
                        else:
                            final_trail_dist = round(new_trail_dist, tick_decimals)
                            final_activation = round(new_activation, tick_decimals)

                        trailing_params = {
                            'category': 'linear',
                            'symbol': product_symbol,
                            'positionIdx': 1,  # 1=long
                            'trailingStop': str(final_trail_dist),
                        }

                        act_msg = ''
                        if final_activation > 0:
                            trailing_params['activePrice'] = str(final_activation)
                            act_msg = f' | Activation: {final_activation}'

                        result = self.bybit.private_post_v5_position_trading_stop(trailing_params)

                        if str(result.get('retCode')) == '0':
                            logger.info(f'Trailing Stop deployed (Dist: {final_trail_dist}{act_msg})')
                        else:
                            logger.warning(f'Unexpected Trailing Stop Response: {result}')

                except Exception as e:
                    logger.error(f'Failed to calculate/set Trailing Stop: {type(e).__name__}: {e}')
            else:
                logger.info('trail_sl_pct is None, skipping Trailing Stop setup')

            # 驗證設置
            time.sleep(0.5)

        # Record trades to CSV
        try:
            record_list = self.record_to_df(trade_param_list)
            record_df = pd.DataFrame(record_list)
            record_df = record_df.drop(columns=record_df.columns[0])
            record_df['rec_time'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            record_df.set_index('rec_time', inplace=True)
            record_df = record_df.reindex(columns=self.col_order)

            write_header = (not os.path.exists(self.file_path_trade)) or (os.path.getsize(self.file_path_trade) == 0)
            record_df.to_csv(
                self.file_path_trade,
                mode='a',
                index=False,
                header=write_header
            )
        except Exception as e:
            logger.error(f'Failed to write CSV record: {e}')

        print('---------------------------------------------------------', symbol)

        return record_df

    # ==========================================
    # 開空單 / 加空單邏輯 (Logic for Open Short)
    # ==========================================
    def trade_short(self, symbol: str,
                    total_bet: float,
                    tp_pct: float = None,
                    sl_pct: float = None,
                    trail_sl_pct: float = None,
                    activation_pct: float = None,
                    leverage: int = 1):

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        self.get_exchange_trade(symbol)
        lever = float(leverage)
        market_symbol = f'{symbol}/USDT:USDT'
        product_symbol = f'{symbol}USDT'

        # ⚠️ 設定為 Hedge Mode (2=空單)
        params = {
            'category': 'linear',
            'positionIdx': 2,  # 1=long, 2=short, 0=one-way
            'timeInForce': 'IOC',
        }

        pos_res = self.bybit.private_get_v5_position_list({'category': 'linear', 'symbol': product_symbol})
        short_pos = next((p for p in pos_res.get('result', {}).get('list', []) if int(p.get('positionIdx', 0)) == 2),
                         None)

        current_leverage = float(short_pos.get('leverage', 0)) if short_pos else 0
        if current_leverage != lever:
            try:
                self.bybit.set_leverage(leverage, market_symbol)
                logger.info(f'Leverage successfully set to {leverage}x')
            except ccxt.BadRequest as e:
                if 'not modified' in str(e).lower() or '110043' in str(e):
                    pass
                else:
                    logger.warning(f'Failed to set leverage: {e}')
            except Exception as e:
                logger.warning(f'Unexpected error when setting leverage: {e}')

        try:
            order = self.bybit.create_order(
                symbol=product_symbol,
                type='market',
                side='sell',  # ⚠️ 開空要用 Sell
                amount=total_bet,
                price=None,
                params=params
            )
            time.sleep(1)
            logger.info(f"Short opened, orderId: {order.get('id', order)}")
        except ccxt.BaseError as e:
            logger.error(f'Create order failed: {e}')
            if hasattr(e, 'body'): logger.error(f'Exchange body: {e.body}')
            if hasattr(e, 'headers'): logger.error(f'Exchange headers: {e.headers}')
            return False

        order_id = order.get('id', order)

        trade_param_list = self.bybit.fetch_order_trades(order_id, product_symbol)

        if trade_param_list:
            filled_amount = sum(float(trade['amount']) for trade in trade_param_list)
            if filled_amount == 0:
                logger.warning('Filled amount is 0, cannot calculate entry price.')
                return False

            total_cost = sum(float(trade['price']) * float(trade['amount']) for trade in trade_param_list)
            entry_price = round(total_cost / filled_amount, 4)

            logger.info(f'Average Entry price: {entry_price}')
            logger.info(f'Filled amount: {filled_amount}')

            print(f"\n{'=' * 60}")
            print(f"🎯 TP/SL Parameters Check (HEDGE MODE | SHORT | LEVERAGE: {leverage}x)")
            print(f"{'=' * 60}")
            print(f"tp_pct: {tp_pct} (Not divided by leverage)")
            print(f"sl_pct: {sl_pct} (Divided by leverage)")
            print(f"trail_sl_pct: {trail_sl_pct} (Divided by leverage)")
            print(f"activation_pct: {activation_pct if 'activation_pct' in locals() else 'Not defined'}")
            print(f"Entry Price: {entry_price}")
            print(f"Product Symbol: {product_symbol}")
            print(f"Filled Amount (This order): {filled_amount}")
            print(f"{'=' * 60}\n")

            # 取得動態小數點精度
            tick_decimals = self._get_price_precision(entry_price)

            # ==================== 1. 獨立 TP & Hard SL (Partial Mode) ====================
            tp_sl_params = {
                'category': 'linear',
                'symbol': product_symbol,
                'positionIdx': 2,  # ⚠️ 2=short
                'tpslMode': 'Partial',
                'tpSize': str(filled_amount),
                'slSize': str(filled_amount),
            }

            has_tp_or_sl = False

            if tp_pct is not None:
                # ⚠️ 空單獲利最大化：價格下跌 (減號)。TP 不除以槓桿。
                tp_price = round(entry_price * (1 - tp_pct / 100), tick_decimals)
                tp_sl_params['takeProfit'] = str(tp_price)
                tp_sl_params['tpTriggerBy'] = 'LastPrice'
                logger.info(f'Setting Partial TP at {tp_price} (BTC -{tp_pct}%, Est. ROE +{tp_pct * lever}%)')
                has_tp_or_sl = True

            if sl_pct is not None:
                # ⚠️ 空單風險最小化：價格上漲 (加號)。SL 除以槓桿。
                real_sl_price_pct = sl_pct / lever
                sl_price = round(entry_price * (1 + real_sl_price_pct / 100), tick_decimals)
                tp_sl_params['stopLoss'] = str(sl_price)
                tp_sl_params['slTriggerBy'] = 'LastPrice'
                logger.info(f'Preparing Partial Hard SL at {sl_price} (BTC +{real_sl_price_pct}%, '
                            f'Max ROE Loss Capped at -{sl_pct}%)')
                has_tp_or_sl = True

            if has_tp_or_sl:
                try:
                    result = self.bybit.private_post_v5_position_trading_stop(tp_sl_params)
                    if str(result.get('retCode')) == '0':
                        logger.info('Partial TP/SL successfully deployed on exchange.')
                    else:
                        logger.warning(f'Unexpected TP/SL Response: {result}')
                except Exception as e:
                    logger.error(f'TP/SL Error: {type(e).__name__}: {e}')

            # ==================== 2. 加權平均 Trailing Stop ====================
            if trail_sl_pct is not None:
                try:
                    pos_res = self.bybit.private_get_v5_position_list({'category': 'linear', 'symbol': product_symbol})
                    short_pos = next((p for p in pos_res['result']['list'] if int(p['positionIdx']) == 2), None)

                    if short_pos:
                        total_qty = float(short_pos['size'])
                        new_qty = filled_amount
                        old_qty = total_qty - new_qty

                        # 距離計算方式不變 (依然是絕對值，不分多空)
                        real_trail_pct = trail_sl_pct / lever
                        new_trail_dist = entry_price * (real_trail_pct / 100)

                        # ⚠️ 空單激活價：跌破某個價格才啟動 (減號)。不除以槓桿。
                        new_activation = entry_price * (1 - activation_pct / 100) if activation_pct is not None else 0

                        if old_qty > 0 and short_pos.get('trailingStop') and float(short_pos.get('trailingStop')) > 0:
                            # 🛑 隱患二修正：確保 trailingStop 距離為絕對值正數
                            old_trail_dist = abs(float(short_pos['trailingStop']))
                            old_activation = float(short_pos.get('activePrice', 0))

                            final_trail_dist = round(
                                ((old_qty * old_trail_dist) + (new_qty * new_trail_dist)) / total_qty, tick_decimals)

                            if old_activation > 0 and new_activation > 0:
                                final_activation = round(
                                    ((old_qty * old_activation) + (new_qty * new_activation)) / total_qty,
                                    tick_decimals)
                            else:
                                final_activation = round(new_activation, tick_decimals)
                            logger.info(f'Averaging in triggered! Merged position size: {total_qty}')
                        else:
                            final_trail_dist = round(new_trail_dist, tick_decimals)
                            final_activation = round(new_activation, tick_decimals)

                        trailing_params = {
                            'category': 'linear',
                            'symbol': product_symbol,
                            'positionIdx': 2,  # ⚠️ 2=short
                            'trailingStop': str(final_trail_dist),
                        }

                        act_msg = ''
                        if final_activation > 0:
                            trailing_params['activePrice'] = str(final_activation)
                            act_msg = f' | Activation: {final_activation}'

                        result = self.bybit.private_post_v5_position_trading_stop(trailing_params)

                        if str(result.get('retCode')) == '0':
                            logger.info(f'Trailing Stop deployed (Dist: {final_trail_dist}{act_msg})')
                        else:
                            logger.warning(f'Unexpected Trailing Stop Response: {result}')

                except Exception as e:
                    logger.error(f'Failed to calculate/set Trailing Stop: {type(e).__name__}: {e}')
            else:
                logger.info('trail_sl_pct is None, skipping Trailing Stop setup')

            # 驗證設置
            time.sleep(0.5)

        # Record trades to CSV
        try:
            record_list = self.record_to_df(trade_param_list)
            record_df = pd.DataFrame(record_list)
            record_df = record_df.drop(columns=record_df.columns[0])
            record_df['rec_time'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            record_df.set_index('rec_time', inplace=True)
            record_df = record_df.reindex(columns=self.col_order)

            write_header = (not os.path.exists(self.file_path_trade)) or (os.path.getsize(self.file_path_trade) == 0)
            record_df.to_csv(
                self.file_path_trade,
                mode='a',
                index=False,
                header=write_header
            )
        except Exception as e:
            logger.error(f'Failed to write CSV record: {e}')

        print('---------------------------------------------------------', symbol)

        return record_df