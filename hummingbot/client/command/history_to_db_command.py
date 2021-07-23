from decimal import Decimal
import threading
import time
from typing import (
    Set,
    Tuple,
    TYPE_CHECKING,
    List,
    Optional
)
from datetime import datetime
from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.client.settings import (
    MAXIMUM_TRADE_FILLS_DISPLAY_OUTPUT,
    CONNECTOR_SETTINGS,
    ConnectorType,
    DERIVATIVES
)
from hummingbot.model.trade_fill import TradeFill
from hummingbot.user.user_balances import UserBalances
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.client.performance import PerformanceMetrics

from sqlalchemy.orm import (
    Session,
    Query
)
from hummingbot.model.history_to_db import HistoryDb
import json

s_float_0 = float(0)
s_decimal_0 = Decimal("0")


if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication


def get_timestamp(days_ago: float = 0.) -> float:
    return time.time() - (60. * 60. * 24. * days_ago)


class HistoryToDbCommand:
    def history_to_db(self,  # type: HummingbotApplication
                days: float = 0,
                verbose: bool = False,
                precision: Optional[int] = None
                ):
        if threading.current_thread() != threading.main_thread():
            self.ev_loop.call_soon_threadsafe(self.history_to_db)
            return

        if self.strategy_file_name is None:
            self._notify("\n  Please first import a strategy config file of which to show historical performance.")
            return
        # if global_config_map.get("paper_trade_enabled").value:
        #     self._notify("\n  Paper Trading ON: All orders are simulated, and no real orders are placed.")
        start_time = get_timestamp(days) if days > 0 else self.init_time
        trades: List[TradeFill] = self._get_trades_from_session(int(start_time * 1e3),
                                                                config_file_path=self.strategy_file_name)
        if not trades:
            # self._notify("\n  No past trades to report.")
            return
        # if verbose:
        #     self.list_trades(start_time)
        if self.strategy_name != "celo_arb":
            safe_ensure_future(self.history_report_to_db(start_time, trades, precision))

    async def history_report_to_db(self,  # type: HummingbotApplication
                             start_time: float,
                             trades: List[TradeFill],
                             precision: Optional[int] = None,
                             display_report: bool = True) -> Decimal:
        market_info: Set[Tuple[str, str]] = set((t.market, t.symbol) for t in trades)
        time_json = self.report_header_json(start_time)
        return_pcts = []
        report_performance_json = []
        for market, symbol in market_info:
            cur_trades = [t for t in trades if t.market == market and t.symbol == symbol]
            cur_balances = await self.get_current_balances(market)
            perf = await PerformanceMetrics.create(market, symbol, cur_trades, cur_balances)
            report_performance = self.report_performance_by_market_json(market, symbol, perf, precision)
            report_performance_json.append(report_performance)
            return_pcts.append(perf.return_pct)
        avg_return = sum(return_pcts) / len(return_pcts) if len(return_pcts) > 0 else s_decimal_0
        data = {
            "Time": time_json,
            "Report": report_performance_json,
            "Averaged Return, %": float(f"{(100*avg_return):.2f}"),
        }
        history = json.dumps(data)
        config_file_path: str = self.strategy_file_name
        # strategy_file: str = self.strategy_file_name
        strategy_name: str = self.strategy_name
        # status = history
        session: Session = self.trade_fill_db.get_shared_session()
        timestamp = data['Time']['Current Time']
        query: Query = (session
                        .query(HistoryDb)
                        .filter(HistoryDb.config_file_path == config_file_path))
        history_to_db: Optional[HistoryDb] = query.one_or_none()
        if history_to_db is not None:
            history_to_db.config_file_path = config_file_path
            history_to_db.strategy = strategy_name
            history_to_db.history = history
            history_to_db.timestamp = timestamp
        else:
            history_to_db = HistoryDb(config_file_path=config_file_path,
                                      strategy=strategy_name,
                                      history=history,
                                      timestamp=timestamp)
            session.add(history_to_db)
        session.commit()
        return avg_return

    async def get_current_balances(self,  # type: HummingbotApplication
                                   market: str):
        if market in self.markets and self.markets[market].ready:
            return self.markets[market].get_all_balances()
        elif "Paper" in market:
            paper_balances = global_config_map["paper_trade_account_balance"].value
            if paper_balances is None:
                return {}
            return {token: Decimal(str(bal)) for token, bal in paper_balances.items()}
        elif "perpetual_finance" == market:
            return await UserBalances.xdai_balances()
        else:
            gateway_eth_connectors = [cs.name for cs in CONNECTOR_SETTINGS.values() if cs.use_ethereum_wallet and
                                      cs.type == ConnectorType.Connector]
            if market in gateway_eth_connectors:
                return await UserBalances.instance().eth_n_erc20_balances()
            else:
                await UserBalances.instance().update_exchange_balance(market)
                return UserBalances.instance().all_balances(market)


    def report_header_json(self,  # type: HummingbotApplication
                      start_time: float):
        current_time = get_timestamp()
        data = {
            "Start Time": int(start_time),
            "Current Time": int(current_time),
            "Duration, s": int(current_time - start_time),
        }
        return data

    def report_performance_by_market_json(self,  # type: HummingbotApplication
                                     market: str,
                                     trading_pair: str,
                                     perf: PerformanceMetrics,
                                     precision: int):
        base, quote = trading_pair.split("-")
        trades_json = {
            "Number of trades": {
                "buy": perf.num_buys,
                "sell": perf.num_sells,
                "total": perf.num_trades,
            },
            f"Total trade volume ({base})": {
                "buy": float(PerformanceMetrics.smart_round(perf.b_vol_base, precision)),
                "sell": float(PerformanceMetrics.smart_round(perf.s_vol_base, precision)),
                "total": float(PerformanceMetrics.smart_round(perf.tot_vol_base, precision)),
            },
            f"Total trade volume ({quote})": {
                "buy": float(PerformanceMetrics.smart_round(perf.b_vol_quote, precision)),
                "sell": float(PerformanceMetrics.smart_round(perf.s_vol_quote, precision)),
                "total": float(PerformanceMetrics.smart_round(perf.tot_vol_quote, precision)),
            },
            "Avg price": {
                "buy": float(PerformanceMetrics.smart_round(perf.avg_b_price, precision)),
                "sell": float(PerformanceMetrics.smart_round(perf.avg_s_price, precision)),
                "total": float(PerformanceMetrics.smart_round(perf.avg_tot_price, precision)),
            },
        }
        assets_json = {
            f"{base}": {"-", "-", "-"} if market in DERIVATIVES else {   # No base asset for derivatives because they are margined
             "start": float(PerformanceMetrics.smart_round(perf.start_base_bal, precision)),
             "current": float(PerformanceMetrics.smart_round(perf.cur_base_bal, precision)),
             "change": float(PerformanceMetrics.smart_round(perf.tot_vol_base, precision))
             },
            f"{quote}": {
             "start": float(PerformanceMetrics.smart_round(perf.start_quote_bal, precision)),
             "current": float(PerformanceMetrics.smart_round(perf.cur_quote_bal, precision)),
             "change": float(PerformanceMetrics.smart_round(perf.tot_vol_quote, precision))
             },
            f"{trading_pair + ' price'}": {
             "start": float(PerformanceMetrics.smart_round(perf.start_price)),
             "current": float(PerformanceMetrics.smart_round(perf.cur_price)),
             "change": float(PerformanceMetrics.smart_round(perf.cur_price - perf.start_price))
             },
            "Base asset, %": {"-", "-", "-"} if market in DERIVATIVES else {   # No base asset for derivatives because they are margined
                "start": float(f"{(100*perf.start_base_ratio_pct):.2f}"),
                "current": float(f"{(100*perf.cur_base_ratio_pct):.2f}"),
                "change": float(f"{(100*(perf.cur_base_ratio_pct - perf.start_base_ratio_pct)):.2f}")
            },
        }
        performance_json = {
            f"Hold portfolio value, {quote}": float(PerformanceMetrics.smart_round(perf.hold_value, precision)),
            f"Current portfolio value, {quote}": float(PerformanceMetrics.smart_round(perf.cur_value, precision)),
            f"Trade P&L, {quote}": float(PerformanceMetrics.smart_round(perf.trade_pnl, precision)),
        }
        for fee_token, fee_amount in perf.fees.items():
            performance_json[f"Fees paid, {fee_token}"] = float(PerformanceMetrics.smart_round(fee_amount, precision))
        performance_json[f"Total P&L, {quote}"] = float(PerformanceMetrics.smart_round(perf.total_pnl, precision))
        performance_json["Return %"] = float(f"{(100*perf.return_pct):.2f}")
        data = {
            "Market": market,
            "Trading pair": trading_pair,
            "Trades": trades_json,
            "Assets": assets_json,
            "Performance": performance_json,
        }
        return data