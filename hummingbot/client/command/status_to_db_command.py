import inspect
import pandas as pd
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.core.utils.ethereum import check_web3
from hummingbot.client.config.security import Security
from hummingbot.client.settings import required_exchanges, ethereum_wallet_required
from hummingbot.core.utils.async_utils import safe_ensure_future
from sqlalchemy.orm import (
    Session,
    Query
)
from hummingbot.model.data_to_db import DataToDb
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
import json

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication


class StatusToDbCommand:
    def status_to_db(self,  # type: HummingbotApplication
               live: bool = False):
        safe_ensure_future(self.status_to_db_check_all(live=live), loop=self.ev_loop)

    async def status_to_db_check_all(self,  # type: HummingbotApplication
                               notify_success=True,
                               live=False) -> bool:
        if self.strategy is not None:
            if live:
                await self.stop_live_update()
                self.app.live_updates = True
                while self.app.live_updates and self.strategy:
                    script_status = '\n Status from script would not appear here. ' \
                                    'Simply run the status command without "--live" to see script status.'
                    await self.cls_display_delay(
                        await self.strategy_status(live=True) + script_status + "\n\n Press escape key to stop update.", 1
                    )
                self._notifiy_status_to_db("Stopped live status display update.")
            else:
                self._notifiy_status_to_db(await self.strategy_status())
            return True

        # Preliminary checks.
        self._notifiy_status_to_db("\nPreliminary checks:")
        if self.strategy_name is None or self.strategy_file_name is None:
            self._notifiy_status_to_db('  - Strategy check: Please import or create a strategy.')
            return False

        if not Security.is_decryption_done():
            self._notifiy_status_to_db('  - Security check: Encrypted files are being processed. Please wait and try again later.')
            return False

        invalid_conns = await self.validate_required_connections()
        if invalid_conns:
            self._notifiy_status_to_db('  - Exchange check: Invalid connections:')
            for ex, err_msg in invalid_conns.items():
                self._notifiy_status_to_db(f"    {ex}: {err_msg}")
        elif notify_success:
            self._notifiy_status_to_db('  - Exchange check: All connections confirmed.')

        missing_configs = self.missing_configurations()
        if missing_configs:
            self._notifiy_status_to_db("  - Strategy check: Incomplete strategy configuration. The following values are missing.")
            for config in missing_configs:
                self._notifiy_status_to_db(f"    {config.key}")
        elif notify_success:
            self._notifiy_status_to_db('  - Strategy check: All required parameters confirmed.')
        if invalid_conns or missing_configs:
            return False

        if self.wallet is not None:
            # Only check node url when a wallet has been initialized
            eth_node_valid = check_web3(global_config_map.get("ethereum_rpc_url").value)
            if not eth_node_valid:
                self._notifiy_status_to_db('  - Node check: Bad ethereum rpc url. '
                             'Please re-configure by entering "config ethereum_rpc_url"')
                return False
            elif notify_success:
                self._notifiy_status_to_db("  - Node check: Ethereum node running and current.")

            if self.wallet.network_status is NetworkStatus.CONNECTED:
                if self._trading_required:
                    has_minimum_eth = self.wallet.get_balance("ETH") > 0.01
                    if not has_minimum_eth:
                        self._notifiy_status_to_db("  - ETH wallet check: Not enough ETH in wallet. "
                                     "A small amount of Ether is required for sending transactions on "
                                     "Decentralized Exchanges")
                        return False
                    elif notify_success:
                        self._notifiy_status_to_db("  - ETH wallet check: Minimum ETH requirement satisfied")
            else:
                self._notifiy_status_to_db("  - ETH wallet check: ETH wallet is not connected.")

        loading_markets: List[ConnectorBase] = []
        for market in self.markets.values():
            if not market.ready:
                loading_markets.append(market)

        if len(loading_markets) > 0:
            self._notifiy_status_to_db("  - Connectors check:  Waiting for connectors " +
                         ",".join([m.name.capitalize() for m in loading_markets]) + " to get ready for trading. \n"
                         "                    Please keep the bot running and try to start again in a few minutes. \n")

            for market in loading_markets:
                market_status_df = pd.DataFrame(data=market.status_dict.items(), columns=["description", "status"])
                self._notifiy_status_to_db(
                    f"  - {market.display_name.capitalize()} connector status:\n" +
                    "\n".join(["     " + line for line in market_status_df.to_string(index=False,).split("\n")]) +
                    "\n"
                )
            return False

        elif not all([market.network_status is NetworkStatus.CONNECTED for market in self.markets.values()]):
            offline_markets: List[str] = [
                market_name
                for market_name, market
                in self.markets.items()
                if market.network_status is not NetworkStatus.CONNECTED
            ]
            for offline_market in offline_markets:
                self._notifiy_status_to_db(f"  - Connector check: {offline_market} is currently offline.")
            return False

        # Paper trade mode is currently not available for connectors other than exchanges.
        # Todo: This check is hard coded at the moment, when we get a clearer direction on how we should handle this,
        # this section will need updating.
        if global_config_map.get("paper_trade_enabled").value:
            if "balancer" in required_exchanges and \
                    str(global_config_map.get("ethereum_chain_name").value).lower() != "kovan":
                self._notifiy_status_to_db("Error: Paper trade mode is not available on balancer at the moment.")
                return False
            if "binance_perpetual" in required_exchanges:
                self._notifiy_status_to_db("Error: Paper trade mode is not available on binance_perpetual at the moment.")
                return False

        self.application_warning()
        self._notifiy_status_to_db("  - All checks: Confirmed.")
        return True

    async def strategy_status_to_db(self, session: Session, order_id: str, timestamp: int):
        # if inspect.iscoroutinefunction(self.strategy.format_status):
        #     st_status = await self.strategy.format_status()
        # else:
        #     st_status = self.strategy.format_status()
        if inspect.iscoroutinefunction(self.strategy.format_status_json):
            st_status_json = await self.strategy.format_status_json()
        else:
            st_status_json = self.strategy.format_status_json()

        # status = st_status
        status = json.dumps(st_status_json)
        # print(str(st_status))
        # print(str(st_status_json))
        data_to_db: Optional[DataToDb] = session.query(DataToDb).filter(DataToDb.order_id == order_id).one_or_none()
        if data_to_db is not None:
            data_to_db.order_id = order_id
            data_to_db.status = status
            data_to_db.timestamp = timestamp
        else:
            data_to_db = DataToDb(order_id=order_id,
                                  status=status,
                                  timestamp=timestamp)
            session.add(data_to_db)

        session.commit()