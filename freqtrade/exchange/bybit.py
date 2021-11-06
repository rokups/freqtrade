""" Bybit exchange subclass """
import logging
from typing import Dict

from freqtrade.exchange import Exchange


logger = logging.getLogger(__name__)


class Bybit(Exchange):
    """
    Bybit exchange class. Contains adjustments needed for Freqtrade to work
    with this exchange.

    Please note that this exchange is not included in the list of exchanges
    officially supported by the Freqtrade development team. So some features
    may still not work as expected.
    """

    _ft_has: Dict = {
        "ohlcv_candle_limit": 200,
    }

    def market_is_tradable(self, market) -> bool:
        """
        Check if the market symbol is tradable by Freqtrade.
        Default checks + check if pair is spot pair (no futures trading yet).
        """
        # BEGIN Futures/leverage
        return market.get('future', False) or market.get('spot', False)
        # END Futures/leverage
