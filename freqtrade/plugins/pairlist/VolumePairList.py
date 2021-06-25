"""
Volume PairList provider

Provides dynamic pair list based on trade volumes
"""
import logging
from typing import Any, Dict, List

from cachetools.ttl import TTLCache

from freqtrade.configuration import TimeRange
from freqtrade.data import history
from freqtrade.exceptions import OperationalException
from freqtrade.plugins.pairlist.IPairList import IPairList


logger = logging.getLogger(__name__)


SORT_VALUES = ['quoteVolume']


class VolumePairList(IPairList):

    def __init__(self, exchange, pairlistmanager,
                 config: Dict[str, Any], pairlistconfig: Dict[str, Any],
                 pairlist_pos: int) -> None:
        super().__init__(exchange, pairlistmanager, config, pairlistconfig, pairlist_pos)

        if 'number_assets' not in self._pairlistconfig:
            raise OperationalException(
                '`number_assets` not specified. Please check your configuration '
                'for "pairlist.config.number_assets"')

        self._stake_currency = config['stake_currency']
        self._number_pairs = self._pairlistconfig['number_assets']
        self._sort_key = self._pairlistconfig.get('sort_key', 'quoteVolume')
        self._min_value = self._pairlistconfig.get('min_value', 0)
        self._refresh_period = self._pairlistconfig.get('refresh_period', 1800)
        self._pair_cache: TTLCache = TTLCache(maxsize=1, ttl=self._refresh_period)

        if not self._exchange.exchange_has('fetchTickers'):
            raise OperationalException(
                'Exchange does not support dynamic whitelist. '
                'Please edit your config and restart the bot.'
            )

        if not self._validate_keys(self._sort_key):
            raise OperationalException(
                f'key {self._sort_key} not in {SORT_VALUES}')

    @property
    def needstickers(self) -> bool:
        """
        Boolean property defining if tickers are necessary.
        If no Pairlist requires tickers, an empty Dict is passed
        as tickers argument to filter_pairlist
        """
        return True

    def _validate_keys(self, key):
        return key in SORT_VALUES

    def short_desc(self) -> str:
        """
        Short whitelist method description - used for startup-messages
        """
        return f"{self.name} - top {self._pairlistconfig['number_assets']} volume pairs."

    def gen_pairlist(self, tickers: Dict) -> List[str]:
        """
        Generate the pairlist
        :param tickers: Tickers (from exchange.get_tickers()). May be cached.
        :return: List of pairs
        """
        # Generate dynamic whitelist
        # Must always run if this pairlist is not the first in the list.
        pairlist = self._pair_cache.get('pairlist')
        if pairlist:
            # Item found - no refresh necessary
            return pairlist
        else:

            # Use fresh pairlist
            # Check if pair quote currency equals to the stake currency.
            filtered_tickers = [
                    v for k, v in tickers.items()
                    if (self._exchange.get_pair_quote_currency(k) == self._stake_currency
                        and v[self._sort_key] is not None)]
            pairlist = [s['symbol'] for s in filtered_tickers]

            pairlist = self.filter_pairlist(pairlist, tickers)
            self._pair_cache['pairlist'] = pairlist

        return pairlist

    def filter_pairlist(self, pairlist: List[str], tickers: Dict) -> List[str]:
        """
        Filters and sorts pairlist and returns the whitelist again.
        Called on each bot iteration - please use internal caching if necessary
        :param pairlist: pairlist to filter or sort
        :param tickers: Tickers (from exchange.get_tickers()). May be cached.
        :return: new whitelist
        """
        # Use the incoming pairlist.
        filtered_tickers = [v for k, v in tickers.items() if k in pairlist]

        if self._min_value > 0:
            filtered_tickers = [
                    v for v in filtered_tickers if v[self._sort_key] > self._min_value]

        # Pairlist generation for a past date.
        past_date_str = self._config.get('timerange', None)
        if past_date_str is not None:
            if '-' not in past_date_str:
                past_date_str += f'-{past_date_str}'
            timerange = TimeRange.parse_timerange(past_date_str)
            timerange.stopts += 24*60*60*1000
            processed_pairs = []
            data = history.load_data(
                datadir=self._config['datadir'],
                pairs=pairlist,
                timeframe='1d',
                timerange=timerange,
                startup_candles=0,
                fail_without_data=False,
                data_format=self._config.get('dataformat_ohlcv', 'json'),
            )
            for pair, df in data.items():
                volume = 0
                processed_pairs.append(pair)
                if not df.empty and int(df.iloc[0]['date'].timestamp()) == timerange.startts:
                    volume = df.iloc[0]['volume']
                tickers[pair]['quoteVolume'] = volume

            since_ms = timerange.startts * 1000
            for pair in pairlist:
                volume = 0
                if pair in processed_pairs:
                    continue
                candles = self._exchange.get_historic_ohlcv(pair, '1d', since_ms)
                if candles:
                    if candles[0][0] == since_ms:
                        volume = candles[0][-1]
                tickers[pair]['quoteVolume'] = volume

        sorted_tickers = sorted(filtered_tickers, reverse=True, key=lambda t: t[self._sort_key])

        # Validate whitelist to only have active market pairs
        pairs = self._whitelist_for_active_markets([s['symbol'] for s in sorted_tickers])
        pairs = self.verify_blacklist(pairs, logger.info)
        # Limit pairlist to the requested number of pairs
        pairs = pairs[:self._number_pairs]

        self.log_once(f"Searching {self._number_pairs} pairs: {pairs}", logger.info)

        return pairs
