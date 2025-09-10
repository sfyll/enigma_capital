import asyncio
import json
import logging
import os
from typing import Dict, List, Optional, Tuple

import aiohttp
from web3 import Web3

from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData

from hyperliquid.info import Info
from hyperliquid.utils import constants


class HyperliquidAPIError(Exception):
    pass


class DataFetcher(ExchangeBase):
    _EXCHANGE = "HYPERLIQUID"

    def __init__(self, secrets: ApiMetaData, session: aiohttp.ClientSession) -> None:
        super().__init__(exchange=self._EXCHANGE, session=session)
        self.logger = logging.getLogger(__name__)

        self._addresses = self.__get_addresses_of_interest()
        if not self._addresses:
            raise ValueError("No valid Hyperliquid addresses found in onchain_meta_data.json.")

        base_url = None
        if getattr(secrets, "other_fields", None):
            base_url = secrets.other_fields.get("BaseUrl") or secrets.other_fields.get("base_url")
        self._info = Info(base_url or constants.MAINNET_API_URL, skip_ws=True)

    def __get_addresses_of_interest(self) -> List[str]:
        current_directory = os.path.dirname(__file__)
        path = os.path.abspath(os.path.join(current_directory, "..", "..", "config", "onchain_meta_data.json"))
        with open(path, "r") as f:
            data = json.load(f)
        raw = data.get("addresses_per_chain", {}).get("HYPERLIQUID", [])
        out: List[str] = []
        for a in raw:
            if isinstance(a, str):
                addr = a.strip()
                if Web3.is_address(addr):
                    out.append(addr)
                else:
                    logging.getLogger(__name__).warning(f"Skipping invalid Hyperliquid address: {a}")
        return out

    async def fetch_balance(self, accountType: Optional[str] = None) -> float:
        loop = asyncio.get_running_loop()
        tasks = [loop.run_in_executor(None, self._info.user_state, addr) for addr in self._addresses]
        try:
            states = await asyncio.gather(*tasks)
        except Exception as e:
            raise HyperliquidAPIError(f"user_state call failed: {e}") from e
        total = 0.0
        for s in states:
            total += self._extract_account_value(s)
        return round(total, 2)

    async def fetch_positions(self, accountType: Optional[str] = None) -> dict:
        loop = asyncio.get_running_loop()
        try:
            mids_raw = await loop.run_in_executor(None, self._info.all_mids)
        except Exception as e:
            raise HyperliquidAPIError(f"all_mids call failed: {e}") from e
        mids = self._mids_dict(mids_raw)

        tasks = [loop.run_in_executor(None, self._info.user_state, addr) for addr in self._addresses]
        try:
            states = await asyncio.gather(*tasks)
        except Exception as e:
            raise HyperliquidAPIError(f"user_state call failed: {e}") from e

        agg: Dict[str, Tuple[float, float]] = {}
        for s in states:
            for sym, qty, notional in self._extract_positions(s, mids):
                q, n = agg.get(sym, (0.0, 0.0))
                agg[sym] = (q + qty, n + notional)

        symbols: List[str] = []
        multipliers: List[int] = []
        quantities: List[float] = []
        dollar_quantities: List[float] = []
        for sym, (qty, notional) in agg.items():
            symbols.append(sym)
            multipliers.append(1)
            quantities.append(round(qty, 6))
            dollar_quantities.append(round(notional, 3))
        return {
            "Symbol": symbols,
            "Multiplier": multipliers,
            "Quantity": quantities,
            "Dollar Quantity": dollar_quantities,
        }

    def _mids_dict(self, mids) -> Dict[str, float]:
        if isinstance(mids, dict):
            return {str(k).upper(): float(v) for k, v in mids.items() if v is not None}
        out: Dict[str, float] = {}
        if isinstance(mids, list):
            for item in mids:
                sym = (item.get("coin") or item.get("symbol") or item.get("asset") or "").upper()
                px = item.get("mid") or item.get("price") or item.get("px")
                if sym and px is not None:
                    out[sym] = float(px)
        return out

    def _extract_account_value(self, user_state: dict) -> float:
        for path in [
            ("marginSummary", "accountValue"),
            ("crossMarginSummary", "accountValue"),
            ("accountValue",),
            ("equity",),
            ("portfolioValue",),
        ]:
            v = self._dig(user_state, *path)
            if v is not None:
                return float(v)
        raise HyperliquidAPIError("Could not find account/net liquidation value in user state.")

    def _extract_positions(self, user_state: dict, mids: Dict[str, float]) -> List[tuple]:
        results: List[tuple] = []
        raw_lists = []
        for key in ("assetPositions", "perpPositions", "positions"):
            lst = user_state.get(key)
            if isinstance(lst, list):
                raw_lists.append(lst)

        seen = set()
        for raw in raw_lists:
            for entry in raw:
                pos = entry.get("position") if isinstance(entry.get("position"), dict) else entry

                # Symbol
                sym = (pos.get("coin") or pos.get("symbol") or pos.get("asset") or "")
                if not sym:
                    continue
                sym = sym.upper()

                # Quantity
                size = pos.get("szi") or pos.get("size") or pos.get("sz")
                try:
                    qty = float(size)
                except (TypeError, ValueError):
                    continue
                if qty == 0:
                    continue

                # Notional: prefer current notional from positionValue; else compute from mark/mid
                notional = None
                pv = pos.get("positionValue")
                if pv is not None:
                    try:
                        notional = abs(float(pv))
                    except (TypeError, ValueError):
                        notional = None
                if notional is None:
                    mark = None
                    for k in ("markPx", "markPrice", "oraclePx", "px"):
                        v = pos.get(k)
                        if v is not None:
                            try:
                                mark = float(v)
                                break
                            except (TypeError, ValueError):
                                pass
                    if mark is None:
                        mark = mids.get(sym)
                    notional = abs(qty) * float(mark) if mark is not None else 0.0

                key = (sym, round(qty, 12), round(notional, 6))
                if key in seen:
                    continue
                seen.add(key)

                results.append((sym, qty, notional))

        return results

    def _dig(self, obj: dict, *path: str):
        cur = obj
        for k in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
        return cur
