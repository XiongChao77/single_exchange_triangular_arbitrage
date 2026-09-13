"""
Binance triangular arbitrage symbol combination generator
--------------------------------
Fetches Binance Spot exchangeInfo, finds triangular symbol combinations,
excludes fiat and non-USDT stablecoins, and filters by required assets.

Usage:
    pip install requests
    python src/required_asset.py

Output:
    Prints the match count and writes binance_triangles.json, for example:
    {
      "triangles": [
        ["ETHBTC", "ETHUSDT", "BTCUSDT"],
        ["BTCUSDT", "BNBBTC", "BNBUSDT"]
      ]
    }
"""

import json
from itertools import combinations

import requests

# ========== Configuration ==========
# A triangle must contain at least one of these assets.
# An empty set disables filtering and may produce many combinations.
REQUIRED_ASSETS = {"USDT"}

# Fiat asset exclusion list. Add new fiat codes here.
# Matches exchangeInfo baseAsset/quoteAsset exactly, not symbol suffixes.
FIAT_ASSETS = {
    "AED",
    "ARS",
    "AUD",
    "AZN",
    "BAM",
    "BDT",
    "BGN",
    "BOB",
    "BRL",
    "BYN",
    "CAD",
    "CHF",
    "CLP",
    "CNY",
    "COP",
    "CRC",
    "CZK",
    "DKK",
    "DOP",
    "DZD",
    "EGP",
    "EUR",
    "GBP",
    "GEL",
    "GHS",
    "HKD",
    "HUF",
    "IDR",
    "ILS",
    "INR",
    "ISK",
    "JPY",
    "KES",
    "KHR",
    "KRW",
    "KWD",
    "KZT",
    "LKR",
    "MAD",
    "MDL",
    "MMK",
    "MNT",
    "MXN",
    "MYR",
    "NGN",
    "NOK",
    "NPR",
    "NZD",
    "PEN",
    "PHP",
    "PKR",
    "PLN",
    "PYG",
    "QAR",
    "RON",
    "RSD",
    "RUB",
    "SAR",
    "SEK",
    "SGD",
    "THB",
    "TND",
    "TRY",
    "TWD",
    "UAH",
    "UGX",
    "USD",
    "UYU",
    "UZS",
    "VES",
    "VND",
    "XAF",
    "XOF",
    "ZAR",
    "ZMW",
}

# Stablecoin codes (not all are necessarily listed on Binance Spot).
# Only USDT is allowed; maintain this list instead of matching USD text.
STABLECOIN_ASSETS = {
    "USDT", "USDC", "FDUSD", "USD1", "USDS", "DAI", "USDE", "U",
    "TUSD", "USDP", "BUSD", "GUSD", "PYUSD", "RLUSD", "FRAX", "USDD",
    "USDG", "XUSD", "AEUR", "EURI", "EURC", "EURT", "EURS",
}
EXCLUDED_ASSETS = FIAT_ASSETS | (STABLECOIN_ASSETS - {"USDT"})

# Count only Spot-enabled symbols whose status is TRADING.
EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"


def fetch_symbols():
    """Fetch tradable (base, quote, symbol) tuples without excluded assets."""
    resp = requests.get(EXCHANGE_INFO_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    symbols = []
    for s in data["symbols"]:
        if s["status"] != "TRADING":
            continue
        if not s.get("isSpotTradingAllowed", True):
            continue
        if s["baseAsset"] in EXCLUDED_ASSETS or s["quoteAsset"] in EXCLUDED_ASSETS:
            continue
        symbols.append((s["baseAsset"], s["quoteAsset"], s["symbol"]))
    return symbols


def build_graph(symbols):
    """
    Build asset adjacency maps:
    pair_symbol[(assetA, assetB)] = symbol (stored in both directions)
    asset_neighbors[asset]        = assets directly tradable against it
    """
    pair_symbol = {}
    asset_neighbors = {}

    for base, quote, symbol in symbols:
        pair_symbol[(base, quote)] = symbol
        pair_symbol[(quote, base)] = symbol
        asset_neighbors.setdefault(base, set()).add(quote)
        asset_neighbors.setdefault(quote, set()).add(base)

    return pair_symbol, asset_neighbors


def find_triangles(pair_symbol, asset_neighbors, required_assets):
    """
    Enumerate triangles: for each asset A, find connected neighbor pairs
    (B, C) such that A-B, B-C, and A-C all exist. Deduplicate with a frozenset.
    """
    seen = set()
    triangles = []

    for a in asset_neighbors:
        neighbors = asset_neighbors[a]
        for b, c in combinations(neighbors, 2):
            if c in asset_neighbors.get(b, ()):
                tri_key = frozenset({a, b, c})
                if tri_key in seen:
                    continue
                seen.add(tri_key)

                if required_assets and not (tri_key & required_assets):
                    continue

                triangles.append(
                    sorted(
                        [
                            pair_symbol[(a, b)],
                            pair_symbol[(b, c)],
                            pair_symbol[(a, c)],
                        ]
                    )
                )

    return triangles


def main():
    symbols = fetch_symbols()
    pair_symbol, asset_neighbors = build_graph(symbols)
    triangles = find_triangles(pair_symbol, asset_neighbors, REQUIRED_ASSETS)

    label = ", ".join(REQUIRED_ASSETS) if REQUIRED_ASSETS else "unrestricted"
    print(f"Found {len(triangles)} triangular combinations (required assets: {label})")

    result = {"triangles": triangles}
    with open("binance_triangles.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("Saved to binance_triangles.json")


if __name__ == "__main__":
    main()
