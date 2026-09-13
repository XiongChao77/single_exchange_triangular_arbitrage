"""
Binance 三角套利 symbol 组合生成器
--------------------------------
从 Binance 现货 exchangeInfo 接口拉取全部交易对，自动找出所有可以构成
三角套利的 symbol 组合，排除法币和 USDT 以外的稳定币，并按必需资产过滤。

用法:
    pip install requests
    python src/required_asset.py

输出:
    控制台打印匹配数量，并生成 binance_triangles.json，
    格式与需求一致，例如:
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

# ========== 配置项 ==========
# 三角组合中必须包含以下资产之一才算匹配。
# 留空 set() 表示不过滤，输出全部三角组合（数量会非常多）。
REQUIRED_ASSETS = {"USDT"}

# 法币资产排除表：新增法币代码时在这里补充。
# 按 exchangeInfo 的 baseAsset / quoteAsset 精确匹配，不按 symbol 后缀匹配。
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

# 稳定币资产代码表（不代表这些资产目前均在 Binance 现货上市）。
# 稳定币中只允许 USDT；新增资产时需维护此表，不能只按 USD 字样判断。
STABLECOIN_ASSETS = {
    "USDT", "USDC", "FDUSD", "USD1", "USDS", "DAI", "USDE", "U",
    "TUSD", "USDP", "BUSD", "GUSD", "PYUSD", "RLUSD", "FRAX", "USDD",
    "USDG", "XUSD", "AEUR", "EURI", "EURC", "EURT", "EURS",
}
EXCLUDED_ASSETS = FIAT_ASSETS | (STABLECOIN_ASSETS - {"USDT"})

# 只统计允许现货交易、状态为 TRADING 的交易对
EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"


def fetch_symbols():
    """拉取可交易且不含法币或其他稳定币的 (base, quote, symbol) 三元组。"""
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
    构建资产之间的邻接关系：
    pair_symbol[(assetA, assetB)] = 交易对名称（无序，正反都存方便查询）
    asset_neighbors[asset]        = 与该资产直接可交易的其他资产集合
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
    枚举所有三角组合：
    对每个资产 A，在其邻居集合中找两两互相连接的 (B, C)，
    即 A-B、B-C、A-C 三条边都存在，构成一个三角形。
    用 frozenset({A, B, C}) 去重，避免同一三角重复计入。
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

    label = "、".join(REQUIRED_ASSETS) if REQUIRED_ASSETS else "不限"
    print(f"共找到 {len(triangles)} 个三角组合（必需资产: {label}）")

    result = {"triangles": triangles}
    with open("binance_triangles.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("已保存到 binance_triangles.json")


if __name__ == "__main__":
    main()
