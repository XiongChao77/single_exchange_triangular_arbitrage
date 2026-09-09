# Binance Spot SBE receiver

C++20 market-data ingestion for two triangles:

| Triangle | Binance symbols |
| --- | --- |
| ETH / BTC / USDT | ETHBTC, BTCUSDT, ETHUSDT |
| USDT / BTC / BNB | BTCUSDT, BNBBTC, BNBUSDT |

The shared BTCUSDT stream is subscribed once. All five symbols use
`<symbol>@bestBidAsk` on one TLS WebSocket connection. Symbols are configurable,
normalized to uppercase internally, and deduplicated. This version accepts ASCII
alphanumeric Binance symbol names, up to 1024 unique streams.

## Build and run

Dependencies: CMake 3.20+, a C++20 compiler, Boost 1.74+ headers, OpenSSL 1.1.1+
development files, nlohmann/json headers, and Python 3 for integration tests.
On Ubuntu these are provided by `cmake g++ libboost-dev libssl-dev
nlohmann-json3-dev python3`; the tests also use the `openssl` command.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j2
ctest --test-dir build --output-on-failure
./build/sbe_receiver
```

If dependencies live outside system include paths, supply
`-DBOOST_INCLUDE_DIR=/path/to/include -DJSON_INCLUDE_DIR=/path/to/include`.
In the current workspace the headers are available in the project-local `.deps` directory:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
  -DBOOST_INCLUDE_DIR="$PWD/.deps/usr/include" \
  -DJSON_INCLUDE_DIR="$PWD/.deps/usr/include"
```

The default config path is `configs/binance.json`, defined directly in the code
and resolved relative to the current working directory. Run from the project
root to use this default. Paths inside the config, including `api_key_file`,
resolve relative to the config file. Runtime settings are constants at the top
of `src/main.cpp`: `kConfigPath`, `kRunSeconds = 10`, and `kQuotesPerSymbol = 10`.
Edit these constants and rebuild to change them. Set `kRunSeconds = 0` to run
until Ctrl+C or SIGTERM. There are no command-line options. Start with:

```bash
./build/sbe_receiver
```