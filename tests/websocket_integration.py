"""Exercise the real async TLS client with a local WebSocket fixture server."""

import base64
import hashlib
import json
from pathlib import Path
import signal
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading


SYMBOLS = ["ETHBTC", "ETHUSDT", "BTCUSDT", "BNBBTC", "BNBUSDT"]
PRIVATE_BODY = "local-response-body-do-not-log"


def frame(opcode, payload, final=True):
    header = bytes([(0x80 if final else 0) | opcode])
    if len(payload) < 126:
        return header + bytes([len(payload)]) + payload
    return header + bytes([126]) + struct.pack("!H", len(payload)) + payload


def exact(sock, size):
    data = b""
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise EOFError("Unexpected EOF")
        data += part
    return data


def read_frame(sock):
    head = exact(sock, 2)
    size = head[1] & 127
    if size == 126:
        size = struct.unpack("!H", exact(sock, 2))[0]
    elif size == 127:
        size = struct.unpack("!Q", exact(sock, 8))[0]
    assert head[1] & 128, "Client frames must be masked"
    mask = exact(sock, 4)
    data = exact(sock, size)
    return head[0] & 15, bytes(value ^ mask[i % 4] for i, value in enumerate(data))


def orderbook_message(symbol, update):
    data = {"u": update, "s": symbol, "b": "0.04200001", "B": "2.00000",
            "a": "0.04200002", "A": "3.00000"}
    return json.dumps({"stream": symbol.lower() + "@bookTicker", "data": data}).encode()



def request(sock):
    data = b""
    while not data.endswith(b"\r\n\r\n"):
        data += exact(sock, 1)
        assert len(data) < 16384
    lines = data.decode().split("\r\n")
    expected = "/stream?streams=" + "/".join(s.lower() + "@bookTicker" for s in SYMBOLS)
    assert lines[0] == f"GET {expected} HTTP/1.1", lines[0]
    headers = dict(line.split(": ", 1) for line in lines[1:] if ": " in line)
    headers = {key.lower(): value for key, value in headers.items()}
    assert "x-mbx-apikey" not in headers
    return headers


def upgrade(sock, headers):
    digest = hashlib.sha1((headers["sec-websocket-key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
    sock.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                 + base64.b64encode(digest) + b"\r\n\r\n")


def main():
    binary = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="json-test-") as folder:
        root = Path(folder)
        cert, private = root / "cert.pem", root / "private.pem"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-keyout", str(private), "-out", str(cert), "-subj", "/CN=localhost",
                        "-addext", "subjectAltName=DNS:localhost"], check=True, capture_output=True)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, private)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(15)
        config = {"execution_mode": "paper", "host": "localhost", "port": str(listener.getsockname()[1]),
                  "ca_file": "../cert.pem", "rest_host": "localhost", "rest_port": str(listener.getsockname()[1]),
                  "triangles": [["ethbtc", "ETHUSDT", "BTCUSDT"], ["BTCUSDT", "BNBBTC", "BNBUSDT"]]}
        (root / "configs").mkdir()
        config_path = root / "configs" / "binance.json"
        config_path.write_text(json.dumps(config))
        errors = []
        complete = threading.Event()

        def serve():
            try:
                raw, _ = listener.accept()
                with context.wrap_socket(raw, server_side=True) as sock:
                    sock.settimeout(5)
                    data = b""
                    while not data.endswith(b"\r\n\r\n"):
                        data += exact(sock, 1)
                    from urllib.parse import urlsplit, parse_qs
                    target = data.decode().split(" ")[1]
                    assert urlsplit(target).path == "/api/v3/exchangeInfo"
                    assert json.loads(parse_qs(urlsplit(target).query)["symbols"][0]) == SYMBOLS
                    assets = [("ETH", "BTC"), ("ETH", "USDT"), ("BTC", "USDT"), ("BNB", "BTC"), ("BNB", "USDT")]
                    body = json.dumps({"symbols": [{"symbol": symbol, "baseAsset": base, "quoteAsset": quote,
                        "status": "TRADING", "isSpotTradingAllowed": True,
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.00000001", "minPrice": "0", "maxPrice": "0"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.00001", "minQty": "0.00001", "maxQty": "1000000"},
                            {"filterType": "MIN_NOTIONAL", "minNotional": "5"}
                        ]} for symbol, (base, quote) in zip(SYMBOLS, assets)]}).encode()
                    sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body)
                # HTTP failure followed by reconnect, without logging the response body.
                raw, _ = listener.accept()
                with context.wrap_socket(raw, server_side=True) as sock:
                    sock.settimeout(5)
                    request(sock)
                    body = PRIVATE_BODY.encode()
                    sock.sendall(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: " + str(len(body)).encode()
                                 + b"\r\nConnection: close\r\n\r\n" + body)
                raw, _ = listener.accept()
                with context.wrap_socket(raw, server_side=True) as sock:
                    sock.settimeout(5)
                    upgrade(sock, request(sock))
                    sock.sendall(frame(1, b'{"result":null,"id":1}'))
                    sock.sendall(frame(2, b"bad"))
                    sock.sendall(frame(1, b"{malformed"))
                    sock.sendall(frame(1, b'{"stream":"btcusdt@bookTicker","data":{}}'))
                    for index, symbol in enumerate(SYMBOLS):
                        payload = orderbook_message(symbol, index + 1)
                        if index == 0:
                            sock.sendall(frame(1, payload[:20], final=False))
                            sock.sendall(frame(9, b"ping-payload"))
                            sock.sendall(frame(0, payload[20:]))
                            assert read_frame(sock) == (10, b"ping-payload"), "Pong mismatch"
                        else:
                            sock.sendall(frame(1, payload))
                    sock.sendall(frame(1, b'{"e":"serverShutdown","E":1788800000000}'))
                    # EOF verifies the old session is canceled before reconnecting.
                    try:
                        assert sock.recv(1) == b""
                    except (ssl.SSLError, ConnectionResetError):
                        pass
                raw, _ = listener.accept()
                with context.wrap_socket(raw, server_side=True) as sock:
                    sock.settimeout(5)
                    upgrade(sock, request(sock))
                    for update in range(6, 57):
                        sock.sendall(frame(1, orderbook_message("BTCUSDT", update)))
                    # A pong confirms the final orderbook has passed through async_read.
                    sock.sendall(frame(9, b"processed"))
                    assert read_frame(sock) == (10, b"processed")
                    complete.set()
                    try:
                        sock.recv(1)
                    except (ssl.SSLError, ConnectionResetError):
                        pass
            except BaseException as error:
                errors.append(error)
                complete.set()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        process = subprocess.Popen([binary], cwd=root,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            assert complete.wait(15), "Server timed out"
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=5)
            thread.join(timeout=5)
            assert not thread.is_alive(), "Server thread did not stop"
            if errors:
                raise errors[0]
            assert process.returncode == 0, stderr
            log_text = max((root / "logs").glob("*.jsonl")).read_text()
            rows = [json.loads(line) for line in log_text.splitlines()]
            events = lambda name: [r["fields"] for r in rows if r["event"] == name]
            assert PRIVATE_BODY not in stdout + stderr + log_text, "Private response body was logged"
            assert any(r["http_status"] == 401 for r in events("connection_lost"))
            assert any(r["stage"] == "serverShutdown" for r in events("connection_lost"))
            execution = events("execution_final")[0]
            assert execution["state"] == "IDLE" and execution["accepted"] > 0, execution
            assert execution["completed"] == 0 and execution["failed"] == execution["accepted"], execution
            final = events("pipeline_final")[0]
            orderbook_stats = final["orderbook_store"]
            assert orderbook_stats == {"received": 56, "symbols": 5, "populated": 5}, final
            assert events("run_started")[0]["triangle_indices"] == [[0, 1, 2], [2, 3, 4]]
            assert final["receiver"]["invalid"] == 3, final
            rx = events("receiver_orderbook")
            assert all(r["event_time_us"] is None and r["exchange_to_receive_us"] is None for r in rx)
            assert all(r["bid_price"] == 4200001 and r["ask_price"] == 4200002 and
                       r["price_exponent"] == -8 and r["qty_exponent"] == -5 for r in rx)
            analyzer = Path(__file__).resolve().parents[1] / "scripts" / "analyze_logs.py"
            summary = json.loads(subprocess.check_output([sys.executable, str(analyzer),
                                 str(max((root / "logs").glob("*.jsonl")))], text=True))
            assert summary["exchange_to_receive_us_clock_dependent"] is None
            assert summary["accounting_ok"]
            assert len(rx) == 56
            latest = events("latest_orderbook")
            assert len(latest) == 5
            assert {r["symbol"]: r["index"] for r in latest} == dict(zip(SYMBOLS, range(5)))
            assert next(r for r in latest if r["symbol"] == "BTCUSDT")["book_update_id"] == 56
            logger = events("logger_final")[0]
            assert logger["dropped"] == 0 and logger["write_lost"] == 0 and not logger["failed"], logger
            assert logger["written"] == logger["accepted"] == logger["submitted"], logger
            assert logger["queued"] == logger["in_flight"] == 0, logger
            for symbol in SYMBOLS:
                assert any(r["symbol"] == symbol for r in events("first_orderbook"))
            assert any(r["symbol"] == "BTCUSDT" and r["received"] == 52 for r in events("symbol_summary"))
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
            listener.close()

        # An untrusted certificate must fail before the WebSocket handshake.
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(5)
        config.pop("ca_file")
        config["port"] = config["rest_port"] = str(listener.getsockname()[1])
        config_path.write_text(json.dumps(config))
        process = subprocess.Popen([binary], cwd=root,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            raw, _ = listener.accept()
            raw.settimeout(3)
            try:
                with context.wrap_socket(raw, server_side=True):
                    raise AssertionError("Untrusted TLS certificate was accepted")
            except ssl.SSLError:
                pass
            _, stderr = process.communicate(timeout=12)
            assert process.returncode == 1, stderr
            assert "exchangeInfo request failed" in stderr and "certificate verify failed" in stderr, stderr
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
            listener.close()
        # Invalid metadata must stop startup before any WebSocket subscription.
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(5)
        config["ca_file"] = "../cert.pem"
        config["port"] = config["rest_port"] = str(listener.getsockname()[1])
        config_path.write_text(json.dumps(config))
        process = subprocess.Popen([binary], cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            raw, _ = listener.accept()
            with context.wrap_socket(raw, server_side=True) as sock:
                sock.settimeout(5)
                request_bytes = b""
                while not request_bytes.endswith(b"\r\n\r\n"):
                    request_bytes += exact(sock, 1)
                assert request_bytes.startswith(b"GET /api/v3/exchangeInfo?")
                body = b'{"symbols":[]}'
                sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body)
            _, stderr = process.communicate(timeout=5)
            assert process.returncode == 1 and "Unknown symbol: ETHBTC" in stderr, stderr
            listener.settimeout(0.2)
            try:
                unexpected, _ = listener.accept()
                unexpected.close()
                raise AssertionError("Subscribed before passing startup validation")
            except socket.timeout:
                pass
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
            listener.close()
        print("TLS, public access, multi-symbol JSON, fragmentation, pong, latest orderbooks, startup validation, reconnect, and shutdown passed")


if __name__ == "__main__":
    main()
