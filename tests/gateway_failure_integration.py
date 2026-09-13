"""Exercise real gateway error handling using fake credentials and a loopback TLS server."""
import json
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
from urllib.parse import parse_qs, urlsplit


def read_request(sock):
    data = bytearray()
    while not data.endswith(b"\r\n\r\n"):
        part = sock.recv(1)
        if not part:
            raise EOFError("Request closed")
        data.extend(part)
    return data.decode().split("\r\n", 1)[0].split(" ")[1]


def reply(sock, status, body):
    body = body.encode()
    sock.sendall(f"HTTP/1.1 {status} Test\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body)


def run_case(binary, root, context, case, http_status, code, expected_status, stage):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(8)
    port = str(listener.getsockname()[1])
    errors = []
    signatures = []

    def serve():
        try:
            raw, _ = listener.accept()
            with context.wrap_socket(raw, server_side=True) as sock:
                sock.settimeout(5)
                assert urlsplit(read_request(sock)).path == "/api/v3/account"
                if case == "connect_failure":
                    listener.close()
                reply(sock, 200, json.dumps({"balances": [{"asset": "USDT", "free": "100"}]}))
            if case == "connect_failure":
                return
            raw, _ = listener.accept()
            if case == "tls_failure":
                raw.close()
                return
            with context.wrap_socket(raw, server_side=True) as sock:
                sock.settimeout(5)
                target = read_request(sock)
                assert urlsplit(target).path == "/api/v3/order"
                signature = parse_qs(urlsplit(target).query)["signature"][0]
                signatures.append(signature)
                if case == "read_failure":
                    return
                if case == "invalid_json":
                    body = "not JSON"
                elif case == "invalid_schema":
                    body = "{}"
                else:
                    body = json.dumps({"code": code, "msg":
                        "Filter failure: LOT_SIZE " + "fake-api-key " + "fake-secret " + signature})
                reply(sock, http_status, body)
        except BaseException as error:
            errors.append(error)
        finally:
            listener.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    result = subprocess.run([binary, port, str(root / "cert.pem"), str(root / "api"), str(root / "secret")],
                            capture_output=True, text=True, timeout=12)
    thread.join(timeout=8)
    assert not thread.is_alive() and not errors, (case, errors)
    assert result.returncode == 0, (case, result.stderr)
    report = json.loads(result.stdout)
    detail = report["failure"]
    assert report["status"] == expected_status, (case, report)
    assert detail["stage"] == stage, (case, report)
    assert detail["method"] == "POST" and detail["endpoint"] == "/api/v3/order"
    assert detail["outcome_uncertain"] == (expected_status == "UNKNOWN")
    if stage in ("exchange_response", "response_parse", "response_decode"):
        assert detail["http_status"] == http_status
        assert detail["request_write_completed"] is True
    if stage == "exchange_response":
        assert detail["binance_code"] == code
        assert detail["binance_message"].startswith("Filter failure: LOT_SIZE")
    if stage in ("tcp_connect", "tls_handshake", "http_read"):
        assert detail["error_category"] and isinstance(detail["error_code"], int)
    encoded = json.dumps(report)
    assert "fake-api-key" not in encoded and "fake-secret" not in encoded
    assert all(signature not in encoded for signature in signatures)
    assert "signature=" not in encoded and "recvWindow=" not in encoded
    print(case + ": passed")


def main():
    binary = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="gateway-failure-") as folder:
        root = Path(folder)
        (root / "api").write_text("fake-api-key")
        (root / "secret").write_text("fake-secret")
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-keyout", str(root / "key.pem"), "-out", str(root / "cert.pem"),
                        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost"],
                       check=True, capture_output=True)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(root / "cert.pem", root / "key.pem")
        for case in [
            ("exchange_reject", 400, -1013, "REJECTED", "exchange_response"),
            ("error_in_200", 200, -2010, "REJECTED", "exchange_response"),
            ("server_error", 500, -1000, "UNKNOWN", "exchange_response"),
            ("backend_timeout", 400, -1007, "UNKNOWN", "exchange_response"),
            ("unexpected_backend_response", 400, -1006, "UNKNOWN", "exchange_response"),
            ("invalid_json", 200, None, "UNKNOWN", "response_parse"),
            ("invalid_schema", 200, None, "UNKNOWN", "response_decode"),
            ("connect_failure", None, None, "REJECTED", "tcp_connect"),
            ("tls_failure", None, None, "REJECTED", "tls_handshake"),
            ("read_failure", None, None, "UNKNOWN", "http_read"),
        ]:
            run_case(binary, root, context, *case)


if __name__ == "__main__":
    main()
