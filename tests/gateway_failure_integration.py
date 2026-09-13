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


def run_reuse_case(binary, root, context, mode):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0)); listener.listen(); listener.settimeout(5)
    port = str(listener.getsockname()[1])
    errors, client_ids = [], []
    connections = 0

    def respond(sock, body, close=False):
        payload = json.dumps(body).encode()
        connection = "close" if close else "keep-alive"
        sock.sendall(f"HTTP/1.1 200 Test\r\nContent-Length: {len(payload)}\r\nConnection: {connection}\r\n\r\n".encode()+payload)

    def serve():
        nonlocal connections
        try:
            account_done, orders = False, 0
            while orders < 3:
                raw,_ = listener.accept(); connections += 1
                with context.wrap_socket(raw,server_side=True) as sock:
                    sock.settimeout(5)
                    while orders < 3:
                        target = read_request(sock)
                        if not account_done:
                            assert urlsplit(target).path == "/api/v3/account"
                            respond(sock,{"balances":[{"asset":"USDT","free":"100"}]})
                            account_done = True
                            continue
                        assert urlsplit(target).path == "/api/v3/order"
                        client_ids.append(parse_qs(urlsplit(target).query)["newClientOrderId"][0])
                        orders += 1
                        if mode == "read_failure" and orders == 1:
                            break
                        close = mode == "server_close" and orders == 1
                        if mode == "filled":
                            respond(sock,{"status":"FILLED","executedQty":"20","cummulativeQuoteQty":"970.8",
                                "orderId":orders,"fills":[{"qty":"20","commission":"0.001","commissionAsset":"TRY"}]})
                        else:
                            respond(sock,{"code":-1013,"msg":"Synthetic rejection"},close)
                        if close: break
        except BaseException as error:
            errors.append(error)
        finally:
            listener.close()

    thread=threading.Thread(target=serve,daemon=True); thread.start()
    result=subprocess.run([binary,port,str(root/"cert.pem"),str(root/"api"),str(root/"secret"),"3"],
                          capture_output=True,text=True,timeout=10)
    thread.join(timeout=5)
    assert not thread.is_alive() and not errors,(mode,errors)
    assert result.returncode==0,(mode,result.stderr)
    reports=json.loads(result.stdout)
    assert client_ids==["probe-first-leg-0","probe-first-leg-1","probe-first-leg-2"],(mode,client_ids)
    assert connections==(1 if mode in ("keep_alive","filled") else 2),(mode,connections)
    assert len(reports)==3
    for index,report in enumerate(reports):
        expected="UNKNOWN" if mode=="read_failure" and index==0 else "FILLED" if mode=="filled" else "REJECTED"
        assert report["status"]==expected,(mode,report)
        detail=report["latency"]["transport"]
        reused=mode in ("keep_alive","filled") or index!=1
        assert detail["connection_reused"]==reused,(mode,index,detail)
        assert len(report["write_events"])==index+1,(mode,index,report)
        assert report["write_events"][-1]["client_id"]==client_ids[index]
        latency=report["latency"]
        assert latency["gateway_enqueued_ns"]<=latency["worker_begin_ns"]<=latency["sign_begin_ns"]<=latency["sign_end_ns"]
        assert latency["report_ready_ns"]<=latency["callback_received_ns"]
        if not (mode=="read_failure" and index==0):
            tx=detail["kernel_tx"]
            assert tx["enabled"] and tx["final_byte_timestamp_present"],(mode,index,tx)
            assert tx["kernel_tx_sched_realtime_ns"]<=tx["kernel_tx_software_realtime_ns"],tx
            assert tx["ciphertext_end"]>tx["ciphertext_begin"],tx
            points=detail["timepoints"]
            assert points["http_write_begin_ns"]<=points["http_write_end_ns"]<=points["http_response_ns"]
        if mode=="read_failure" and index==0:
            assert detail["stage"]=="http_read" and detail["outcome_uncertain"] is True
        if reused:
            assert "tls_handshake" not in detail["timing_us"],detail
        else:
            assert "tls_handshake" in detail["timing_us"],detail
    print("connection_"+mode+": passed")


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
        for mode in ("keep_alive", "server_close", "read_failure", "filled"):
            run_reuse_case(binary, root, context, mode)


if __name__ == "__main__":
    main()
