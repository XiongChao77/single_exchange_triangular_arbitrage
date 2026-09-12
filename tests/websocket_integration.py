"""Exercise TLS and 20-level partial depth streams at 100 ms."""
import base64, hashlib, json
from pathlib import Path
import signal, socket, ssl, struct, subprocess, sys, tempfile, threading

SYMBOLS=["ETHBTC","ETHUSDT","BTCUSDT","BNBBTC","BNBUSDT"]

def exact(sock,size):
    data=b""
    while len(data)<size:
        part=sock.recv(size-len(data))
        if not part: raise EOFError("Unexpected EOF")
        data+=part
    return data

def frame(opcode,payload):
    if len(payload)<126: return bytes([0x80|opcode,len(payload)])+payload
    return bytes([0x80|opcode,126])+struct.pack("!H",len(payload))+payload

def read_frame(sock):
    head=exact(sock,2); size=head[1]&127
    if size==126: size=struct.unpack("!H",exact(sock,2))[0]
    elif size==127: size=struct.unpack("!Q",exact(sock,8))[0]
    mask=exact(sock,4) if head[1]&128 else b""
    data=exact(sock,size)
    if mask: data=bytes(value^mask[i%4] for i,value in enumerate(data))
    return head[0]&15,data

def http_request(sock):
    data=b""
    while not data.endswith(b"\r\n\r\n"): data+=exact(sock,1)
    lines=data.decode().split("\r\n")
    headers={k.lower():v for k,v in (line.split(": ",1) for line in lines[1:] if ": " in line)}
    return lines[0].split(" ")[1],headers

def upgrade(sock,expected):
    target,headers=http_request(sock); assert target==expected,target
    digest=hashlib.sha1((headers["sec-websocket-key"]+"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
    sock.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "+base64.b64encode(digest)+b"\r\n\r\n")

def levels(start,step):
    return [[f"{start+i*step:.8f}","10.00000"] for i in range(20)]

def depth_event(symbol,update):
    data={"lastUpdateId":update,"bids":levels(1.0,-0.001),"asks":levels(1.01,0.001)}
    return json.dumps({"stream":symbol.lower()+"@depth20@100ms","data":data}).encode()

def main():
    binary=str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="depth-test-") as folder:
        root=Path(folder); cert=root/"cert.pem"; key=root/"key.pem"
        subprocess.run(["openssl","req","-x509","-newkey","rsa:2048","-nodes","-days","1","-keyout",str(key),
                        "-out",str(cert),"-subj","/CN=localhost","-addext","subjectAltName=DNS:localhost"],
                       check=True,capture_output=True)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(cert,key)
        listener=socket.socket(); listener.bind(("127.0.0.1",0)); listener.listen(); listener.settimeout(15)
        port=str(listener.getsockname()[1])
        config={"execution_mode":"paper","host":"localhost","port":port,"rest_host":"localhost","rest_port":port,
                "ca_file":"../cert.pem",
                "triangles":[["ETHBTC","ETHUSDT","BTCUSDT"],["BTCUSDT","BNBBTC","BNBUSDT"]]}
        (root/"configs").mkdir(); (root/"configs/binance.json").write_text(json.dumps(config))
        errors=[]; complete=threading.Event()

        def accept_tls():
            raw,_=listener.accept(); sock=context.wrap_socket(raw,server_side=True); sock.settimeout(8); return sock

        def serve():
            try:
                with accept_tls() as sock:
                    target,_=http_request(sock); assert target.startswith("/api/v3/exchangeInfo?")
                    assets=[("ETH","BTC"),("ETH","USDT"),("BTC","USDT"),("BNB","BTC"),("BNB","USDT")]
                    body=json.dumps({"symbols":[{"symbol":s,"baseAsset":b,"quoteAsset":q,"status":"TRADING",
                      "isSpotTradingAllowed":True,"filters":[
                      {"filterType":"PRICE_FILTER","tickSize":"0.00000001","minPrice":"0","maxPrice":"0"},
                      {"filterType":"LOT_SIZE","stepSize":"0.00001","minQty":"0.00001","maxQty":"1000000"},
                      {"filterType":"MIN_NOTIONAL","minNotional":"0.01"}]} for s,(b,q) in zip(SYMBOLS,assets)]}).encode()
                    sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: "+str(len(body)).encode()+b"\r\nConnection: close\r\n\r\n"+body)
                stream=accept_tls(); expected="/stream?streams="+"/".join(s.lower()+"@depth20@100ms" for s in SYMBOLS)
                upgrade(stream,expected)
                for symbol in SYMBOLS: stream.sendall(frame(1,depth_event(symbol,101)))
                stream.sendall(frame(1,depth_event("BTCUSDT",102)))
                stream.sendall(frame(9,b"processed")); assert read_frame(stream)==(10,b"processed")
                complete.set()
                try: stream.recv(1)
                except (ssl.SSLError,ConnectionResetError): pass
                stream.close()
            except BaseException as error:
                errors.append(error); complete.set()

        thread=threading.Thread(target=serve,daemon=True); thread.start()
        process=subprocess.Popen([binary],cwd=root,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            assert complete.wait(15),"Server timed out"; process.send_signal(signal.SIGTERM)
            stdout,stderr=process.communicate(timeout=8); thread.join(timeout=5)
            if errors: raise errors[0]
            assert process.returncode==0,(stdout,stderr)
            rows=[json.loads(line) for line in max((root/"logs").glob("*.jsonl")).read_text().splitlines()]
            events=lambda name:[row["fields"] for row in rows if row["event"]==name]
            final=events("pipeline_final")[0]
            assert final["orderbook_store"]=={"received":6,"symbols":5,"populated":5},final
            received=events("receiver_orderbook")
            assert len(received)==6 and all(row["bid_levels"]==20 and row["ask_levels"]==20 for row in received)
            assert all(row["event_time_us"] is None for row in received)
            timing_fields=("json_parse_ns","depth_decode_ns","symbol_lookup_ns","book_update_ns","scan_edge_ns","opportunity_ns")
            for row in received:
                assert all(row[field]>=0 for field in timing_fields),row
                assert row["processing_before_log_ns"]>=sum(row[field] for field in timing_fields),row
                assert isinstance(row["edge_found"],bool)
            latest=events("latest_orderbook"); assert len(latest)==5
            assert next(row for row in latest if row["symbol"]=="BTCUSDT")["book_update_id"]==102
        finally:
            if process.poll() is None: process.kill(); process.communicate()
            listener.close()
        print("20-level partial depth stream passed")

if __name__=="__main__": main()
