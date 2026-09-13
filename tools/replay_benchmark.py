#!/usr/bin/env python3
"""Record public partial depth, then compare receivers with a shared CLOCK_MONOTONIC timeline."""
import argparse, base64, hashlib, json, os, signal, socket, ssl, struct
import subprocess, threading, time, urllib.request, platform
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def exact(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part: raise EOFError('Socket closed')
        data.extend(part)
    return bytes(data)

def frame(opcode, payload, masked=False):
    size = len(payload); flag = 128 if masked else 0
    head = bytes([128 | opcode, flag | (size if size < 126 else 126 if size < 65536 else 127)])
    if size >= 126: head += struct.pack('!H' if size < 65536 else '!Q', size)
    if masked:
        mask = os.urandom(4)
        return head + mask + bytes(x ^ mask[i % 4] for i, x in enumerate(payload))
    return head + payload

def read_frame(sock):
    head = exact(sock, 2); size = head[1] & 127
    if size == 126: size = struct.unpack('!H', exact(sock, 2))[0]
    elif size == 127: size = struct.unpack('!Q', exact(sock, 8))[0]
    mask = exact(sock, 4) if head[1] & 128 else None
    payload = exact(sock, size)
    if mask: payload = bytes(x ^ mask[i % 4] for i, x in enumerate(payload))
    return bool(head[0] & 128), head[0] & 15, payload

def headers(sock):
    data = bytearray()
    while not data.endswith(b'\r\n\r\n'):
        data.extend(exact(sock, 1))
        if len(data) > 65536: raise ValueError('HTTP header too large')
    lines = data.decode().split('\r\n')
    return lines[0], {k.lower(): v.strip() for k, v in (x.split(':', 1) for x in lines[1:] if ':' in x)}

def digest(key):
    return base64.b64encode(hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())

def symbols_of(triangles):
    return list(dict.fromkeys(s for triangle in triangles for s in triangle))

def record(args):
    args.output.mkdir(parents=True, exist_ok=False)
    config = json.loads((ROOT / 'configs/binance.json').read_text())
    with urllib.request.urlopen('https://api.binance.com/api/v3/exchangeInfo', timeout=20) as response:
        metadata = json.load(response)
    markets = {m['symbol']: m for m in metadata['symbols'] if m['status'] == 'TRADING'}
    selected = list(config['triangles']); symbols = set(symbols_of(selected))
    for triangle in json.loads((ROOT / 'binance_triangles.json').read_text())['triangles']:
        if not all(s in markets for s in triangle): continue
        if len(symbols | set(triangle)) > args.max_symbols: continue
        if triangle not in selected: selected.append(triangle); symbols.update(triangle)
    if not all(s in markets for s in symbols): raise ValueError('Current config contains unavailable markets')
    safe_config = {k: config[k] for k in ['commission_taker', 'edge_threshold', 'max_arbitrage_usdt'] if k in config}
    safe_config['execution_mode'] = 'disabled'
    manifest = {'current_triangles': config['triangles'], 'large_triangles': selected,
                'config': safe_config, 'record_seconds': args.seconds,
                'timing': 'Python time.monotonic_ns / C++ steady_clock on the same Linux host'}
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    (args.output / 'exchange_info.json').write_text(json.dumps({'symbols': [markets[s] for s in symbols]}))
    streams = '/'.join(s.lower() + '@depth20@100ms' for s in symbols_of(selected))
    context = ssl.create_default_context()
    with context.wrap_socket(socket.create_connection(('stream.binance.com', 9443), timeout=20), server_hostname='stream.binance.com') as sock:
        key = base64.b64encode(os.urandom(16)).decode()
        sock.sendall((f'GET /stream?streams={streams} HTTP/1.1\r\nHost: stream.binance.com:9443\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
        status, h = headers(sock)
        assert ' 101 ' in status and h['sec-websocket-accept'].encode() == digest(key), status
        begin = time.monotonic_ns(); deadline = time.monotonic() + args.seconds; count = 0; fragments = bytearray()
        with (args.output / 'messages.jsonl').open('w') as output:
            while time.monotonic() < deadline:
                sock.settimeout(max(.001, deadline - time.monotonic()))
                try: final, opcode, payload = read_frame(sock)
                except socket.timeout: break
                if opcode == 9: sock.sendall(frame(10, payload, True)); continue
                if opcode == 8: break
                if opcode == 1: fragments = bytearray(payload)
                elif opcode == 0: fragments.extend(payload)
                else: continue
                if not final: continue
                raw = bytes(fragments).decode(); message = json.loads(raw)
                if not ('stream' in message and 'lastUpdateId' in message.get('data', {})): continue
                output.write(json.dumps({'offset_ns': time.monotonic_ns() - begin, 'payload': raw}) + '\n'); count += 1
    print(json.dumps({'dataset': str(args.output), 'groups': len(selected), 'symbols': len(symbols), 'messages': count}), flush=True)

def distribution(values):
    if not values: return None
    a = sorted(values)
    def p(q):
        x = (len(a) - 1) * q; i = int(x)
        return a[i] + (a[min(i + 1, len(a) - 1)] - a[i]) * (x - i)
    return {k: round(v / 1000, 3) for k, v in {'mean': sum(a) / len(a), 'p50': p(.5), 'p95': p(.95), 'p99': p(.99), 'max': a[-1]}.items()}

def run_once(binary, folder, dataset, scenario, speed, burst, cert, key):
    folder.mkdir(parents=True); (folder / 'configs').mkdir()
    manifest = json.loads((dataset / 'manifest.json').read_text())
    triangles = manifest[scenario + '_triangles']; symbols = symbols_of(triangles)
    allowed = set(s.lower() for s in symbols)
    messages = []
    for line in (dataset / 'messages.jsonl').open():
        row = json.loads(line); payload = json.loads(row['payload'])
        if payload['stream'].split('@')[0] in allowed: messages.append((row['offset_ns'], payload))
    if not messages: raise ValueError('No recorded messages for scenario')
    first = messages[0][0]
    offsets = [int((offset - first) / speed) for offset, _ in messages]
    if burst: offsets = [(i // burst) * 100_000_000 for i in range(len(messages))]
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(cert, key)
    listener = socket.socket(); listener.bind(('127.0.0.1', 0)); listener.listen(); listener.settimeout(20)
    config = dict(manifest['config'], triangles=triangles, host='localhost', port=str(listener.getsockname()[1]),
                  rest_host='localhost', rest_port=str(listener.getsockname()[1]), ca_file=str(cert))
    (folder / 'configs/binance.json').write_text(json.dumps(config))
    complete = threading.Event(); release = threading.Event(); errors = []; sent = []
    def accept():
        raw, _ = listener.accept(); raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock = context.wrap_socket(raw, server_side=True); sock.settimeout(30); return sock
    def serve():
        try:
            with accept() as sock:
                status, _ = headers(sock); assert status.startswith('GET /api/v3/exchangeInfo?'), status
                body = (dataset / 'exchange_info.json').read_bytes()
                sock.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: ' + str(len(body)).encode() + b'\r\nConnection: close\r\n\r\n' + body)
            with accept() as sock:
                status, h = headers(sock)
                target = '/stream?streams=' + '/'.join(s.lower() + '@depth20@100ms' for s in symbols)
                assert status.split(' ')[1] == target, status
                sock.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + digest(h['sec-websocket-key']) + b'\r\n\r\n')
                origin = time.monotonic_ns() + 500_000_000
                frames = []
                for i, ((_, payload), offset) in enumerate(zip(messages, offsets), 1):
                    payload['_replay'] = {'sequence': i, 'scheduled_ns': origin + offset}
                    frames.append(frame(1, json.dumps(payload, separators=(',', ':')).encode()))
                for i, (packet, offset) in enumerate(zip(frames, offsets), 1):
                    scheduled = origin + offset; remaining = scheduled - time.monotonic_ns()
                    if remaining > 0: time.sleep(remaining / 1e9)
                    start = time.monotonic_ns(); sock.sendall(packet); end = time.monotonic_ns()
                    sent.append({'sequence': i, 'scheduled_ns': scheduled, 'send_start_ns': start, 'send_end_ns': end})
                complete.set(); release.wait(20)
        except BaseException as error: errors.append(repr(error)); complete.set()
    thread = threading.Thread(target=serve, daemon=True); thread.start()
    env = dict(os.environ, TRIANGULAR_REPLAY='1')
    with (folder / 'console.txt').open('w') as console:
        process = subprocess.Popen([str(binary)], cwd=folder, env=env, stdout=console, stderr=console)
        begin = time.monotonic(); cpu = 0
        try:
            deadline = time.monotonic() + max(30, offsets[-1] / 1e9 + 25)
            while not complete.wait(.1):
                if process.poll() is not None: raise RuntimeError((folder / 'console.txt').read_text())
                if time.monotonic() > deadline: raise TimeoutError('Replay send timeout')
            if errors: raise RuntimeError(errors)
            # Keep the stream open and allow consumers to drain. Never end at the send deadline.
            log = next((folder / 'logs').glob('*.jsonl'))
            deadline = time.monotonic() + 10; previous = -1; unchanged = time.monotonic()
            while time.monotonic() < deadline:
                count = log.read_text().count('"event":"replay_decision"')
                if count == len(messages): break
                if count != previous: previous = count; unchanged = time.monotonic()
                if time.monotonic() - unchanged > 2: break
                time.sleep(.1)
            stat = Path(f'/proc/{process.pid}/stat').read_text().split()
            cpu = (int(stat[13]) + int(stat[14])) / os.sysconf('SC_CLK_TCK')
            process.send_signal(signal.SIGINT); process.wait(timeout=10)
            if process.returncode: raise RuntimeError((folder / 'console.txt').read_text())
        finally:
            if process.poll() is None: process.kill(); process.wait()
            release.set(); listener.close(); thread.join(timeout=2)
    (folder / 'sender.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in sent))
    rows = [json.loads(line) for line in log.read_text().splitlines()]
    decisions = [r['fields'] for r in rows if r['event'] == 'replay_decision']
    by_seq = {r['sequence']: r for r in sent}; sequences = [r['sequence'] for r in decisions]
    assert len(sequences) == len(set(sequences)) and sequences == sorted(sequences), 'Duplicate/reordered replay decisions'
    assert all(r['decision_ns'] >= r['received_ns'] >= r['scheduled_ns'] for r in decisions), 'Clock mismatch'
    final = next(r['fields'] for r in rows if r['event'] == 'pipeline_final')
    if len(decisions) == len(messages):
        expected = {payload['stream'].split('@')[0].upper(): payload['data']['lastUpdateId']
                    for _, payload in messages}
        actual = {r['fields']['symbol']: r['fields']['book_update_id']
                  for r in rows if r['event'] == 'latest_orderbook'}
        assert actual == expected, 'Final books do not match the recorded sequence'
    result = {'binary': str(binary), 'scenario': scenario, 'speed': speed, 'burst': burst, 'groups': len(triangles), 'symbols': len(symbols),
              'sent': len(sent), 'processed': len(decisions), 'missing_sequences': sorted(set(by_seq) - set(sequences)),
              'cpu_seconds': cpu, 'wall_seconds': time.monotonic() - begin,
              'scheduled_duration_seconds': offsets[-1] / 1e9, 'log': str(log), 'pipeline_final': final,
              'scheduled_to_decision_us': distribution([r['decision_ns'] - r['scheduled_ns'] for r in decisions]),
              'send_start_to_receive_us': distribution([r['received_ns'] - by_seq[r['sequence']]['send_start_ns'] for r in decisions]),
              'send_start_to_decision_us': distribution([r['decision_ns'] - by_seq[r['sequence']]['send_start_ns'] for r in decisions]),
              'receive_to_decision_us': distribution([r['decision_ns'] - r['received_ns'] for r in decisions]),
              'sender_lateness_us': distribution([r['send_start_ns'] - r['scheduled_ns'] for r in sent]),
              'send_call_us': distribution([r['send_end_ns'] - r['send_start_ns'] for r in sent])}
    (folder / 'metrics.json').write_text(json.dumps(result, indent=2)); return result

def benchmark(args):
    args.output.mkdir(parents=True, exist_ok=False)
    cert = args.output / 'cert.pem'; key = args.output / 'key.pem'
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-keyout', str(key), '-out', str(cert),
                    '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'], check=True, capture_output=True)
    environment = {'platform': platform.platform(), 'cpu_count': os.cpu_count(),
                   'clock': str(time.get_clock_info('monotonic')),
                   'binaries': {label: {'path': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
                                for label, binary in [('main', args.main), ('lock-free', args.lock_free)]}}
    (args.output / 'environment.json').write_text(json.dumps(environment, indent=2))
    results = []
    for scenario in args.scenarios:
        loads = [('speed-' + str(speed), speed, 0) for speed in args.speeds] + [('burst-' + str(n), 1, n) for n in args.bursts]
        for name, speed, burst in loads:
            for repeat in range(args.repeats):
                variants = [('main', args.main), ('lock-free', args.lock_free)]
                if repeat % 2: variants.reverse()
                for label, binary in variants:
                    folder = args.output / scenario / name / str(repeat + 1) / label
                    result = run_once(binary, folder, args.dataset, scenario, speed, burst, cert, key)
                    result.update(variant=label, repeat=repeat + 1); results.append(result)
                    (args.output / 'results.json').write_text(json.dumps(results, indent=2))
                    print(json.dumps({k: result[k] for k in ['variant', 'scenario', 'speed', 'burst', 'repeat', 'sent', 'processed', 'cpu_seconds', 'scheduled_to_decision_us', 'sender_lateness_us']}), flush=True)
    lines = ['# Local replay benchmark', '', 'All durations in μs. Disabled execution; identical recordings, sequential Release runs. Missing counts include unprocessed messages and must be read with queue/logger statistics.', '',
             '|Scenario|Load|Repeat|Version|Processed/sent|CPU s|Scheduled→decision P50|P99|Max|Sender lateness P99|', '|---|---|---:|---|---:|---:|---:|---:|---:|---:|']
    for r in results:
        m = r['scheduled_to_decision_us']; load = 'burst-' + str(r['burst']) if r['burst'] else str(r['speed']) + 'x'
        lines.append(f'|{r["scenario"]}|{load}|{r["repeat"]}|{r["variant"]}|{r["processed"]}/{r["sent"]}|{r["cpu_seconds"]}|{m["p50"]}|{m["p99"]}|{m["max"]}|{r["sender_lateness_us"]["p99"]}|')
    lines += ['', 'Scheduled→decision includes sender delays/backpressure. Send-start→decision also available in results.json, but hides lateness accumulated before the send call. Sender delays are reported explicitly; high sender delays prevent treating the requested speed as sustained achieved throughput.', '',
              'Recorded offsets reproduce local arrival timing, not unknown exchange event formation time. ReplayDecision is extra test logging enabled only by TRIANGULAR_REPLAY=1. Same Linux host monotonic clock required. Busy polling consumes a core. No claim about live exchange-to-decision latency.']
    (args.output / 'report.md').write_text('\n'.join(lines) + '\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__); commands = parser.add_subparsers(dest='command', required=True)
    rec = commands.add_parser('record'); rec.add_argument('--output', type=Path, required=True); rec.add_argument('--seconds', type=float, default=15); rec.add_argument('--max-symbols', type=int, default=180)
    bench = commands.add_parser('benchmark'); bench.add_argument('--dataset', type=Path, required=True); bench.add_argument('--output', type=Path, required=True)
    bench.add_argument('--main', type=Path, required=True); bench.add_argument('--lock-free', type=Path, required=True)
    bench.add_argument('--scenarios', nargs='+', choices=['current', 'large'], default=['current', 'large'])
    bench.add_argument('--speeds', nargs='+', type=float, default=[1, 10, 50]); bench.add_argument('--bursts', nargs='*', type=int, default=[500]); bench.add_argument('--repeats', type=int, default=2)
    args = parser.parse_args()
    for name in ['output', 'dataset', 'main', 'lock_free']:
        if hasattr(args, name): setattr(args, name, getattr(args, name).resolve())
    if args.command == 'benchmark' and (args.repeats < 1 or any(x <= 0 for x in args.speeds + args.bursts)): parser.error('Positive load and repeat values required')
    record(args) if args.command == 'record' else benchmark(args)
