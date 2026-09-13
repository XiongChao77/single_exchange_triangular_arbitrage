# External Bidirectional Capture and Business Correlation

This tool validates the boundary between market-data arrival and first-leg order submission. It combines application
logs, local capture, external capture, and TLS record reconstruction, then reports measured latency and the portion
that remains unassigned.

Recommended wiring is trading host -> switch source port -> upstream, with ingress and egress mirrored to a
dedicated capture interface. The capture host must not run the trading process. A network TAP is also suitable.
Ensure both directions are captured and hardware timestamp clocks are aligned when multiple interfaces are used.

## Capture

Start capture before the trading process so that TCP/TLS handshakes and the WebSocket HTTP upgrade are included.
Replace the interface, trading-host MAC, and output path in this example. Disable GRO/LRO on the capture interface.

```bash
sudo ethtool -K enp2s0 gro off lro off
sudo tcpdump -i enp2s0 -nn -s 0 -B 32768 -U \
  -w capture.pcap 'ether host aa:bb:cc:dd:ee:ff and tcp'
```

Keep tcpdump's dropped-packet count and inspect mirror-port counters. A shared capture-host clock means directional
differences do not require trading-host clock synchronization, but capture software, drivers, and mirroring can add
direction-dependent observation delay.

Configure the trading process with a relative TLS keylog path and `latency_timestamps=true`. The keylog is created
with mode 0600; it decrypts API credentials and signatures and must be protected. The application also writes
`capture_market_identity`, containing the raw WebSocket SHA256 and global `receive_sequence`.

## Offline Matching

```bash
python3 tools/match_capture.py \
  --pcap capture.pcap --keylog capture-session.keys \
  --log json-session.jsonl --output capture_matches.json
```

The tool invokes tshark for TLS decryption, TCP/TLS reassembly, and PDML parsing. Use `--tls-port 12345` for
additional TLS ports, or `--pdml decrypted.pdml` to reuse decoded input. Market messages are matched by raw-payload
SHA256 to `receive_sequence`; order requests are matched by `newClientOrderId` to `client_id`. `leg=0` is the first leg.

The output records candidate frames, TCP streams, capture times, and match status. Duplicate, retransmitted, or
missing data is marked `missing_or_ambiguous`; the tool never chooses the nearest frame by time alone.

## First-Leg Latency Report

```bash
python3 tools/analyze_capture_latency.py \
  --local-pcap local.pcap --pcap external.pcapng \
  --log json-session.jsonl --keys capture-session.keys \
  --output latency-analysis.json
```

The report is limited to the first leg and ends at complete HTTP request transmission. It includes application
stages, externally observed latency, local-capture latency, and separate unassigned differences. The optional local
capture is A; the external capture is B.

## TCP/TLS Boundary Validation

`--verify-boundaries` validates TLS record lengths and ciphertext against TCP segment bytes and sequence ranges.
It handles out-of-order segments and duplicate retransmissions conservatively. Missing bytes, conflicting overlap,
truncation, timestamp reversal, unsupported WebSocket fragmentation/compression, HTTP bodies, or ambiguous mapping
produce `unverified`. Shared TLS records share the record-completion boundary.

The validated duration is a packet-level capture measurement, not a physical last-bit NIC timestamp. The application
field `exact_message_mapping` remains false; offline validation does not change that capability declaration.

## A/B Observation Overhead

When both `--local-pcap` and `--pcap` are supplied, the script extracts real inbound TCP data segments and their
corresponding pure ACKs from both captures. A sample is retained only when the ACK exactly confirms that segment,
there is no intervening reverse data, and tshark reports no retransmission, out-of-order, or lost-segment flag.
Keep-alive, zero-window probes, delayed or cumulative ACKs, and unmatched frames are excluded by these checks.

Samples are paired by direction, TCP sequence, payload length, and payload hash. For each pair:

```text
delta = (B_ack - B_data) - (A_ack - A_data)
```

The report includes sample counts and P50, P95, P99, minimum, and maximum in microseconds. This estimates the
additional observation path cost using real Binance TCP traffic. It estimates the difference between two observation
paths and includes capture-host, mirror/TAP, driver, and timestamping effects; it is not a direct one-way propagation
measurement, exchange RTT, or business-processing time.
