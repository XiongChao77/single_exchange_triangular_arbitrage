"""Check clock-domain filtering and missing/ambiguous timestamp reporting."""
import json
from pathlib import Path
import sys
import tempfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from summarize_latency import summarize

with tempfile.TemporaryDirectory() as folder:
    path=Path(folder)/'sample.jsonl'
    offset=10_000_000_000
    latency={
        'leg':0,'market_received_ns':1_000_000,'market_received_realtime_ns':offset+1_000_000,
        'market_processed_ns':1_001_000,'edge_found_ns':1_002_000,
        'gateway_enqueued_ns':1_004_000,'worker_begin_ns':1_006_000,
        'transport':{'timepoints':{'http_write_begin_ns':1_008_000,'http_write_begin_realtime_ns':offset+1_008_000,
                                  'http_write_end_ns':1_010_000},
                     'kernel_tx':{'enabled':True,'final_byte_timestamp_present':True,
                                  'kernel_tx_software_realtime_ns':offset+1_012_000,
                                  'kernel_tx_sched_realtime_ns':offset+1_011_000}},
        'kernel_rx':{'batches':[{'kernel_rx_realtime_ns':offset+990_000}], 'exact_message_mapping':False}}
    row={'event':'execution_order_latency','fields':{'client_id':'sample-order','latency':latency}}
    path.write_text(json.dumps(row)+'\n')
    result=summarize(path)
    assert '| user_market_to_http_write_end | 1 | 10.000 |' in result,result
    assert '| user_market_to_tx_software_clock_checked | 1 | 12.000 |' in result,result
    assert '| observed_latest_rx_batch_to_tx_software | 1 | 22.000 |' in result,result
    assert 'not exact per-message' in result
    latency['transport']['timepoints']['http_write_begin_realtime_ns']+=2_000_000
    path.write_text(json.dumps(row)+'\n')
    result=summarize(path)
    assert 'clock_inconsistent_orders=1' in result and '| user_market_to_tx_software' not in result,result
    assert '| user_market_to_http_write_end | 1 | 10.000 |' in result,result
    latency['transport']['kernel_tx']['final_byte_timestamp_present']=False
    latency['transport']['kernel_tx']['kernel_tx_software_realtime_ns']=None
    path.write_text(json.dumps(row)+'\n')
    result=summarize(path)
    assert 'orders_missing_final_tx_software=1' in result,result
print('Latency summary checks passed')
