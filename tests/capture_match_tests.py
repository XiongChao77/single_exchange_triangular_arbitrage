import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('capture', Path(__file__).resolve().parents[1]/'tools/match_capture.py')
capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture)

class MatchingTest(unittest.TestCase):
    def test_exact_payload_and_client_id_with_ambiguity(self):
        payload = b'{"stream":"btcusdt@depth20@100ms","data":{"lastUpdateId":7}}'
        packet = '<packet><field name="frame.number" show="4"/><field name="frame.time_epoch" show="1.000001"/><field name="tcp.stream" show="0"/><field name="websocket.payload.text" value="'+payload.hex()+'"/></packet>'
        order = '<packet><field name="frame.number" show="9"/><field name="http.request.method" show="POST"/><field name="http.request.uri" show="/api/v3/order?newClientOrderId=abc&amp;symbol=BTCUSDT&amp;signature=SECRET"/></packet>'
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); pdml=root/'capture.pdml'; log=root/'log.jsonl'
            log.write_text('\n'.join(json.dumps(row) for row in [
                {'event':'capture_market_identity','fields':{'receive_sequence':8,'payload_sha256':hashlib.sha256(payload).hexdigest()}},
                {'event':'execution_order_latency','fields':{'client_id':'abc','latency':{'leg':0,'trigger_receive_sequence':8}}}]))
            pdml.write_text('<pdml>'+packet+order+'</pdml>')
            result=capture.match(pdml,log)
            self.assertEqual(result['orders'][0]['match_status'],'unique')
            self.assertFalse(result['orders'][0]['exact_wire_latency_available'])
            self.assertNotIn('SECRET',json.dumps(result))
            pdml.write_text('<pdml>'+packet+packet+order+'</pdml>')
            self.assertEqual(capture.match(pdml,log)['orders'][0]['match_status'],'missing_or_ambiguous')

if __name__=='__main__': unittest.main()
