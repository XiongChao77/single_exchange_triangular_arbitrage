import json
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'tools'))
from analyze_capture_latency import analyze, render
from match_capture import match


class AnalysisTest(unittest.TestCase):
    def test_precision_missing_ambiguity_and_later_leg(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory)/'log.jsonl'
            latency = dict(leg=0, market_received_ns=1000, cycle_started_ns=2000,
                order_prepared_ns=9000, previous_report_ns=8000,
                transport={'timepoints': {'http_write_end_ns': 10000}})
            log.write_text(json.dumps({'event': 'execution_order_latency', 'fields': {
                'client_id': 'a', 'latency': latency}})+'\n')
            row = dict(client_id='a', match_status='unique', market_candidates=[{
                'presentation_time_epoch': '1789315864.955817300'}], order_candidates=[{
                'presentation_time_epoch': '1789315864.957354200'}])
            matches = dict(orders=[row], decoded_market_payloads=1, decoded_order_requests=1)
            result = analyze(log, matches)['orders'][0]
            self.assertEqual(result['presentation_gap_us'], 1536.9)
            self.assertEqual(result['durations_us']['user_trigger_to_write_end'], 9)
            self.assertNotIn('response_wait', result['durations_us'])
            self.assertNotIn('previous_report_to_write_end', result['durations_us'])
            self.assertIsNone(result['durations_us']['sign'])
            self.assertEqual(result['boundary_validation'], 'not_automatically_verified')
            row['match_status'] = 'missing_or_ambiguous'
            self.assertIsNone(analyze(log, matches)['orders'][0]['presentation_gap_us'])

    def test_submission_without_response_and_ignore_later_legs(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory)/'log.jsonl'
            events, matches = [], []
            for leg in range(3):
                client = str(leg)
                latency = dict(leg=leg, market_received_ns=1000,
                    transport={'timepoints': {'http_write_end_ns': 10000}})
                events.append({'event': 'execution_order_write', 'fields': {
                    'client_id': client, 'latency': latency}})
                matches.append(dict(client_id=client, leg=leg, match_status='missing_or_ambiguous',
                    market_candidates=[], order_candidates=[]))
            log.write_text('\n'.join(map(json.dumps, events)))
            pdml = Path(directory)/'capture.pdml'
            pdml.write_text('<pdml/>')
            matched = match(pdml, log, first_leg_only=True)
            self.assertEqual([r['client_id'] for r in matched['orders']], ['0'])
            result = analyze(log, dict(orders=matches, decoded_market_payloads=1, decoded_order_requests=3))
            self.assertEqual([r['client_id'] for r in result['orders']], ['0'])
            self.assertEqual(result['first_leg_samples'], 1)
            self.assertEqual(result['orders'][0]['durations_us']['user_trigger_to_write_end'], 9)
            report = render(result, log)
            self.assertNotIn('response_wait', report)
            self.assertNotIn('previous_report', report)


if __name__ == '__main__':
    unittest.main()
