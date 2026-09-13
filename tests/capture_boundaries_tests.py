import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'tools'))
from capture_boundaries import coverage, BoundaryIndex, validate_matches


def packet(n, seq, data, time=None, truncated=False):
    return dict(frame=n, seq=seq, data=data, time=time or str(n), truncated=truncated)


def field(name, show=None, **attrs):
    if show is not None:
        attrs['show'] = str(show)
    return '<field name="'+name+'" '+ ' '.join(f'{k}="{v}"' for k,v in attrs.items())+'/>'


def pdml_packet(n, seq, data, record=False, segments=(), source='server', app='market'):
    out = '<packet>'+field('frame.number', n)+field('frame.time_epoch', f'1.{n:09d}')
    for name, show in [('tcp.stream', 1), ('tcp.seq', seq), ('tcp.srcport', 443),
                       ('tcp.dstport', 50000), ('ip.src', source), ('ip.dst', 'client'),
                       ('frame.cap_len', len(data)+66), ('frame.len', len(data)+66)]:
        out += field(name, show)
    out += field('tcp.payload', value=data.hex(), pos=66, size=len(data))
    if record:
        ciphertext = b'\x17\x03\x03\x00\x04abcd'
        if segments:
            out += field('tcp.reassembled.data', value=ciphertext.hex())
            for frame, offset, fragment in segments:
                out += field('tcp.segment', frame, pos=offset, size=len(fragment), value=fragment.hex())
        out += '<field name="tls.record" pos="'+('0' if segments else '66')+'">'
        out += field('tls.record.length', 4)+field('tls.app_data', value='61626364')+'</field>'
        if app == 'market':
            out += '<proto name="websocket">'+field('websocket.fin', 'True')+field('websocket.opcode', 1)
            out += field('websocket.payload.text', value='7b7d')+'</proto>'
        else:
            out += field('http.request.method', 'POST')+field('http.request.version', 'HTTP/1.1')
            if app == 'body':
                out += field('http.content_length', 10)
    return out+'</packet>'


class CoverageTest(unittest.TestCase):
    def test_out_of_order_first_coverage_and_duplicates(self):
        result = coverage([packet(1, 103, b'def'), packet(2, 103, b'def'),
                           packet(3, 100, b'abc'), packet(4, 100, b'abcdef')], 100, b'abcdef')
        self.assertEqual(result['completion_frame'], 3)
        self.assertEqual([p['frame'] for p in result['contributing_frames']], [1, 3])

    def test_missing_conflicting_truncated_and_clock(self):
        cases = [([packet(1, 100, b'ab')], 'missing_tcp_bytes'),
                 ([packet(1, 100, b'abc'), packet(2, 101, b'x')], 'conflicting_tcp_overlap'),
                 ([packet(1, 100, b'abc', truncated=True)], 'truncated_capture_frame'),
                 ([packet(1, 100, b'a', '2'), packet(2, 101, b'bc', '1')], 'non_monotonic')]
        for packets, reason in cases:
            with self.subTest(reason=reason), self.assertRaisesRegex(ValueError, reason):
                coverage(packets, 100, b'abc')


class RecordTest(unittest.TestCase):
    def verify(self, xml, app='market'):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'capture.pdml'
            path.write_text('<pdml>'+xml+'</pdml>')
            return BoundaryIndex(path, ['2']).verify({'presentation_frame': '2'}, app)

    def test_split_record_provenance(self):
        data = b'\x17\x03\x03\x00\x04abcd'
        xml = pdml_packet(1, 100, b'prefix'+data[:6])
        xml += pdml_packet(2, 112, data[6:], True, [(1, 0, data[:6]), (2, 6, data[6:])])
        result = self.verify(xml)
        self.assertEqual(result['status'], 'verified')
        self.assertEqual(result['record']['tcp_seq_start'], 106)
        self.assertEqual(result['completion_frame'], 2)
        self.assertEqual(len(result['record']['contributing_frames']), 2)

    def test_wrong_direction_or_missing_segment(self):
        data = b'\x17\x03\x03\x00\x04abcd'
        tail = pdml_packet(2, 106, data[6:], True, [(1, 0, data[:6]), (2, 6, data[6:])])
        for first in ['', pdml_packet(1, 100, data[:6], source='other')]:
            self.assertEqual(self.verify(first+tail)['status'], 'unverified')

    def test_single_packet_order_and_unsupported_body(self):
        data = b'\x17\x03\x03\x00\x04abcd'
        self.assertEqual(self.verify(pdml_packet(2, 100, data, True, app='order'), 'order')['status'], 'verified')
        self.assertEqual(self.verify(pdml_packet(2, 100, data, True, app='body'), 'order')['status'], 'unverified')

    def test_fragment_and_multiple_records_fail_closed(self):
        data = b'\x17\x03\x03\x00\x04abcd'
        xml = pdml_packet(2, 100, data, True)
        self.assertEqual(self.verify(xml.replace('show="True"', 'show="False"'))['status'], 'unverified')
        self.assertEqual(self.verify(xml.replace('</packet>', '<field name="tls.record"/></packet>'))['status'], 'unverified')

    def test_ambiguous_match_no_verified_latency(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'capture.pdml'; path.write_text('<pdml/>')
            matches = {'orders': [{'market_candidates': [], 'order_candidates': [], 'match_status': 'missing_or_ambiguous'}]}
            validate_matches(path, matches)
            self.assertIsNone(matches['orders'][0]['verified_capture_latency_us'])


if __name__ == '__main__':
    unittest.main()
