#!/usr/bin/env python3
"""Read actual ExEx notifications and corroborate them with the same node's RPC."""
import argparse
import json
from pathlib import Path
import socket
import struct
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True)
    parser.add_argument('--rpc', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()

    def rpc(method, *params):
        body = json.dumps(dict(jsonrpc='2.0', id=1, method=method, params=params)).encode()
        request = urllib.request.Request(args.rpc, body, {'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=10) as response:
            result = json.load(response)
        assert 'error' not in result, result.get('error')
        return result['result']

    report = dict(socket=args.socket, rpc=args.rpc, blocks=[], pending_transactions=0,
                  sampled_accounts=0, sampled_storage=0, scope='live producer read-only smoke')
    with socket.socket(socket.AF_UNIX) as stream:
        stream.settimeout(45)
        stream.connect(args.socket)
        hello = json.dumps(dict(type='hello', version=4, resume_from=None, cursor=None, pending=True)).encode()
        stream.sendall(struct.pack('>I', len(hello)) + hello)

        def read(n):
            result = bytearray()
            while len(result) < n:
                part = stream.recv(n-len(result))
                assert part, 'ExEx closed'
                result.extend(part)
            return result

        previous = None
        deadline = time.monotonic() + 55
        while len(report['blocks']) < 3:
            assert time.monotonic() < deadline, 'live block observation timeout'
            length = struct.unpack('>I', read(4))[0]
            assert 0 < length <= 64*1024*1024
            frame = json.loads(read(length))
            if frame['type'] == 'pending':
                report['pending_transactions'] += len(frame['transactions'])
                continue
            assert frame['type'] == 'notification', frame['type']
            cursor = frame['cursor']
            if previous:
                assert cursor['stream'] == previous['stream']
                assert cursor['sequence'] == previous['sequence'] + 1
            previous = cursor
            notification = frame['notification']
            assert notification['kind'] == 'commit', 'retry observation after a live reorg'
            segment = notification['committed']
            for block in segment['blocks']:
                actual = rpc('eth_getBlockByHash', block['hash'], False)
                assert actual['hash'] == block['hash']
                assert int(actual['number'], 16) == block['number']
                assert actual['parentHash'] == block['parent_hash']
                assert actual['transactions'] == block['transactions']
                logs = rpc('eth_getLogs', {'blockHash': block['hash']})
                normalized = [dict(address=l['address'], topics=l['topics'], data=l['data'],
                                   log_index=int(l['logIndex'],16),
                                   transaction_index=int(l['transactionIndex'],16),
                                   transaction_hash=l['transactionHash']) for l in logs]
                assert normalized == block['logs'], 'ExEx logs differ from RPC'
                report['blocks'].append(dict(number=block['number'], hash=block['hash'],
                                             transactions=len(block['transactions']), logs=len(logs)))
            anchor = dict(blockHash=segment['blocks'][-1]['hash'], requireCanonical=True)
            for account in segment['diff']['accounts'][:8]:
                info = account['info']
                if info is not None:
                    assert int(rpc('eth_getBalance', account['address'], anchor),16) == int(info['balance'],16)
                    assert int(rpc('eth_getTransactionCount', account['address'], anchor),16) == info['nonce']
                    report['sampled_accounts'] += 1
                for slot, value in account['storage'][:2]:
                    assert int(rpc('eth_getStorageAt', account['address'], slot, anchor),16) == int(value,16)
                    report['sampled_storage'] += 1
        report['last_cursor'] = previous
    report['status'] = 'passed'
    args.out.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
