"""Gera a lista de blocos (matrix) para o workflow: python plano.py v2 2026-04-13T19 2026-08-15T09 72 [prefixo]"""
import datetime as dt, json, sys
src, h0, h1, bl = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]); pre = sys.argv[5] if len(sys.argv) > 5 else src
a = dt.datetime.fromisoformat(h0); b = dt.datetime.fromisoformat(h1); out = []; k = 0
while a <= b:
    e = min(a + dt.timedelta(hours=bl - 1), b)
    out.append({'tag': f'{pre}b{k:03d}', 'h0': a.strftime('%Y-%m-%dT%H:00:00'), 'h1': e.strftime('%Y-%m-%dT%H:00:00')})
    a = e + dt.timedelta(hours=1); k += 1
print(json.dumps(out, separators=(',', ':')))
