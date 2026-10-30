# H2 / L0-v2 — pmxt v2 (13/04–09/08) + ag6 (10/08–15/08) -> livro das MÁXIMAS (catálogo H2) por minuto, formato do L0.
# Linha t (ms UTC, múltiplo de 60 000) = estado do token SIM com todos os eventos de `timestamp` < t. Janela 05–23 h local do dia do evento.
# Topo = best_bid/best_ask das price_change (ou foto book). Tamanho do topo = tamanho do nível naquele preço se já visto (foto ou price_change).
# Cabeçalho injetado: TAG, H0, H1 (UTC inclusivo), NWORK.
import datetime as dt, glob, json, os, subprocess, sys, time
from zoneinfo import ZoneInfo
import gzip
TAG = os.environ['TAG']; H0 = os.environ['H0']; H1 = os.environ['H1']; NWORK = int(os.environ.get('NWORK', '2'))
MODE = os.environ.get('MODE', 'download')   # download = baixa a hora inteira; remote = duckdb lê só colunas/row groups via HTTP range
MEMLIM = os.environ.get('MEMLIM', '2500MB')
def _rm(x):
    if x and not x.startswith('http') and os.path.exists(x): os.remove(x)
WARM = 2; PREF = 2
import duckdb
import numpy as np, pyarrow as pa, pyarrow.parquet as pq
OUT = os.environ.get('OUT', 'out'); TMP = os.environ.get('TMPD', 'tmp')
for d_ in (TMP, f'{OUT}/min', f'{OUT}/neg', f'{OUT}/cov'):
    os.makedirs(d_, exist_ok=True)
CAT = json.load(gzip.open(os.environ['CAT'], 'rt', encoding='utf-8'))
YES = {}; ALLTOK = {}; EVW = []; tok_by_date = {}; yes_by_date = {}
mi = 0
for e in CAT:
    z = ZoneInfo(e['zone']); d = dt.date.fromisoformat(e['date'])
    a = int(dt.datetime(d.year, d.month, d.day, 5, 0, tzinfo=z).timestamp() * 1000)
    b = int(dt.datetime(d.year, d.month, d.day, 23, 0, tzinfo=z).timestamp() * 1000)
    ids = []
    for m in e['markets']:
        ids.append(mi); YES[m['yes']] = mi; ALLTOK[m['yes']] = (mi, 0); ALLTOK[m['no']] = (mi, 1)
        tok_by_date.setdefault(e['date'], []).extend([m['yes'], m['no']]); yes_by_date.setdefault(e['date'], []).append(m['yes']); mi += 1
    EVW.append((a, b, ids))
SCH_MIN = pa.schema([('t', pa.int64()), ('mi', pa.int32()), ('bb', pa.float32()), ('ba', pa.float32()), ('bs', pa.float32()),
                     ('as_', pa.float32()), ('hb', pa.int8()), ('age', pa.int32())])
SCH_NEG = pa.schema([('ts', pa.int64()), ('tr', pa.int64()), ('mi', pa.int32()), ('tok', pa.int8()), ('px', pa.float32()), ('sz', pa.float32()),
                     ('side', pa.int8()), ('fee', pa.int16()), ('tx', pa.string())])
AG6_0 = dt.datetime(2026, 8, 10, 0, tzinfo=dt.timezone.utc)


def url(h):
    s = h.strftime('%Y-%m-%dT%H')
    base = 'third-party/ag6' if h >= AG6_0 else 'pmxt/v2'
    return f'https://dl.pendulumflow.com/{base}/polymarket_orderbook_{s}.parquet', ('ag6' if h >= AG6_0 else 'v2')


def work(arg):
    W, hrs = arg
    LOG = open(f'{OUT}/cov/log_{TAG}w{W}.txt', 'a'); COV = open(f'{OUT}/cov/cov_{TAG}w{W}.jsonl', 'a')

    def P(*a):
        s = ' '.join(str(x) for x in a); print(W, s, flush=True); LOG.write(s + '\n'); LOG.flush()
    c = duckdb.connect()
    c.execute(f"SET threads TO 2; SET memory_limit='{MEMLIM}'; SET TimeZone='UTC'; SET enable_progress_bar=false; "
              f"SET temp_directory='{TMP}/spill{W}'")
    BID = {}; ASK = {}; BB = {}; BA = {}; HB = {}; LAST = {}
    real0 = hrs[0]
    hours = [real0 - dt.timedelta(hours=x) for x in range(WARM, 0, -1)] + hrs
    from concurrent.futures import ThreadPoolExecutor

    def dl(h):
        u, src = url(h); f = f'{TMP}/w{W}_{h.strftime("%Y-%m-%dT%H")}.parquet'
        if MODE == 'remote':
            return u if subprocess.run(['curl', '-sfI', '-A', 'curl/8.5.0', '--retry', '5', u], capture_output=True).returncode == 0 else None
        for att in range(6):
            r = subprocess.run(['curl', '-sfL', '-A', 'curl/8.5.0', '--retry', '5', '--retry-delay', '10', '--max-time', '900', '--speed-limit', '2000000', '--speed-time', '60', '-o', f, u])
            if r.returncode == 0 and os.path.exists(f) and os.path.getsize(f) > 1000:
                return f
            if r.returncode == 22:
                return None
            time.sleep(15 * (att + 1))
        return None
    ex = ThreadPoolExecutor(PREF)
    futs = {k: ex.submit(dl, h) for k, h in enumerate(hours[:PREF])}
    T0 = time.time()
    for k, h in enumerate(hours):
        rec = {'h': h.strftime('%Y-%m-%dT%H'), 'warm': h < real0, 'src': url(h)[1]}
        t = time.time(); u = futs.pop(k).result(); rec['t_dl'] = round(time.time() - t, 1)
        if k + PREF < len(hours):
            futs[k + PREF] = ex.submit(dl, hours[k + PREF])
        if u is None:
            rec['ok'] = False; rec['err'] = 'download'; COV.write(json.dumps(rec) + '\n'); COV.flush(); P('FALHOU download', rec['h']); continue
        rec['mb'] = round(os.path.getsize(u) / 1e6) if not u.startswith('http') else -1
        dset = [(h.date() + dt.timedelta(days=x)).isoformat() for x in (-1, 0, 1)]
        toks = [x for d_ in dset for x in tok_by_date.get(d_, [])]; ytoks = [x for d_ in dset for x in yes_by_date.get(d_, [])]
        hs = int(h.timestamp() * 1000); he = hs + 3600000
        rows = None; t = time.time()
        if toks:
            c.execute("CREATE OR REPLACE TEMP TABLE tk AS SELECT unnest(?::VARCHAR[]) a", [toks])
            c.execute("CREATE OR REPLACE TEMP TABLE ty AS SELECT unnest(?::VARCHAR[]) a", [ytoks])
            for att in range(5):
                try:
                    rows = c.execute(f"""SELECT CASE event_type WHEN 'book' THEN 0 WHEN 'price_change' THEN 1 WHEN 'best_bid_ask' THEN 2
                          WHEN 'last_trade_price' THEN 3 ELSE 9 END et,
                        asset_id aid, epoch_ms(timestamp) ts, epoch_ms(timestamp_received) tr, price::DOUBLE pr, size::DOUBLE sz, side::VARCHAR sd,
                        best_bid::DOUBLE bb, best_ask::DOUBLE ba,
                        CASE WHEN event_type='book' THEN bids END bl, CASE WHEN event_type='book' THEN asks END al,
                        CASE WHEN event_type='last_trade_price' THEN lower(transaction_hash) END tx,
                        CASE WHEN event_type='last_trade_price' THEN fee_rate_bps::INT END fee
                        FROM read_parquet('{u}', file_row_number=true)
                        WHERE (event_type IN ('book','price_change','best_bid_ask') AND asset_id IN (SELECT a FROM ty))
                           OR (event_type = 'last_trade_price' AND asset_id IN (SELECT a FROM tk))
                        ORDER BY timestamp, file_row_number""")
                    break
                except Exception as e:
                    P('erro sql', rec['h'], att, str(e)[:300]); time.sleep(10 * (att + 1))
            if rows is None:
                _rm(u); rec['ok'] = False; rec['err'] = 'sql'; COV.write(json.dumps(rec) + '\n'); COV.flush(); continue
        rec['t_sql'] = round(time.time() - t, 1); t = time.time(); NR = [0]

        def stream():
            if rows is None:
                return
            while True:
                ch = c.fetchmany(200000)
                if not ch:
                    break
                NR[0] += len(ch)
                yield from ch
        ACT = [w for w in EVW if w[0] < he and w[1] > hs] if h >= real0 else []
        acts = sorted({i for w in ACT for i in w[2]})
        M = {k_: [] for k_ in SCH_MIN.names}; N = {k_: [] for k_ in SCH_NEG.names}
        nbook = 0; seen = set(); nxt = hs; lag = []; npc = 0; badq = 0

        def emit(tm):
            for a_, b_, ids in ACT:
                if not (a_ <= tm < b_):
                    continue
                for i in ids:
                    bb = BB.get(i); ba = BA.get(i); bd = BID.get(i); ad = ASK.get(i)
                    bs = bd.get(int(round(bb * 10000))) if (bb is not None and bd is not None) else None
                    as_ = ad.get(int(round(ba * 10000))) if (ba is not None and ad is not None) else None
                    M['t'].append(tm); M['mi'].append(i); M['bb'].append(bb); M['ba'].append(ba); M['bs'].append(bs); M['as_'].append(as_)
                    M['hb'].append(1 if HB.get(i) else 0); lt = LAST.get(i); M['age'].append(None if lt is None else int((tm - lt) // 1000))
        for et, aid, ts, tr, pr, sz, sd, bb, ba, bl, al, tx, fee in stream():
            while ts >= nxt and nxt < he:
                if ACT: emit(nxt)
                nxt += 60000
            if et == 3:
                ii = ALLTOK.get(aid)
                if ii is not None and pr is not None and sz:
                    N['ts'].append(ts); N['tr'].append(tr); N['mi'].append(ii[0]); N['tok'].append(ii[1]); N['px'].append(pr); N['sz'].append(sz)
                    N['side'].append(1 if sd == 'BUY' else -1 if sd == 'SELL' else 0); N['fee'].append(fee); N['tx'].append(tx)
                continue
            i = YES.get(aid)
            if i is None:
                continue
            seen.add(i); LAST[i] = ts
            if et == 0:
                nbook += 1
                try:
                    BID[i] = {int(round(float(p) * 10000)): float(s) for p, s in json.loads(bl or '[]') if float(s) > 0}
                    ASK[i] = {int(round(float(p) * 10000)): float(s) for p, s in json.loads(al or '[]') if float(s) > 0}
                except Exception:
                    continue
                HB[i] = True
                BB[i] = max(BID[i]) / 1e4 if BID[i] else None; BA[i] = min(ASK[i]) / 1e4 if ASK[i] else None
            elif et == 1:
                npc += 1
                if tr is not None and npc % 37 == 0:
                    lag.append(tr - ts)
                if pr is not None and sz is not None and sd in ('BUY', 'SELL'):
                    d_ = BID.setdefault(i, {}) if sd == 'BUY' else ASK.setdefault(i, {}); p = int(round(pr * 10000))
                    if sz == 0: d_.pop(p, None)
                    else: d_[p] = sz
                if bb is not None: BB[i] = bb if bb > 0 else None
                if ba is not None: BA[i] = ba if 0 < ba < 1.0001 else None
                if BB.get(i) is not None and BA.get(i) is not None and BB[i] >= BA[i]:
                    badq += 1
            else:
                BB[i] = bb if bb is not None and bb > 0 else None; BA[i] = ba if ba is not None and 0 < ba < 1.0001 else None
        while nxt < he:
            if ACT: emit(nxt)
            nxt += 60000
        _rm(u); rec['rows'] = NR[0]
        if M['t']:
            pq.write_table(pa.table([pa.array(M[n_], SCH_MIN.field(n_).type) for n_ in SCH_MIN.names], schema=SCH_MIN),
                           f"{OUT}/min/min_{rec['h']}.parquet", compression='zstd', compression_level=6)
        if N['ts']:
            pq.write_table(pa.table([pa.array(N[n_], SCH_NEG.field(n_).type) for n_ in SCH_NEG.names], schema=SCH_NEG),
                           f"{OUT}/neg/neg_{rec['h']}.parquet", compression='zstd')
        lg = np.array(lag) if lag else np.array([np.nan])
        rec.update(ok=True, t_py=round(time.time() - t, 1), nbook=nbook, npc=npc, badq=badq, seen=len(seen), act=len(acts),
                   act_seen=sum(1 for i in acts if i in LAST), act_quote=sum(1 for i in acts if BB.get(i) is not None or BA.get(i) is not None),
                   nmin=len(M['t']), nneg=len(N['ts']), lag50=float(np.nanmedian(lg)), lag99=float(np.nanpercentile(lg, 99)))
        COV.write(json.dumps(rec) + '\n'); COV.flush()
        P(json.dumps(rec), f'tot {time.time()-T0:.0f}s')
    P('FIM', round(time.time() - T0))
    return W


if __name__ == '__main__':
    from multiprocessing import Pool
    a = dt.datetime.fromisoformat(H0).replace(tzinfo=dt.timezone.utc); b = dt.datetime.fromisoformat(H1).replace(tzinfo=dt.timezone.utc)
    HS = []
    while a <= b:
        HS.append(a); a += dt.timedelta(hours=1)
    sz = (len(HS) + NWORK - 1) // NWORK
    ch = [(k, HS[k * sz:(k + 1) * sz]) for k in range(NWORK) if HS[k * sz:(k + 1) * sz]]
    with Pool(len(ch)) as pool:
        print(pool.map(work, ch), flush=True)
    print('FIM GERAL', flush=True)
