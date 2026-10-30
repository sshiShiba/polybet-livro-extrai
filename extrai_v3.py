# L0 — PendulumFlow v3 -> livro das MÁXIMAS (catálogo G3) por minuto: topo + níveis 2–3 dos dois lados (token SIM),
# negócios (ms, os dois tokens, com hash) e resoluções. Janela por evento: 05:00–23:00 hora local do dia do evento.
# Linha de minuto t (ms UTC, múltiplo de 60 000) = estado do livro com TODOS os eventos de timestamp < t (sem olhar à frente).
# Cabeçalho injetado pelo gerador: TAG, H0, H1 (UTC inclusivo), NWORK.
import datetime as dt, glob, heapq, json, os, subprocess, sys, time, traceback
from zoneinfo import ZoneInfo
import gzip
TAG = os.environ['TAG']; H0 = os.environ['H0']; H1 = os.environ['H1']; NWORK = int(os.environ.get('NWORK', '2'))
MODE = os.environ.get('MODE', 'download')   # download = baixa a hora inteira; remote = duckdb lê só colunas/row groups via HTTP range
MEMLIM = os.environ.get('MEMLIM', '2500MB')
def _rm(x):
    if x and not x.startswith('http') and os.path.exists(x): os.remove(x)
WARM = 2; PREF = 3
import duckdb
import numpy as np, pyarrow as pa, pyarrow.parquet as pq
OUT = os.environ.get('OUT', 'out'); TMP = os.environ.get('TMPD', 'tmp')
os.makedirs(TMP, exist_ok=True); os.makedirs(f'{OUT}/min', exist_ok=True); os.makedirs(f'{OUT}/neg', exist_ok=True)
os.makedirs(f'{OUT}/cov', exist_ok=True)
CAT = json.load(gzip.open(os.environ['CAT'], 'rt', encoding='utf-8'))
MK = []; YES = {}; ALLTOK = {}; EVW = []; by_date = {}
for e in CAT:
    z = ZoneInfo(e['zone']); d = dt.date.fromisoformat(e['date'])
    a = int(dt.datetime(d.year, d.month, d.day, 5, 0, tzinfo=z).timestamp() * 1000)
    b = int(dt.datetime(d.year, d.month, d.day, 23, 0, tzinfo=z).timestamp() * 1000)
    ids = []
    for m in e['markets']:
        i = len(MK); MK.append(m); ids.append(i)
        y = format(int(m['yes']), '064X'); n = format(int(m['no']), '064X')
        YES[y] = i; ALLTOK[y] = (i, 0); ALLTOK[n] = (i, 1)
    EVW.append((a, b, ids))
    by_date.setdefault(e['date'], []).extend(m['cid'] for m in e['markets'])
SCH_MIN = pa.schema([('t', pa.int64()), ('mi', pa.int32()), ('bb', pa.float32()), ('ba', pa.float32()), ('bs', pa.float32()),
                     ('as_', pa.float32()), ('bp2', pa.float32()), ('bs2', pa.float32()), ('bp3', pa.float32()), ('bs3', pa.float32()),
                     ('ap2', pa.float32()), ('as2', pa.float32()), ('ap3', pa.float32()), ('as3', pa.float32()),
                     ('hb', pa.int8()), ('age', pa.int32())])
SCH_NEG = pa.schema([('ts', pa.int64()), ('mi', pa.int32()), ('tok', pa.int8()), ('px', pa.float32()), ('sz', pa.float32()),
                     ('side', pa.int8()), ('fee', pa.int16()), ('tx', pa.string())])


def work(arg):
    W, hrs = arg
    LOG = open(f'{OUT}/cov/log_{TAG}w{W}.txt', 'a'); COV = open(f'{OUT}/cov/cov_{TAG}w{W}.jsonl', 'a')

    def P(*a):
        s = ' '.join(str(x) for x in a); print(W, s, flush=True); LOG.write(s + '\n'); LOG.flush()
    c = duckdb.connect()
    c.execute(f"SET threads TO 2; SET memory_limit='{MEMLIM}'; SET TimeZone='UTC'; SET enable_progress_bar=false; "
              f"SET temp_directory='{TMP}/spill{W}'")
    BID = {}; ASK = {}; BB = {}; BA = {}; HB = {}; LAST = {}   # por índice de mercado (token SIM)
    RES = []
    real0 = hrs[0]
    hours = [real0 - dt.timedelta(hours=x) for x in range(WARM, 0, -1)] + hrs
    from concurrent.futures import ThreadPoolExecutor

    def dl(h):
        ds = h.strftime('%Y-%m-%d'); hh = h.strftime('%H'); f = f'{TMP}/w{W}_{ds}T{hh}.parquet'
        if MODE == 'remote':
            u_ = f'https://dl.pendulumflow.com/v3/{ds}/{hh}/{ds}T{hh}.parquet'
            return u_ if subprocess.run(['curl', '-sfI', '--retry', '5', u_], capture_output=True).returncode == 0 else None
        for att in range(6):
            r = subprocess.run(['curl', '-sfL', '--retry', '5', '--retry-delay', '10', '--max-time', '900', '--speed-limit', '2000000', '--speed-time', '60', '-o', f,
                                f'https://dl.pendulumflow.com/v3/{ds}/{hh}/{ds}T{hh}.parquet'])
            if r.returncode == 0 and os.path.exists(f) and os.path.getsize(f) > 1000:
                return f
            if r.returncode == 22:   # 404 = hora não existe
                return None
            time.sleep(15 * (att + 1))
        return None
    ex = ThreadPoolExecutor(PREF)
    futs = {k: ex.submit(dl, h) for k, h in enumerate(hours[:PREF])}
    T0 = time.time()
    for k, h in enumerate(hours):
        rec = {'h': h.strftime('%Y-%m-%dT%H'), 'warm': h < real0}
        ds = h.strftime('%Y-%m-%d')
        t = time.time(); u = futs.pop(k).result(); rec['t_dl'] = round(time.time() - t, 1)
        if k + PREF < len(hours):
            futs[k + PREF] = ex.submit(dl, hours[k + PREF])
        if u is None:
            rec['ok'] = False; rec['err'] = 'download'; COV.write(json.dumps(rec) + '\n'); COV.flush(); P('FALHOU download', rec['h']); continue
        rec['mb'] = round(os.path.getsize(u) / 1e6) if not u.startswith('http') else -1
        dset = [(h.date() + dt.timedelta(days=x)).isoformat() for x in (-1, 0, 1, 2)]
        cids = [x for d_ in dset for x in by_date.get(d_, [])]
        hs = int(h.timestamp() * 1000); he = hs + 3600000
        lst = ','.join(f"from_hex('{x[2:]}')" for x in cids)
        rows = None; t = time.time()
        for att in range(5):
            try:
                rows = c.execute(f"""SELECT CASE event_type WHEN 'book' THEN 0 WHEN 'price_change' THEN 1 WHEN 'best_bid_ask' THEN 2
                      WHEN 'last_trade_price' THEN 3 WHEN 'market_resolved' THEN 4 ELSE 9 END et,
                    hex(asset_id) aid, hex(market) mk, epoch_ms(timestamp) ts, price::DOUBLE pr, size::DOUBLE sz, side::VARCHAR sd,
                    best_bid::DOUBLE bb, best_ask::DOUBLE ba,
                    CASE WHEN event_type='book' THEN list_transform(bids, lambda z: [z.price::DOUBLE, z.size::DOUBLE]) END bl,
                    CASE WHEN event_type='book' THEN list_transform(asks, lambda z: [z.price::DOUBLE, z.size::DOUBLE]) END al,
                    CASE WHEN event_type='market_resolved' THEN hex(winning_asset_id) END win,
                    CASE WHEN event_type='last_trade_price' THEN lower(hex(transaction_hash)) END tx,
                    CASE WHEN event_type='last_trade_price' THEN fee_rate_bps::INT END fee
                    FROM read_parquet('{u}', union_by_name=true)
                    WHERE market IN ({lst}) AND event_type IN ('book','price_change','best_bid_ask','last_trade_price','market_resolved')
                    ORDER BY timestamp, sequence""")
                break
            except Exception as e:
                P('erro sql', rec['h'], att, str(e)[:300]); time.sleep(10 * (att + 1))
        if rows is None:
            _rm(u); rec['ok'] = False; rec['err'] = 'sql'; COV.write(json.dumps(rec) + '\n'); COV.flush(); continue
        rec['t_sql'] = round(time.time() - t, 1); t = time.time(); NR = [0]

        def stream():
            while True:
                ch = c.fetchmany(200000)
                if not ch:
                    break
                NR[0] += len(ch)
                yield from ch
        ACT = [w for w in EVW if w[0] < he and w[1] > hs] if h >= real0 else []
        acts = sorted({i for w in ACT for i in w[2]})
        M = {k_: [] for k_ in SCH_MIN.names}; N = {k_: [] for k_ in SCH_NEG.names}
        nbook = 0; seen = set(); nxt = hs

        def emit(tm):
            for a_, b_, ids in ACT:
                if not (a_ <= tm < b_):
                    continue
                for i in ids:
                    bb = BB.get(i); ba = BA.get(i); bd = BID.get(i); ad = ASK.get(i); hb = 1 if HB.get(i) else 0
                    if hb:
                        bk = heapq.nlargest(3, bd) if bd else []; ak = heapq.nsmallest(3, ad) if ad else []
                        bs = bd.get(int(round(bb * 10000)), 0.0) if bb is not None else None
                        as_ = ad.get(int(round(ba * 10000)), 0.0) if ba is not None else None
                    else:
                        bk = ak = []; bs = as_ = None
                    M['t'].append(tm); M['mi'].append(i); M['bb'].append(bb); M['ba'].append(ba); M['bs'].append(bs); M['as_'].append(as_)
                    for nm, lv, src in (('bp2', 1, bk), ('bp3', 2, bk), ('ap2', 1, ak), ('ap3', 2, ak)):
                        M[nm].append(src[lv] / 1e4 if len(src) > lv else None)
                    M['bs2'].append(bd[bk[1]] if len(bk) > 1 else None); M['bs3'].append(bd[bk[2]] if len(bk) > 2 else None)
                    M['as2'].append(ad[ak[1]] if len(ak) > 1 else None); M['as3'].append(ad[ak[2]] if len(ak) > 2 else None)
                    M['hb'].append(hb); lt = LAST.get(i); M['age'].append(None if lt is None else int((tm - lt) // 1000))
        for et, aid, mk, ts, pr, sz, sd, bb, ba, bl, al, win, tx, fee in stream():
            while ts >= nxt and nxt < he:
                if ACT: emit(nxt)
                nxt += 60000
            if et == 4:
                RES.append((mk, win, ts)); continue
            if et == 3:
                ii = ALLTOK.get(aid)
                if ii is not None and pr is not None and sz:
                    N['ts'].append(ts); N['mi'].append(ii[0]); N['tok'].append(ii[1]); N['px'].append(pr); N['sz'].append(sz)
                    N['side'].append(1 if sd == 'BUY' else -1 if sd == 'SELL' else 0); N['fee'].append(fee); N['tx'].append(tx)
                continue
            i = YES.get(aid)
            if i is None:
                continue
            seen.add(i); LAST[i] = ts
            if et == 0:
                nbook += 1
                BID[i] = {int(round(p * 10000)): s for p, s in (bl or []) if s and s > 0}
                ASK[i] = {int(round(p * 10000)): s for p, s in (al or []) if s and s > 0}
                HB[i] = True
                BB[i] = max(BID[i]) / 1e4 if BID[i] else None; BA[i] = min(ASK[i]) / 1e4 if ASK[i] else None
            elif et == 1:
                if pr is not None and sz is not None and HB.get(i):
                    d_ = BID[i] if sd == 'BUY' else ASK[i]; p = int(round(pr * 10000))
                    if sz == 0: d_.pop(p, None)
                    else: d_[p] = sz
                if bb is not None: BB[i] = bb if bb > 0 else None
                if ba is not None: BA[i] = ba if 0 < ba < 1.0001 else None
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
        rec.update(ok=True, t_py=round(time.time() - t, 1), nbook=nbook, seen=len(seen), act=len(acts),
                   act_hb=sum(1 for i in acts if HB.get(i)), act_quote=sum(1 for i in acts if BB.get(i) is not None or BA.get(i) is not None),
                   nmin=len(M['t']), nneg=len(N['ts']))
        COV.write(json.dumps(rec) + '\n'); COV.flush()
        P(json.dumps(rec), f'tot {time.time()-T0:.0f}s')
        json.dump(RES, open(f'{OUT}/cov/res_{TAG}w{W}.json', 'w'))
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
