#!/usr/bin/env python3
"""Reproduce the reported LaminaDB space observations and run diagnostic probes.

Phases:
  A  Reproduce: 31 commits -> 37 versions on 7 keys; vacuum -> 7; checkpoint;
     journal 7350 -> 7413; restart -> versions=7, journal_records=33.
  B  Test the "move the old journal away" suggestion (on copies).
  C  Supported compaction: export /changes, backup, restore into a new dir,
     verify continuity semantics for offline consumers.
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error

BIN = "/tmp/lamina-diagnosis/laminad"
ROOT = "/tmp/lamina-diagnosis"
PORT_A = 7411
PORT_B = 7412


def http(method, port, path, body=None, raw=False):
    url = f"http://127.0.0.1:{port}/v1/lamina{path}"
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            payload = resp.read()
            return resp.status, (payload if raw else json.loads(payload))
    except urllib.error.HTTPError as e:
        payload = e.read()
        if raw:
            return e.code, payload
        return e.code, json.loads(payload)


class Instance:
    def __init__(self, name, port, data_dir, restore=None):
        self.name = name
        self.port = port
        self.dir = data_dir
        cmd = [BIN, "-data", data_dir, "-listen", f"127.0.0.1:{port}"]
        if restore:
            cmd += ["-restore", restore]
        self.proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.log = os.path.join(ROOT, f"{name}.log")

    def wait_ready(self, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                out = self.proc.stdout.read() if self.proc.stdout else ""
                raise RuntimeError(f"{self.name} exited early: {out}")
            try:
                status, body = http("GET", self.port, "/health")
                if status == 200:
                    return body
            except Exception:
                pass
            time.sleep(0.05)
        raise RuntimeError(f"{self.name} not ready")

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            self.proc.wait(timeout=10)
        out = self.proc.stdout.read() if self.proc.stdout else ""
        with open(self.log, "a") as f:
            f.write(out)
        return self.proc.returncode


def dir_sizes(path):
    out = {}
    if os.path.isdir(path):
        for name in sorted(os.listdir(path)):
            p = os.path.join(path, name)
            if os.path.isfile(p):
                out[name] = os.path.getsize(p)
    out["__total__"] = sum(v for k, v in out.items() if not k.startswith("__"))
    return out


def journal_size(path):
    return os.path.getsize(os.path.join(path, "journal.lmw"))


def txn(port):
    _, tx = http("POST", port, "/transactions", {})
    return tx["id"]


def commit_rows(port, rows):
    tx = txn(port)
    for row in rows:
        s, b = http("POST", port, f"/transactions/{tx}/put",
                    {"relation": "spectra", "row": row})
        assert s == 200, b
    s, b = http("POST", port, f"/transactions/{tx}/commit")
    assert s == 200, b
    return b["sequence"]


def run_workload(port, band_len, long_band=None):
    """7 inserts + 30 updates in 31 commits -> 37 versions on 7 keys.

    long_band: optional (commit_index, row_key) -> extra band length for
    size calibration.
    """
    http("POST", port, "/relations", {
        "name": "spectra", "primary": "sample",
        "columns": [
            {"name": "sample", "type": "int"},
            {"name": "band", "type": "text"},
            {"name": "amplitude", "type": "int", "nullable": True},
        ]})
    seqs = []
    # commits 1..7: insert keys 1..7 (seq 2..8)
    for k in range(1, 8):
        seqs.append(commit_rows(port, [
            {"sample": k, "band": "b" * band_len, "amplitude": 10 + k}]))
    # commit 8: single update (seq 9)
    seqs.append(commit_rows(port, [
        {"sample": 1, "band": "b" * band_len, "amplitude": 20}]))
    # commits 9..31 (seq 10..32): 17 single updates + 6 double updates = 29
    key_cycle = [2, 3, 4, 5, 6, 7, 1]
    ci = 0
    for i in range(17):
        k = key_cycle[ci % 7]; ci += 1
        seqs.append(commit_rows(port, [
            {"sample": k, "band": "b" * band_len, "amplitude": 30 + i}]))
    for i in range(6):
        k1 = key_cycle[ci % 7]; ci += 1
        k2 = key_cycle[ci % 7]; ci += 1
        rows = [
            {"sample": k1, "band": "b" * band_len, "amplitude": 50 + i},
            {"sample": k2, "band": "b" * band_len, "amplitude": 60 + i},
        ]
        if long_band is not None and i == long_band[0]:
            rows[long_band[1]]["band"] = "b" * (band_len + long_band[2])
        seqs.append(commit_rows(port, rows))
    return seqs


def phase_a(band_len, long_band, keep_dir=None):
    print("=" * 72)
    print("PHASE A: reproduce the reported observations")
    print("=" * 72)
    data = os.path.join(ROOT, "data-a")
    shutil.rmtree(data, ignore_errors=True)
    inst = Instance("A1", PORT_A, data)
    inst.wait_ready()
    seqs = run_workload(PORT_A, band_len, long_band)
    assert seqs[-1] == 32, seqs
    _, st = http("GET", PORT_A, "/stats")
    j0 = journal_size(data)
    print(f"[A1] after 31 commits: sequence={st['sequence']} "
          f"versions={st['versions']} keys={st['keys']} "
          f"journal_records={st['journal_records']} "
          f"journal_bytes={j0}")
    # vacuum with no active transactions, horizon = head
    s, vac = http("POST", PORT_A, "/vacuum", {"horizon": st["sequence"]})
    print(f"[A2] vacuum -> {vac}")
    _, st = http("GET", PORT_A, "/stats")
    print(f"[A3] after vacuum: versions={st['versions']} "
          f"journal_records={st['journal_records']} horizon={st['horizon']}")
    j1 = journal_size(data)
    s, ck = http("POST", PORT_A, "/checkpoint")
    j2 = journal_size(data)
    print(f"[A4] checkpoint -> {ck}; journal_bytes {j0} -> {j1} (vacuum) "
          f"-> {j2} (checkpoint); delta={j2 - j0}")
    print(f"[A5] dir after checkpoint: {dir_sizes(data)}")
    # change-feed cross-check: history still complete
    s, ch = http("GET", PORT_A, "/changes?after=0&limit=1000")
    kinds = {}
    for r in ch["records"]:
        kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    print(f"[A6] changes after=0: records={len(ch['records'])} "
          f"kinds={kinds} history_base={ch['history_base']} head={ch['head']}")
    # snapshot reads across the horizon
    s_old, b_old = http("POST", PORT_A, "/query",
                        {"relation": "spectra", "at": 31})
    s_at, b_at = http("POST", PORT_A, "/query",
                      {"relation": "spectra", "at": 32})
    print(f"[A7] query at=31 -> {s_old} {b_old.get('error', {}).get('code')}; "
          f"at=32 -> {s_at} rows={len(b_at.get('rows', []))}")
    inst.stop()
    # restart on the same directory
    inst2 = Instance("A2", PORT_A, data)
    inst2.wait_ready()
    _, st = http("GET", PORT_A, "/stats")
    print(f"[A8] after restart: versions={st['versions']} "
          f"journal_records={st['journal_records']} "
          f"history_base={st['history_base']} "
          f"recovered_tail_bytes={st['recovered_tail_bytes']} "
          f"horizon={st['horizon']} sequence={st['sequence']}")
    s_old, b_old = http("POST", PORT_A, "/query",
                        {"relation": "spectra", "at": 31})
    s_at, b_at = http("POST", PORT_A, "/query",
                      {"relation": "spectra", "at": 32})
    print(f"[A9] post-restart query at=31 -> {s_old}; at=32 -> {s_at} "
          f"rows={len(b_at.get('rows', []))}")
    s, ch = http("GET", PORT_A, "/changes?after=0&limit=1000")
    print(f"[A10] post-restart changes: records={len(ch['records'])} "
          f"history_base={ch['history_base']}")
    print(f"[A11] dir after restart: {dir_sizes(data)}")
    inst2.stop()
    if keep_dir:
        shutil.rmtree(keep_dir, ignore_errors=True)
        shutil.copytree(data, keep_dir)
    return j0, j2


def phase_b():
    print("=" * 72)
    print("PHASE B: test the 'move the old journal away before start' suggestion")
    print("=" * 72)
    src = os.path.join(ROOT, "data-a-keep")
    # B1: checkpoint present, journal moved away
    d1 = os.path.join(ROOT, "data-b1")
    shutil.rmtree(d1, ignore_errors=True)
    shutil.copytree(src, d1)
    os.rename(os.path.join(d1, "journal.lmw"),
              os.path.join(d1, "journal.lmw.aside"))
    p = subprocess.run([BIN, "-data", d1, "-listen", f"127.0.0.1:{PORT_B}"],
                       capture_output=True, text=True, timeout=15)
    print(f"[B1] journal moved aside, checkpoint kept: exit={p.returncode}")
    print(f"     stderr: {p.stderr.strip() or p.stdout.strip()}")
    # B2: both checkpoint and journal moved away (fresh-looking dir)
    d2 = os.path.join(ROOT, "data-b2")
    shutil.rmtree(d2, ignore_errors=True)
    shutil.copytree(src, d2)
    os.rename(os.path.join(d2, "journal.lmw"),
              os.path.join(d2, "journal.lmw.aside"))
    os.rename(os.path.join(d2, "checkpoint.lmi"),
              os.path.join(d2, "checkpoint.lmi.aside"))
    inst = Instance("B2", PORT_B, d2)
    inst.wait_ready()
    _, st = http("GET", PORT_B, "/stats")
    s, b = http("POST", PORT_B, "/query", {"relation": "spectra"})
    print(f"[B2] both files moved aside: starts as EMPTY db: "
          f"sequence={st['sequence']} keys={st['keys']} "
          f"query spectra -> {s} {b.get('error', {}).get('code')}")
    inst.stop()


def phase_c():
    print("=" * 72)
    print("PHASE C: supported compaction = export changes + backup + restore")
    print("=" * 72)
    data = os.path.join(ROOT, "data-a")
    inst = Instance("C1", PORT_A, data)
    inst.wait_ready()
    # C0: export the full change history for offline consumers BEFORE cutover
    s, ch = http("GET", PORT_A, "/changes?after=0&limit=1000")
    export_path = os.path.join(ROOT, "changes-export-seq0-33.json")
    with open(export_path, "w") as f:
        json.dump(ch, f, indent=1)
    print(f"[C0] exported {len(ch['records'])} change records "
          f"(cursor={ch['cursor']}, head={ch['head']}) -> {export_path} "
          f"({os.path.getsize(export_path)} bytes)")
    # C1: online backup
    s, blob = http("GET", PORT_A, "/backup", raw=True)
    backup = os.path.join(ROOT, "lamina-backup.lmi")
    with open(backup, "wb") as f:
        f.write(blob)
    print(f"[C1] backup image: {len(blob)} bytes "
          f"(journal is {journal_size(data)} bytes)")
    inst.stop()
    # C2: restore into a NEW directory
    data_r = os.path.join(ROOT, "data-restored")
    shutil.rmtree(data_r, ignore_errors=True)
    inst_r = Instance("C2", PORT_B, data_r, restore=backup)
    inst_r.wait_ready()
    _, st = http("GET", PORT_B, "/stats")
    print(f"[C2] restored instance: sequence={st['sequence']} "
          f"horizon={st['horizon']} versions={st['versions']} "
          f"journal_records={st['journal_records']} "
          f"history_base={st['history_base']}")
    print(f"[C3] restored dir: {dir_sizes(data_r)}")
    # C3: continuity semantics for consumers
    s0, b0 = http("GET", PORT_B, "/changes?after=0&limit=10")
    s32, b32 = http("GET", PORT_B, "/changes?after=32&limit=10")
    s33, b33 = http("GET", PORT_B, "/changes?after=33&limit=10")
    print(f"[C4] changes after=0 -> {s0} {b0.get('error', b0)}")
    print(f"     changes after=32 -> {s32} {b32.get('error', b32)}")
    print(f"     changes after=33 -> {s33} records={len(b33.get('records', []))} "
          f"cursor={b33.get('cursor')} head={b33.get('head')}")
    # C4: retained snapshot reads survive restore
    s_at, b_at = http("POST", PORT_B, "/query",
                      {"relation": "spectra", "at": 32})
    s_old, b_old = http("POST", PORT_B, "/query",
                        {"relation": "spectra", "at": 31})
    print(f"[C5] restored query at=32 -> {s_at} rows="
          f"{len(b_at.get('rows', []))}; at=31 -> {s_old} "
          f"{b_old.get('error', {}).get('code')}")
    # C5: consumers re-align at history_base and keep following new commits
    seq = commit_rows(PORT_B, [
        {"sample": 1, "band": "post-restore", "amplitude": 99}])
    s, b = http("GET", PORT_B, "/changes?after=33&limit=10")
    print(f"[C6] new commit seq={seq}; changes after=33 -> "
          f"records={len(b['records'])} kinds="
          f"{[r['kind'] for r in b['records']]}")
    _, st = http("GET", PORT_B, "/stats")
    print(f"[C7] restored dir after 1 new commit: {dir_sizes(data_r)}; "
          f"journal_records={st['journal_records']}")
    inst_r.stop()


def calibrate():
    """Find band length + one long band so the journal is exactly 7350 bytes
    before vacuum."""
    for band_len, extra in [(16, 0), (16, 32), (17, 0), (15, 0)]:
        data = os.path.join(ROOT, "data-cal")
        shutil.rmtree(data, ignore_errors=True)
        inst = Instance("CAL", PORT_A, data)
        inst.wait_ready()
        lb = (0, 0, extra) if extra else None
        run_workload(PORT_A, band_len, lb)
        j = journal_size(data)
        inst.stop()
        print(f"[cal] band_len={band_len} extra={extra}: journal={j}")
        if j == 7350:
            return band_len, lb
    # interpolation fallback: sizes are linear in band length
    return None, None


def main():
    os.makedirs(ROOT, exist_ok=True)
    if len(sys.argv) > 1 and sys.argv[1] == "calibrate":
        calibrate()
        return
    band_len, lb = 18, None
    j0, j2 = phase_a(band_len, lb, keep_dir=os.path.join(ROOT, "data-a-keep"))
    assert j0 == 7350, f"pre-vacuum journal {j0} != 7350"
    assert j2 == 7413, f"post-checkpoint journal {j2} != 7413"
    phase_b()
    phase_c()
    print("=" * 72)
    print("ALL PHASES DONE")


if __name__ == "__main__":
    main()
