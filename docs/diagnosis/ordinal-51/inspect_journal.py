#!/usr/bin/env python3
"""Parse journal.lmw (LMW1) and list every frame: offset, size, kind, seq."""
import json
import struct
import sys
import zlib

path = sys.argv[1]
data = open(path, "rb").read()
magic, base, hcrc = data[:4], struct.unpack(">Q", data[4:12])[0], data[12:16]
assert magic == b"LMW1", magic
print(f"header: 16 bytes, base={base}, file={len(data)} bytes")
off = 16
n = 0
while off < len(data):
    (length,) = struct.unpack(">I", data[off:off + 4])
    crc = data[off + 4:off + 8]
    payload = data[off + 8:off + 8 + length]
    ok = zlib.crc32(payload) == struct.unpack(">I", crc)[0]
    r = json.loads(payload)
    kind = r["kind"]
    extra = ""
    if kind == "commit":
        extra = f"writes={len(r['writes'])}"
    elif kind == "vacuum":
        extra = f"horizon={r['horizon']}"
    elif kind == "relation":
        extra = f"name={r['relation']['name']}"
    print(f"  frame@{off:5d} size={8 + length:4d} seq={r['sequence']:3d} "
          f"kind={kind:8s} crc_ok={ok} {extra}")
    off += 8 + length
    n += 1
print(f"frames={n}, end_offset={off}")
