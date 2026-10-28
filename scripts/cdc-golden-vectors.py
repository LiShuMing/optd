#!/usr/bin/env python3
"""Independent optd C1 fixtures using only Python struct and hashlib.

Run from the repository root. The Rust protocol tests embed these byte/hash values
so a self-consistent Rust encoder/decoder bug cannot validate its own wire format.
"""
import hashlib
import json
import struct


def encode(value):
    if value is None:
        return b"\x00"
    if isinstance(value, bool):
        return b"\x01" + bytes([value])
    if isinstance(value, int):
        return b"\x02" + struct.pack(">q", value)
    if isinstance(value, str):
        content = value.encode("utf-8")
        return b"\x03" + struct.pack(">Q", len(content)) + content
    if isinstance(value, list):
        return b"\x05" + struct.pack(">Q", len(value)) + b"".join(map(encode, value))
    if isinstance(value, dict):
        pairs = sorted(value.items(), key=lambda item: item[0].encode("utf-8"))
        return b"\x06" + struct.pack(">Q", len(pairs)) + b"".join(
            encode(key) + encode(item) for key, item in pairs
        )
    raise TypeError(type(value))


for fixture in ([None, True, -1, "中"], ["optd-delta/1", []], {"z": None, "a": [1, False]}):
    encoded = encode(fixture)
    print(json.dumps({
        "value": fixture,
        "c1_hex": encoded.hex(),
        "fingerprint": "sha256:" + hashlib.sha256(encoded).hexdigest(),
    }, ensure_ascii=False))

import base64
row_identity = ["optd-row/1", "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002", [[1000, 7]]]
print(json.dumps({"row_identity": row_identity,
                  "row_id": base64.urlsafe_b64encode(encode(row_identity)).decode().rstrip("=")}))
