import struct
import random
import threading
from time import time_ns
from collections import defaultdict
from typing import Any, List, Optional, Dict, Tuple

key = str | int | bool
_vs = "vs"
_t = "t"
_k = "k"
_v = "v"
_ttl_expiry = "ttl_expiry"


class versionstamp:
    def __init__(self) -> None:
        self._prev_time = 0
        self._count = 0
        self._lock = threading.Lock()

    def make(self) -> str:
        now = time_ns() // 1_000
        with self._lock:
            if now != self._prev_time:
                self._prev_time = now
                self._count = 0
            else:
                self._count += 1
            packed = struct.pack(">QHH", self._prev_time, self._count, random.getrandbits(16))
        return packed.hex()
v = versionstamp()
def recursive_dict():
    return defaultdict(recursive_dict)

class crater:
    _lock = threading.Lock()
    def __init__(self, db: bool = True):
        self._store = recursive_dict()
        self._versions = versionstamp()
        self._kfv= {}
        self._db = db
        if db:
            self._db_path = "crater.json"
            

    def _get_node(self, key: List[key], create: bool = False) -> Optional[defaultdict]:
        node = self._store
        for part in key[:-1]:
            if part not in node:
                if create:
                    node[part] = recursive_dict()
                else:
                    return None
            node = node[part]
        return node

    def _is_expired(self, val: Dict) -> bool:
        expiry_time = val.get(_ttl_expiry)
        if expiry_time:
            if time_ns() // 1_000 > expiry_time:
                return True
        return False

    def set(self, key: List[key], value: Any, expected_version: str | None = None, ttl_seconds: int | None=None) -> Optional[Dict]:
        with self._lock:
            node = self._get_node(key, create=True)
            if node is not None:
                prev_val = node.get(key[-1])
                if prev_val and expected_version:
                    if prev_val.get(_vs) != expected_version:
                        return None
                version = self._versions.make()
                now = time_ns() // 1_000
                expiry = (now + ttl_seconds * 1_000_000) if ttl_seconds and ttl_seconds> 0 else None 
                val = {
                    _vs:version,
                    _t :now,
                    _k:key,
                    _v:value,
                    _ttl_expiry: expiry
                }
                node[key[-1]] = val
                self._kfv[version] = key
                return val


    def get(self, key: List[key]) -> Optional[Dict]:
        with self._lock:
            node = self._get_node(key)
            if not node:
                return None
            val = node.get(key[-1])
            if isinstance(val, Dict):
                if self._is_expired(val):
                    # TODO: delete the node
                    return None
                else:
                    return val
            return None

    def get_key_from_version(self, version: str) -> Optional[List[key]]:
        with self._lock:
            return self._kfv.get(version)

    def delete(self, key: List[key]) -> Optional[Dict]:
        with self._lock:
            node = self._get_node(key)
            if node and key[-1] in node:
                val = node.pop(key[-1])
                if isinstance(val, Dict):
                    ver = val.get(_vs)
                    if ver:
                        self._kfv.pop(ver, None)
                    return val
            return None

    def list(self, prefix: List[key], limit: int | None = None) -> Optional[List[Dict]]:
        with self._lock:
            def dfs(node, collected: List[Dict]):
                for v in node.values():
                    if isinstance(v, Dict):
                        collected.append(v)
                    elif isinstance(v, dict):
                        dfs(v, collected)
                    if limit and len(collected) >= limit:
                        break

            node = self._store
            for part in prefix:
                if part not in node:
                    return None
                node = node[part]

            results: List[Dict] = []
            dfs(node, results)
            return results or None

