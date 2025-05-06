import struct
import random
import threading
from enum import Enum
from time import time_ns
from collections import defaultdict
from typing import Any, List, Optional, Dict

class Err(Enum):
    WriteFail = "Failed to Write to Key"
    IncrDecrTypeError = "Attempted incr/decr on a non-integer value"
    TTLKeyExpiry = "Attempted access on an expired key"
    InvalidDataFormat = "Retrived data is not in the expected formet"

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

def recursive_dict():
    return defaultdict(recursive_dict)

class crater:
    _lock = threading.Lock()
    def __init__(self):
        self._store = recursive_dict()
        self._versions = versionstamp()
        self._kfv: Dict = {}
        self._errors: List[Err] = []

    def _log_err(self, err: Err) -> None:
        print(f"[ERR] {err.value}")
        self._errors.append(err)
        return None

    def _delete_node(self) -> None:
        raise NotImplemented

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

    def incr(self, key: List[key], ttl_seconds: int | None = None) -> Optional[int]:
        with self._lock:
            node = self._get_node(key, create=True)
            if node is not None:
                val = node.get(key[-1])
                if isinstance(val, Dict):
                    if self._is_expired(val):
                        return self._log_err(Err.TTLKeyExpiry)
                    if isinstance(val.get(_v), int):
                        version = self._versions.make()
                        self._kfv.pop(val[_vs], None)
                        val[_vs] = version
                        val[_v] += 1
                        node[key[-1]] = val
                        self._kfv[version] = key
                        return val[_v]
                    else:
                        return self._log_err(Err.IncrDecrTypeError)
                else:
                    version = self._versions.make()
                    now = time_ns() // 1_000
                    expiry = (now + ttl_seconds * 1_000_000) if ttl_seconds and ttl_seconds > 0 else None
                    val = { _vs: version, _t: now, _k: key, _v: 1, _ttl_expiry : expiry}
                    node[key[-1]] = val
                    self._kfv[version] = key
                    return 1
            return None

    def decr(self, key: List[key], ttl_seconds: int | None = None) -> Optional[int]:
        with self._lock:
            node = self._get_node(key, create=True)
            if node is not None:
                val = node.get(key[-1])
                if isinstance(val, Dict):
                    if self._is_expired(val):
                        return self._log_err(Err.TTLKeyExpiry)
                    if isinstance(val.get(_v), int):
                        version = self._versions.make()
                        self._kfv.pop(val[_vs], None)
                        val[_vs] = version
                        val[_v] -= 1
                        node[key[-1]] = val
                        self._kfv[version] = key 
                        return val[_v]
                    else:
                        return self._log_err(Err.IncrDecrTypeError)
                else:
                    version = self._versions.make()
                    now = time_ns() // 1_000
                    expiry = (now + ttl_seconds * 1_000_000) if ttl_seconds and ttl_seconds > 0 else None
                    val = { _vs: version, _t: now, _k: key, _v: -1, _ttl_expiry : expiry}
                    node[key[-1]] = val
                    self._kfv[version] = key
                    return -1
            return None

    def set(self,
            key: List[key],
            value: Any,
            expected_version: Optional[str] = None,
            ttl_seconds: Optional[int] = None
           ) -> Optional[Dict]:
        with self._lock:
            node = self._get_node(key, create=False)
            current_val = None
            last_key_part = key[-1]

            if node and last_key_part in node:
                potential_val = node.get(last_key_part)
                if isinstance(potential_val, dict) and _vs in potential_val:
                    if self._is_expired(potential_val):
                        return self._log_err(Err.TTLKeyExpiry)
                    current_val = potential_val
            perform_set = False
            old_version_to_remove = None

            if expected_version is not None:
                if current_val is not None and current_val.get(_vs) == expected_version:
                    perform_set = True
                    old_version_to_remove = expected_version
                else:
                    self._log_err(Err.WriteFail)
                    return None
            else:
                perform_set = True
                if current_val is not None:
                     old_version_to_remove = current_val.get(_vs)

            if perform_set:
                new_version = self._versions.make()
                now = time_ns() // 1000
                expiry_time = (now + ttl_seconds * 1_000_000) if ttl_seconds is not None and ttl_seconds > 0 else None

                new_val_dict = {
                    _vs: new_version,
                    _t: now,
                    _k: key,
                    _v: value,
                    _ttl_expiry: expiry_time
                }

                node = self._get_node(key, create=True)
                if node is None:
                     self._log_err(Err.WriteFail)
                     return None

                node[last_key_part] = new_val_dict

                if old_version_to_remove:
                    self._kfv.pop(old_version_to_remove, None)
                self._kfv[new_version] = key

                return new_val_dict

            return self._log_err(Err.WriteFail)

    def get(self, key: List[key]) -> Optional[Dict]:
        with self._lock:
            node = self._get_node(key)
            if not node:
                return None
            val = node.get(key[-1])
            if isinstance(val, Dict):
                if self._is_expired(val):
                    return self._log_err(Err.TTLKeyExpiry)
                return val
            return self._log_err(Err.InvalidDataFormat)

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

    def list(self, prefix: List[key], limit: Optional[int] = None) -> Optional[List[Dict]]:
        with self._lock:
            node = self._store
            for part in prefix:
                if part not in node or not isinstance(node.get(part), dict):
                    return None

                node = node[part]

            results: List[Dict] = []
            stack = [node]

            while stack:
                current_node = stack.pop()

                for item_value in current_node.values():
                    if isinstance(item_value, Dict) and _vs in item_value:
                        if self._is_expired(item_value):
                            return self._log_err(Err.TTLKeyExpiry)
                        results.append(item_value)
                        if limit and len(results) >= limit:
                            return results
                    elif isinstance(item_value, dict):
                        if not limit or len(results) < limit:
                             stack.append(item_value)

            return results or None
