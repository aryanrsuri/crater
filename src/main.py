import time
from crater import versionstamp, crater

    
def test_expired_tokens(c: crater):
    c.set(["expired_in_1"], 1, ttl_seconds=1)
    print("get")
    print(c.get(["expired_in_1"]))
    print("list")
    print(c.list(["expired_in_1"]))
    time.sleep(1)
    print("Getting expired token")
    print("lsit")
    print(c.list(["expired_in_1"]))
    print("get")
    print(c.get(["expired_in_1"]))

if __name__ == "__main__":
    c = crater()
    vs = versionstamp()
    first = c.set(["counter"], 1, ttl_seconds=None)
    if first:
        print(first)
        vers = first["vs"]
    else: 
        raise KeyError
    print(c.get_key_from_version(vers))
    print(c.incr(["counter"]))
    print(c.incr(["counter"]))
    print(c.incr(["counter"]))
    print(c.decr(["counter"]))
    print(c.get_key_from_version(vers))
    print(c.set(["s_counter"], "1"))
    print(c.incr(["s_counter"]))

    print("creating and incrementing counter")
    print(c.incr(["uninit_counter"]))

    print("creating and decrementing counter")
    print(c.decr(["uninit_decounter"], ttl_seconds=10))
    d = c.get(["uninit_decounter"])
    print(d)
    test_expired_tokens(c)



