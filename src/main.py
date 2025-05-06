from crater import versionstamp, crater


if __name__ == "__main__":
    c = crater()
    vs = versionstamp()
    for _ in range(100):
        print(vs.make())
