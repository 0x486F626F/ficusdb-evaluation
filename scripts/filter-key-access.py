import sys

get_ops = set(["getstate", "getcodehash", "getnonce", "getbalance"])
put_ops = set(["setstate", "setcodehash", "setnonce", "setbalance", "createaccount", "addbalance", "subbalance"])
no_ops = set(["newstatedb", "commit", "blocknum", "revertsnapshot", "snapshot", "finalise", "removeaccount"])

output = open(sys.argv[2], "w")
for line in open(sys.argv[1]):
    parts = line.strip().split()
    if len(parts) < 2:
        continue
    if parts[0] in get_ops:
        if parts[0] == "getstate":
            output.write("get {} {}\n".format(parts[1], parts[2]))
        else:
            output.write("get {}\n".format(parts[1]))
    elif parts[0] in put_ops:
        if parts[0] == "setstate":
            output.write("put {} {}\n".format(parts[1], parts[2]))
        else:
            output.write("put {}\n".format(parts[1]))
    