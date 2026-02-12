import sys

get_ops = set(["getstate", "getcodehash", "getnonce", "getbalance"])
put_ops = set(["setstate", "setcode", "setnonce", "setbalance", "createaccount", "addbalance", "subbalance"])

accounts = {}

for line in open(sys.argv[1]):
    parts = line.strip().split()
    if len(parts) < 2:
        continue
    if parts[0] in get_ops or parts[0] in put_ops:
        acc = parts[1]
        if acc not in accounts:
            accounts[acc] = 1
        accounts[acc] += 1

output = open(sys.argv[2], "w")
for acc, freq in sorted(accounts.items(), key=lambda kv: (-kv[1], kv[0])):
    output.write("{} {}\n".format(acc, freq))