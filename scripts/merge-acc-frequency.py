import sys

accounts = {}
for file in sys.argv[1:-1]:
    for line in open(file):
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        acc = parts[0]
        if acc not in accounts:
            accounts[acc] = int(parts[1])
        accounts[acc] += int(parts[1])

output = open(sys.argv[-1], "w")
for acc, freq in sorted(accounts.items(), key=lambda kv: (-kv[1], kv[0])):
    output.write("{} {}\n".format(acc, freq))