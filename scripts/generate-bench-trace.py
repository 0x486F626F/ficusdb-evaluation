import sys
import random

keys = []
freq = []

for line in open(sys.argv[1]):
    parts = line.strip().split()
    keys.append(parts[0])
    freq.append(int(parts[1]))

print(f"Loaded {len(keys)} keys...")

nops = int(sys.argv[2])
output = open(sys.argv[3], "w")

BATCH = 1_000_000
remaining = nops
while remaining > 0:
    print(f"Generating {remaining} operations...")
    k = BATCH if remaining > BATCH else remaining
    sample_keys = random.choices(keys, weights=freq, k=k)
    output.writelines(key + "\n" for key in sample_keys)
    remaining -= k