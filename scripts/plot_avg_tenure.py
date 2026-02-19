import re
import os
from pathlib import Path
import csv
import math
import matplotlib.pyplot as plt

# Paths
base = Path(os.path.expanduser("~")) / "Documents"
input_csv = base / "location_tenure_counts.csv"
output_dir = base / "output"
output_dir.mkdir(parents=True, exist_ok=True)
output_png = output_dir / "avg_tenure_by_location.png"

def tenure_to_years(s):
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    if re.search(r"<\s*1", s):
        return 0.5
    m = re.search(r"(\d+)\s*-\s*(\d+)", s)
    if m:
        a = int(m.group(1))
        b = int(m.group(2))
        return (a + b) / 2.0
    m2 = re.search(r"(\d+)\s*\+", s)
    if m2:
        a = int(m2.group(1))
        return a + 2.0
    m3 = re.search(r"(\d+)", s)
    if m3:
        return float(m3.group(1))
    return None

# Read CSV and aggregate
counts = {}
with open(input_csv, newline='', encoding='utf-8') as fh:
    reader = csv.DictReader(fh)
    # infer count column as the column that's not Location or Tenure
    fieldnames = reader.fieldnames or []
    count_col = None
    for fn in fieldnames:
        if fn not in ('Location', 'Tenure'):
            count_col = fn
            break
    if count_col is None:
        raise SystemExit('Could not find count column in CSV')
    for row in reader:
        loc = row.get('Location')
        ten = row.get('Tenure')
        try:
            cnt = float(row.get(count_col) or 0)
        except Exception:
            cnt = 0.0
        years = tenure_to_years(ten)
        if years is None:
            continue
        rec = counts.setdefault(loc, {'total': 0.0, 'weighted': 0.0})
        rec['total'] += cnt
        rec['weighted'] += years * cnt

# Compute averages
rows = []
for loc, v in counts.items():
    if v['total'] <= 0:
        continue
    avg = v['weighted'] / v['total']
    rows.append((loc, v['total'], avg))

rows.sort(key=lambda x: x[2], reverse=True)

# Plot top 30
topn = rows[:30]
if not topn:
    raise SystemExit('No data to plot')
locations = [r[0] for r in topn]
avgs = [r[2] for r in topn]

plt.figure(figsize=(12,8))
plt.barh(locations, avgs, color='C0')
plt.gca().invert_yaxis()
plt.xlabel('Average tenure (years)')
plt.title('Average tenure by Location (top 30 by average)')
plt.tight_layout()
plt.savefig(output_png)
print('WROTE', output_png)
for loc, total, avg in topn:
    print(f"{loc}: count={int(total):,}, avg_tenure={avg:.2f}")
