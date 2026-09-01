import pandas as pd
import numpy as np

df = pd.read_csv("mls_3way_juice.csv")

rows = []

for side in ["home","draw","away"]:
    sub = df[df.side == side].sort_values("fair_prob").copy()

    y = sub["extra_juice"].to_numpy().copy()

    # enforce non-decreasing curve
    for i in range(1, len(y)):
        if y[i] < y[i-1]:
            y[i] = y[i-1]

    sub["extra_juice"] = y
    rows.append(sub)

final = pd.concat(rows).sort_values(["side","fair_prob"])

final.to_csv("mls_3way_juice.csv", index=False)

print("curve smoothed successfully")
print(final[final.side=="home"][["fair_prob","extra_juice"]].tail(10))