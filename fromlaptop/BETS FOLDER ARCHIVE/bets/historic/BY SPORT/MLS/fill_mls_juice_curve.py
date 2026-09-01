import pandas as pd
import numpy as np

obs = pd.read_csv("mls_3way_juice.csv")
grid = pd.read_csv("mls_full_grid.csv")

rows = []

for side in ["home","draw","away"]:
    sub = obs[obs.side == side].sort_values("fair_prob")

    x = sub.fair_prob.values
    y = sub.extra_juice.values

    bins = grid[grid.side == side].fair_prob.values

    y_interp = np.interp(bins, x, y)

    for b,v in zip(bins,y_interp):
        rows.append((side,b,v))

final = pd.DataFrame(rows,columns=["side","fair_prob","extra_juice"])

final.to_csv("mls_3way_juice.csv",index=False)

print("MLS 3way juice curve rebuilt")
print(final.groupby("side").size())