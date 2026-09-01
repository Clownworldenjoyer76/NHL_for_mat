import pandas as pd
import numpy as np

df = pd.read_csv("mls_probs.csv")

rows = []
bins = np.linspace(0.02, 0.90, 50)

for side, imp, fair in [
    ("home", "home_imp", "home_fair"),
    ("draw", "draw_imp", "draw_fair"),
    ("away", "away_imp", "away_fair"),
]:
    for b in bins:
        mask = (df[fair] > b - 0.01) & (df[fair] < b + 0.01)

        if mask.sum() > 10:
            extra = (df.loc[mask, imp] - df.loc[mask, fair]).mean()
            rows.append((side, b, extra))

juice = pd.DataFrame(rows, columns=["side", "fair_prob", "extra_juice"])

juice.to_csv("mls_3way_juice.csv", index=False)

print("Created mls_3way_juice.csv")
print(juice.head())