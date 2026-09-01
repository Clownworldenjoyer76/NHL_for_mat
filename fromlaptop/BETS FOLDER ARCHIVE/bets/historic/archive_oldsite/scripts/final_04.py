import pandas as pd
import glob
from pathlib import Path

BASE_IN = Path("docs/win/final/step_1")
BASE_OUT = Path("docs/win/final/step_2")

FILES = 0
ROWS_IN = 0
ROWS_OUT = 0


def run():
    global FILES, ROWS_IN, ROWS_OUT

    for f in glob.glob(str(BASE_IN / "**/*.csv"), recursive=True):
        df = pd.read_csv(f)
        FILES += 1
        ROWS_IN += len(df)

        rel_path = Path(f).relative_to(BASE_IN)
        out_path = BASE_OUT / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(out_path, index=False)
        ROWS_OUT += len(df)

        print(f"Wrote {out_path} | rows={len(df)}")

    print("\n=== FINAL_04 SUMMARY ===")
    print(f"Files processed: {FILES}")
    print(f"Rows in: {ROWS_IN}")
    print(f"Rows out: {ROWS_OUT}")

    if FILES == 0:
        raise RuntimeError("final_04: 0 files processed")

    if ROWS_OUT == 0:
        raise RuntimeError("final_04: 0 rows written")


if __name__ == "__main__":
    run()
