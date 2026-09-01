# scripts/winners_04.py

#!/usr/bin/env python3

import os
import glob
import traceback
from pathlib import Path
from datetime import datetime

import pandas as pd

INPUT_DIR = Path("docs/win/winners/step_02_1")
OUTPUT_DIR = Path("docs/win/winners/step_02_1")
ERROR_LOG = Path("docs/win/errors/09_winners/winners_04.txt")

CLEANED_DIR = Path("docs/win/dump/csvs/cleaned")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)


def _s(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def _f(x):
    try:
        if pd.isna(x):
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def build_projection_map(log):
    proj_map = {}

    cleaned_files = sorted(glob.glob(str(CLEANED_DIR / "*.csv")))
    log.write("=== CLEANED SCAN ===\n")
    log.write(f"CWD: {os.getcwd()}\n")
    log.write(f"CLEANED_DIR: {CLEANED_DIR}\n")
    log.write(f"Cleaned files found: {len(cleaned_files)}\n")

    scanned = 0
    used = 0
    skipped_no_game_projected_points = 0
    skipped_no_game_id = 0
    loaded_rows = 0

    for fp in cleaned_files:
        scanned += 1
        try:
            df = pd.read_csv(fp)

            if "game_projected_points" not in df.columns:
                skipped_no_game_projected_points += 1
                log.write(f"SKIP(no game_projected_points): {fp}\n")
                continue

            if "game_id" not in df.columns:
                skipped_no_game_id += 1
                log.write(f"SKIP(no game_id): {fp}\n")
                continue

            used += 1
            file_loaded = 0

            for _, r in df.iterrows():
                gid = _s(r["game_id"])
                gpp = _f(r["game_projected_points"])
                if gid and gpp is not None:
                    proj_map[gid] = gpp
                    file_loaded += 1

            loaded_rows += file_loaded
            log.write(f"USE: {fp} | loaded_rows={file_loaded}\n")

        except Exception:
            log.write(f"ERROR reading: {fp}\n")
            log.write(traceback.format_exc())
            log.write("\n")

    log.write("\n--- CLEANED SUMMARY ---\n")
    log.write(f"Files scanned: {scanned}\n")
    log.write(f"Files used: {used}\n")
    log.write(f"Skipped(no game_projected_points): {skipped_no_game_projected_points}\n")
    log.write(f"Skipped(no game_id): {skipped_no_game_id}\n")
    log.write(f"Projection rows loaded: {loaded_rows}\n\n")

    return proj_map


def process():
    ts = datetime.utcnow().isoformat() + "Z"

    with open(ERROR_LOG, "w") as log:
        log.write("=== WINNERS_04 START ===\n")
        log.write(f"Timestamp: {ts}\n")
        log.write(f"INPUT_DIR: {INPUT_DIR}\n")
        log.write(f"OUTPUT_DIR: {OUTPUT_DIR}\n")
        log.write(f"ERROR_LOG: {ERROR_LOG}\n\n")

        proj_map = build_projection_map(log)

        winners_files = sorted(glob.glob(str(INPUT_DIR / "*.csv")))
        log.write("=== WINNERS SCAN ===\n")
        log.write(f"Winners files found: {len(winners_files)}\n\n")

        files_processed = 0
        global_rows = 0
        global_eligible = 0
        global_proj_match = 0
        global_proj_miss = 0
        global_total_missing = 0
        global_total_bad = 0
        global_rows_updated = 0

        for fp in winners_files:
            try:
                df = pd.read_csv(fp)

                # Ensure columns exist
                if "proj_total" not in df.columns:
                    df["proj_total"] = pd.NA
                if "total_diff" not in df.columns:
                    df["total_diff"] = pd.NA

                # FORCE dtype to float64 (this fixes the crash)
                df["proj_total"] = pd.to_numeric(df["proj_total"], errors="coerce")
                df["total_diff"] = pd.to_numeric(df["total_diff"], errors="coerce")

                file_rows = len(df)
                file_eligible = 0
                file_proj_match = 0
                file_proj_miss = 0
                file_total_missing = 0
                file_total_bad = 0
                file_rows_updated = 0

                for i, r in df.iterrows():
                    global_rows += 1

                    bet = _s(r.get("bet", ""))
                    if bet != "over_bet" and bet != "under_bet":
                        continue

                    file_eligible += 1
                    global_eligible += 1

                    gid = _s(r.get("game_id", ""))
                    if gid in proj_map:
                        proj_total = proj_map[gid]
                        df.at[i, "proj_total"] = proj_total
                        file_proj_match += 1
                        global_proj_match += 1

                        if "total" not in df.columns:
                            file_total_missing += 1
                            global_total_missing += 1
                            continue

                        total_val = r.get("total", None)
                        if pd.isna(total_val) or str(total_val).strip() == "":
                            file_total_missing += 1
                            global_total_missing += 1
                            continue

                        total_f = _f(total_val)
                        if total_f is None:
                            file_total_bad += 1
                            global_total_bad += 1
                            continue

                        df.at[i, "total_diff"] = total_f - proj_total
                        file_rows_updated += 1
                        global_rows_updated += 1
                    else:
                        file_proj_miss += 1
                        global_proj_miss += 1

                out_path = OUTPUT_DIR / Path(fp).name
                df.to_csv(out_path, index=False)

                files_processed += 1

                log.write(f"FILE: {fp}\n")
                log.write(f"  rows: {file_rows}\n")
                log.write(f"  eligible(over_bet/under_bet): {file_eligible}\n")
                log.write(f"  proj_match: {file_proj_match}\n")
                log.write(f"  proj_miss: {file_proj_miss}\n")
                log.write(f"  total_missing: {file_total_missing}\n")
                log.write(f"  total_bad_number: {file_total_bad}\n")
                log.write(f"  rows_updated(proj_total+total_diff): {file_rows_updated}\n\n")

            except Exception:
                log.write(f"ERROR processing: {fp}\n")
                log.write(traceback.format_exc())
                log.write("\n")

        log.write("=== GLOBAL SUMMARY ===\n")
        log.write(f"Files processed: {files_processed}\n")
        log.write(f"Rows processed: {global_rows}\n")
        log.write(f"Eligible rows: {global_eligible}\n")
        log.write(f"Projection matches: {global_proj_match}\n")
        log.write(f"Projection misses: {global_proj_miss}\n")
        log.write(f"Total missing: {global_total_missing}\n")
        log.write(f"Total bad number: {global_total_bad}\n")
        log.write(f"Rows updated: {global_rows_updated}\n")
        log.write("=== WINNERS_04 COMPLETE ===\n")


if __name__ == "__main__":
    process()
