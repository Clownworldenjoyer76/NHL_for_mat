from pathlib import Path
from datetime import datetime, timezone

LOGFILE = Path("outputs/network_log.txt")

def log_event(event: str):
    """Append a line to outputs/network_log.txt with a timestamp."""
    try:
        LOGFILE.parent.mkdir(parents=True, exist_ok=True)
        with LOGFILE.open("a", encoding="utf-8") as f:
            f.write(f"[{datetime.now(timezone.utc).isoformat()}] {event}\n")
    except Exception:
        pass
