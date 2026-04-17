from datetime import datetime, time
from config import CONFIG


def _t(s: str) -> time:
    """Parse HH:MM string to time object on demand."""
    return datetime.strptime(s, "%H:%M").time()


def _as_time(value) -> time:
    """Normalize pandas/py datetime-like values to ``datetime.time``."""
    if isinstance(value, time):
        return value
    if hasattr(value, "time"):
        return value.time()
    raise TypeError(f"Unsupported time value type: {type(value)!r}")


def is_entry_allowed(current_time) -> bool:
    ct = _as_time(current_time)
    return _t(CONFIG["entry_start_time"]) <= ct <= _t(CONFIG["entry_end_time"])


def is_force_exit_time(current_time) -> bool:
    ct = _as_time(current_time)
    return ct >= _t(CONFIG["force_exit_time"])
