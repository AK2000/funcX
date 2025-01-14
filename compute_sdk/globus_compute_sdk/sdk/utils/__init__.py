from __future__ import annotations

import sys
import typing as t
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from platform import platform

import dill
import globus_compute_sdk.version as sdk_version


def chunk_by(iterable: t.Iterable, size) -> t.Iterable[tuple]:
    to_chunk_iter = iter(iterable)
    return iter(lambda: tuple(islice(to_chunk_iter, size)), ())


def _log_tmp_file(item, filename: Path | None = None) -> None:
    """
    Convenience method for logging to a tmp file for debugging, getting
    around most thread or logging setup complexities.
    Usage of this should be removed after dev testing is done

    Not intended for non-development debugging usage

    :param item: The info to log to file
    :param filename: An optional path to the log file.   If None,
                      use a file in the current user's home directory
    """
    if filename is None:
        filename = Path.home() / "compute_debug.log"
    with open(filename, "a") as f:
        ts = datetime.now(timezone.utc).astimezone().isoformat()
        # Console as well as write to file
        print(ts, item, file=f)


def get_env_details() -> t.Dict[str, t.Any]:
    return {
        "os": platform(),
        "dill_version": dill.__version__,
        "python_version": ".".join(map(str, sys.version_info[:3])),
        "globus_compute_sdk_version": sdk_version.__version__,
    }


def check_version(task_details: dict | None) -> str | None:
    """
    This method adds an optional string to some Exceptions below to
    warn of differing environments as a possible cause.  It is pre-pended

    """
    if task_details:
        worker_py = task_details.get("python_version", "UnknownPy")
        worker_dill = task_details.get("dill_version", "UnknownDill")
        worker_os = task_details.get("os", "UnknownOS")
        client_details = get_env_details()
        sdk_py = client_details["python_version"]
        sdk_dill = client_details["dill_version"]
        if (sdk_py, sdk_dill) != (worker_py, worker_dill):
            return (
                f"Warning:  Client SDK uses Python {sdk_py}/Dill {sdk_dill} "
                f"but worker used {worker_py}/{worker_dill} (OS: {worker_os})"
            )
    return None
