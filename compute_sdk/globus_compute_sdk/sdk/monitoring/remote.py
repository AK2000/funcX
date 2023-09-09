import os
import time
import logging
import datetime
from functools import wraps
import inspect

from parsl.multiprocessing import ForkProcess
from multiprocessing import Event, Barrier
from parsl.process_loggers import wrap_with_logs

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios import MonitoringRadio, UDPRadio, HTEXRadio, FilesystemRadio
from typing import Any, Callable, Dict, List, Sequence, Tuple

logger = logging.getLogger(__name__)


def monitor_wrapper(f: Any) -> Callable:

    """Wrap the Parsl app with a function that will call the monitor function and point it at the correct pid when the task begins.
    """

    # this makes assumptions that when subsequently executed with the same
    # cache key, then the relevant parameters will not have changed from the
    # first invocation with that cache key (otherwise, the resulting cached
    # closure will be incorrectly cached)
    @wraps(f)
    def wrapped(*args: List[Any], _globus_compute_task_id: str, **kwargs: Dict[str, Any]) -> Any:
        # Fetch monitoring information from environment
        import os
        monitoring_hub_url = os.environ["PARSL_MONITORING_HUB_URL"]
        radio_mode = os.environ["PARSL_MONITORING_RADIO_MODE"]
        run_id = os.environ["PARSL_RUN_ID"]
        run_dir = os.environ["PARSL_RUN_DIR"]

        task_id = _globus_compute_task_id
        try_id = 0
        terminate_event = Event()
        # Send first message to monitoring router
        send_first_message(try_id,
                            task_id,
                            monitoring_hub_url,
                            run_id,
                            radio_mode,
                            run_dir)

        try:
            return f(*args, **kwargs)
        finally:
            send_last_message(try_id,
                                task_id,
                                monitoring_hub_url,
                                run_id,
                                radio_mode, run_dir)
    return wrapped


@wrap_with_logs
def send_first_message(try_id: int,
                       task_id: int,
                       monitoring_hub_url: str,
                       run_id: str, radio_mode: str, run_dir: str) -> None:
    send_first_last_message(try_id, task_id, monitoring_hub_url, run_id,
                            radio_mode, run_dir, False)


@wrap_with_logs
def send_last_message(try_id: int,
                      task_id: int,
                      monitoring_hub_url: str,
                      run_id: str, radio_mode: str, run_dir: str) -> None:
    send_first_last_message(try_id, task_id, monitoring_hub_url, run_id,
                            radio_mode, run_dir, True)


def send_first_last_message(try_id: int,
                            task_id: int,
                            monitoring_hub_url: str,
                            run_id: str, radio_mode: str, run_dir: str,
                            is_last: bool) -> None:
    import platform
    import os

    radio: MonitoringRadio
    if radio_mode == "udp":
        radio = UDPRadio(monitoring_hub_url,
                         source_id=task_id)
    elif radio_mode == "htex":
        radio = HTEXRadio(monitoring_hub_url,
                          source_id=task_id)
    elif radio_mode == "filesystem":
        radio = FilesystemRadio(monitoring_url=monitoring_hub_url,
                                source_id=task_id, run_dir=run_dir)
    else:
        raise RuntimeError(f"Unknown radio mode: {radio_mode}")

    msg = (MessageType.RESOURCE_INFO,
           {'run_id': run_id,
            'try_id': try_id,
            'task_id': task_id,
            'hostname': platform.node(),
            'block_id': os.environ.get('PARSL_WORKER_BLOCK_ID'),
            'first_msg': not is_last,
            'last_msg': is_last,
            'timestamp': datetime.datetime.now(),
            'pid': os.getpid()
    })
    radio.send(msg)
    return