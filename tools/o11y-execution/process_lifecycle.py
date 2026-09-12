"""Collect per-child lifetime usage; never label aggregate child usage as one PID."""
import os
import time
from compare import process_snapshot


def stop(child, timeout=30):
    before = process_snapshot(child.pid)
    started = time.perf_counter_ns()
    forced = False
    usage = None
    # Popen.poll()/wait() may already have reaped an exited child. Its usage
    # cannot subsequently be recovered with wait4, and is not a measured zero.
    if child.returncode is None:
        child.terminate()
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            try:
                pid, status, collected = os.wait4(child.pid, os.WNOHANG)
            except ChildProcessError:
                break
            if pid:
                child.returncode = os.waitstatus_to_exitcode(status)
                usage = collected
                break
            if deadline is not None and time.monotonic() >= deadline:
                forced = True
                child.kill()
                try:
                    _, status, usage = os.wait4(child.pid, 0)
                    child.returncode = os.waitstatus_to_exitcode(status)
                except ChildProcessError:
                    pass
                break
            time.sleep(.01)
    return {'before_shutdown': before,
            'lifetime_cpu_ns': int((usage.ru_utime + usage.ru_stime) * 1e9) if usage is not None else None,
            'lifetime_peak_rss_bytes': usage.ru_maxrss * 1024 if usage is not None else None,
            'usage_unavailable_reason': 'child was already reaped; per-PID wait4 usage unavailable' if usage is None else None,
            'shutdown_wall_ns': time.perf_counter_ns() - started,
            'forced_kill': forced, 'exit_code': child.returncode,
            'scope': 'Per-PID wait4 CPU and peak RSS cover the whole child lifetime, not shutdown only. Process exit is not a measured summary retirement operation.'}
