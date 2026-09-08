import subprocess
import sys
import unittest
from process_lifecycle import stop


class LifecycleTests(unittest.TestCase):
    def test_lifetime_wait4_not_shutdown_delta(self):
        child = subprocess.Popen([sys.executable, '-u', '-c',
            'import time; end=time.process_time()+.05\nwhile time.process_time()<end: pass\nprint("ready", flush=True)\ntime.sleep(20)'], stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), 'ready')
            usage = stop(child)
            self.assertGreater(usage['lifetime_cpu_ns'], 30_000_000)
            self.assertGreater(usage['lifetime_peak_rss_bytes'], 0)
            self.assertFalse(usage['forced_kill'])
            self.assertIsNone(usage['usage_unavailable_reason'])
        finally:
            if child.returncode is None:
                child.kill(); child.wait()
            child.stdout.close()

    def test_already_reaped_is_unknown_not_zero(self):
        child = subprocess.Popen([sys.executable, '-c', 'pass'])
        child.wait()
        self.assertIsNone(stop(child)['lifetime_cpu_ns'])

    def test_forced_kill_is_recorded(self):
        child = subprocess.Popen([sys.executable, '-u', '-c',
            'import signal,time;signal.signal(signal.SIGTERM, signal.SIG_IGN);print("ready",flush=True);time.sleep(20)'], stdout=subprocess.PIPE, text=True)
        try:
            child.stdout.readline()
            self.assertTrue(stop(child, timeout=.01)['forced_kill'])
            self.assertIsNotNone(child.returncode)
        finally:
            if child.returncode is None:
                child.kill(); child.wait()
            child.stdout.close()
