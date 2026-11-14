"""Counter parsing contracts used to distinguish storage stalls from CPU waits."""
import unittest

from host_monitor import disk_delta, disk_stats, pressure, process_stat


class HostCounterTests(unittest.TestCase):
    def test_old_and_new_disk_fields_and_flush_latency(self):
        old = "8 1 sda1 1 0 8 2 3 0 16 4 0 4 4"
        new = "8 0 sda 1 0 8 2 3 0 16 4 0 4 4 0 0 0 0 2 200"
        values = disk_stats(old + "\n" + new, {"sda", "sda1"})
        self.assertNotIn("flush_ms", values["sda1"])
        before = {**values["sda"], "flushes": 1, "flush_ms": 50}
        rates = disk_delta(before, values["sda"], 2)
        self.assertEqual(rates["flush_await_ms"], 150)
        self.assertIsNone(rates["write_await_ms"])
        self.assertEqual(disk_delta(values["sda"], before, 1)["flush_await_ms"], None)

    def test_pressure_keeps_microsecond_counters(self):
        parsed = pressure("some avg10=1.20 avg60=0.50 avg300=0.20 total=900000\nfull avg10=0.01 avg60=0.01 avg300=0.01 total=25")
        self.assertEqual(parsed["some"]["total"], 900000)
        self.assertEqual(parsed["full"]["avg10"], .01)

    def test_process_names_and_pid_reuse_identity(self):
        fields = ["S"] + ["0"] * 49
        fields[11], fields[12], fields[19], fields[21], fields[39] = "7", "3", "987", "23", "6"
        value = process_stat("123 (worker ) with spaces) " + " ".join(fields))
        self.assertEqual(value, {"state": "S", "cpu_ticks": 10, "start_ticks": 987, "rss_pages": 23, "block_io_ticks": 6})


if __name__ == "__main__":
    unittest.main()
