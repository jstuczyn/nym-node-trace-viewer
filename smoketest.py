"""Headless smoke test of the Textual app against a local fake prometheus endpoint.

Serves a synthetic mixnet_packet_* exposition (counts grow between scrapes so the WINDOW
delta is non-trivial), drives the app with Textual's test pilot, and asserts the waterfall
table populates and mode-toggle works. Run: python smoketest.py
"""

import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from trace_viewer.app import TraceViewer
from trace_viewer.scrape import DEFAULT_PATH, build_url
from trace_viewer.stats import STAGE_DEFS

BUCKETS = [0.0001, 0.0002, 0.0004, 0.0008, 0.0016, 0.0032, 0.0064, 0.0128,
           0.0256, 0.0512, 0.1024, 0.2048, 0.4096, 0.8192]

# mutable so successive scrapes return monotonically growing counters (simulates live traffic)
_scale = {"n": 0}


DRAIN_METRIC = "mixnet_packet_forwarder_drain_batch_size"
DELAY_DRAIN_METRIC = "mixnet_packet_forwarder_delay_drain_batch_size"
DRAIN_BUCKETS = [1, 2, 4, 8, 16, 32, 64, 128, 256]
EGRESS_METRIC = "mixnet_packet_egress_buffer_fill_ratio"
EGRESS_BUCKETS = [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0]


def exposition() -> str:
    n = _scale["n"]
    lines = []
    for i, (metric, _) in enumerate(STAGE_DEFS):
        lines.append(f"# HELP {metric} help")
        lines.append(f"# TYPE {metric} histogram")
        cum = 0
        for b in BUCKETS:
            cum += (i + 1) * 2 * n  # different shape per stage
            lines.append(f'{metric}_bucket{{le="{b}"}} {cum}')
        cum += i * n  # some overflow into +Inf for later stages
        lines.append(f'{metric}_bucket{{le="+Inf"}} {cum}')
        lines.append(f"{metric}_sum {0.001 * cum}")
        lines.append(f"{metric}_count {cum}")
    # the forwarder drain-batch count histograms (ingress + delay; mass in the high buckets, i.e.
    # batching is engaging) so the TUI's drain readouts have something to show
    for drain_name in (DRAIN_METRIC, DELAY_DRAIN_METRIC):
        lines.append(f"# TYPE {drain_name} histogram")
        cum = 0
        for b in DRAIN_BUCKETS:
            if b >= 64:
                cum += 10 * n
            lines.append(f'{drain_name}_bucket{{le="{b}"}} {cum}')
        lines.append(f'{drain_name}_bucket{{le="+Inf"}} {cum}')
        lines.append(f"{drain_name}_sum {200 * n}")
        lines.append(f"{drain_name}_count {cum}")
    # egress buffer fill-ratio histogram (mass around half-full)
    lines.append(f"# TYPE {EGRESS_METRIC} histogram")
    cum = 0
    for b in EGRESS_BUCKETS:
        if b >= 0.5:
            cum += 5 * n
        lines.append(f'{EGRESS_METRIC}_bucket{{le="{b}"}} {cum}')
    lines.append(f'{EGRESS_METRIC}_bucket{{le="+Inf"}} {cum}')
    lines.append(f"{EGRESS_METRIC}_sum {0.5 * cum}")
    lines.append(f"{EGRESS_METRIC}_count {cum}")
    # tokio runtime gauges (stable + the tokio_unstable-only ones)
    lines.append("# TYPE nym_node_tokio_runtime_num_workers gauge")
    lines.append("nym_node_tokio_runtime_num_workers 8")
    lines.append("# TYPE nym_node_tokio_runtime_alive_tasks gauge")
    lines.append(f"nym_node_tokio_runtime_alive_tasks {100 + n}")
    lines.append("# TYPE nym_node_tokio_runtime_global_queue_depth gauge")
    lines.append(f"nym_node_tokio_runtime_global_queue_depth {n % 5}")
    lines.append("# TYPE nym_node_tokio_runtime_busy_ratio gauge")
    lines.append("nym_node_tokio_runtime_busy_ratio 0.42")
    lines.append("# TYPE nym_node_tokio_runtime_worker_poll_count gauge")
    lines.append(f"nym_node_tokio_runtime_worker_poll_count {1000 * n}")
    # node counters, including a _rate variant that must be skipped by the parser
    lines.append("# TYPE nym_node_mixnet_egress_forward_packets_dropped gauge")
    lines.append(f"nym_node_mixnet_egress_forward_packets_dropped {3 * n}")
    lines.append("# TYPE nym_node_mixnet_egress_forward_packets_dropped_rate gauge")
    lines.append("nym_node_mixnet_egress_forward_packets_dropped_rate 0.5")
    lines.append("# TYPE nym_node_mixnet_ingress_forward_packets_received gauge")
    lines.append(f"nym_node_mixnet_ingress_forward_packets_received {500 * n}")
    lines.append("# TYPE nym_node_mixnet_egress_forward_packets_sent gauge")
    lines.append(f"nym_node_mixnet_egress_forward_packets_sent {500 * n}")
    # connection-count gauges (grow with n so the trend sparkline has movement)
    lines.append("# TYPE nym_node_network_active_ingress_mixnet_connections gauge")
    lines.append(f"nym_node_network_active_ingress_mixnet_connections {10 * n}")
    lines.append("# TYPE nym_node_network_active_egress_mixnet_connections gauge")
    lines.append(f"nym_node_network_active_egress_mixnet_connections {9 * n}")
    lines.append("# TYPE nym_node_network_active_ingress_web_socket_connections gauge")
    lines.append("nym_node_network_active_ingress_web_socket_connections 0")
    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if not self.path.startswith(DEFAULT_PATH):
            self.send_response(404)
            self.end_headers()
            return
        _scale["n"] += 1  # counters grow on every scrape, like live traffic
        body = exposition().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


async def drive(port: int) -> None:
    app = TraceViewer(url=build_url(f"http://127.0.0.1:{port}"), token="x", interval=0.2, verify=True)
    async with app.run_test() as pilot:
        await pilot.pause(0.4)          # first scrape (establishes baseline)
        await pilot.pause(0.4)          # further scrapes -> window deltas computed
        from textual.widgets import DataTable

        table = app.query_one("#waterfall", DataTable)
        # drain metrics (ingress + delay) must NOT be waterfall rows (counts, not latency stages)
        assert table.row_count == len(STAGE_DEFS), f"rows={table.row_count}"
        assert app.last_error is None, app.last_error
        total = app.latest["mixnet_packet_total_latency_seconds"]
        assert total.rate and total.rate > 0, f"rate={total.rate}"
        assert total.p50 is not None
        # both drain readouts present and reflecting batching (mass in the high buckets)
        drain = app.latest[DRAIN_METRIC]
        assert drain.p99 and drain.p99 >= 64, f"drain p99={drain.p99}"
        delay_drain = app.latest[DELAY_DRAIN_METRIC]
        assert delay_drain.p99 and delay_drain.p99 >= 64, f"delay drain p99={delay_drain.p99}"
        # egress fill ratio parsed but kept out of the waterfall
        assert EGRESS_METRIC in app.latest, "egress metric missing from latest"
        # runtime gauges parsed and stored
        assert app.runtime.get("nym_node_tokio_runtime_num_workers") == 8, app.runtime
        assert "nym_node_tokio_runtime_global_queue_depth" in app.runtime, app.runtime
        # node counters parsed; the _rate variant must be excluded; window delta computed
        assert "nym_node_mixnet_egress_forward_packets_dropped" in app.counters, app.counters
        assert "nym_node_mixnet_egress_forward_packets_dropped_rate" not in app.counters, app.counters
        assert app.counter_deltas.get("nym_node_mixnet_egress_forward_packets_dropped", 0) > 0
        # connection gauges parsed + history tracked for the sparkline
        assert app.network.get("nym_node_network_active_ingress_mixnet_connections", 0) > 0, app.network
        assert len(app.conn_history.get("nym_node_network_active_egress_mixnet_connections", [])) >= 2
        # toggle to cumulative and back, ensure no crash
        await pilot.press("c")
        await pilot.pause(0.1)
        await pilot.press("c")
        await pilot.pause(0.1)
        print(f"ok  rows={table.row_count}  total p50={total.p50:.6f}s rate={total.rate:.1f}/s")


def main():
    server = HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        asyncio.run(drive(port))
    finally:
        server.shutdown()
    print("smoke ok")


if __name__ == "__main__":
    main()
