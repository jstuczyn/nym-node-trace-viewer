"""Textual live dashboard for nym-node mixnet packet-latency stages."""

from __future__ import annotations

import time
from collections import deque

import httpx
from rich.table import Table
from rich.text import Text
from textual.app import App, ComposeResult
from textual.widgets import DataTable, Footer, Header, Static

from .scrape import BuildInfo, ScrapeError, fetch_build_info, scrape
from .stats import (
    CONNECTION_GAUGES,
    DELAY_DRAIN_BATCH_METRIC,
    DRAIN_BATCH_METRIC,
    DROP_COUNTERS,
    EGRESS_FILL_METRIC,
    STAGE_LABELS,
    THROUGHPUT_COUNTERS,
    TOTAL_METRIC,
    Snapshot,
    StageStats,
    compute_stage_stats,
    human_count,
    human_pct,
    human_rate,
    human_seconds,
    ordered_metric_names,
    pct_of_total,
    per_bucket_counts,
    sparkline,
)

COLUMNS = [
    ("stage", "Stage", 18),
    ("p50", "p50", 9),
    ("p90", "p90", 9),
    ("p99", "p99", 9),
    ("mean", "mean", 9),
    ("pct", "%tot", 6),
    ("count", "count", 7),
    ("rate", "rate", 8),
    ("over", ">6.55s", 7),
]

# how many p99 samples to retain per stage for the sparkline
HISTORY = 240


class TraceViewer(App):
    CSS_PATH = "app.tcss"
    TITLE = "nym mixnet trace viewer"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("c", "toggle_mode", "Window/Cumulative"),
        ("space", "toggle_pause", "Pause"),
        ("r", "refresh_now", "Refresh"),
    ]

    def __init__(self, url: str, token: str | None, interval: float, verify: bool):
        super().__init__()
        self.url = url
        self.token = token
        self.interval = interval
        self.verify = verify
        self.cumulative = False
        self.paused = False
        self._client: httpx.AsyncClient | None = None
        self._fetching = False
        self.prev: dict[str, Snapshot] = {}
        self.prev_at: float | None = None
        self.latest: dict[str, StageStats] = {}
        self.row_order: list[str] = []
        self.history: dict[str, deque] = {}  # metric -> recent p99 values (for the sparkline)
        self.runtime: dict[str, float] = {}  # latest nym_node_tokio_runtime_* gauges
        self.prev_runtime: dict[str, float] = {}
        self.rq_history: deque = deque(maxlen=HISTORY)  # global run-queue depth over time
        self.poll_rate: float | None = None
        self.counters: dict[str, float] = {}  # latest nym_node_mixnet_* counters
        self.prev_counters: dict[str, float] = {}
        self.counter_deltas: dict[str, float] = {}  # per-counter increase over the last window
        self.network: dict[str, float] = {}  # latest nym_node_network_* connection gauges
        self.conn_history: dict[str, deque] = {}  # per-gauge connection count over time
        self.last_error: str | None = None
        self.last_ok_at: float | None = None
        self.build_info: BuildInfo | None = None  # /api/v1/build-information, fetched once

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static("starting...", id="status")
        yield DataTable(id="waterfall", cursor_type="row", zebra_stripes=True)
        yield Static("", id="trend")
        yield Static("", id="dist")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#waterfall", DataTable)
        for key, label, width in COLUMNS:
            table.add_column(label, key=key, width=width)
        self._client = httpx.AsyncClient(timeout=5.0, verify=self.verify)
        self.set_interval(self.interval, self.tick)
        self.call_after_refresh(self.tick)

    async def on_unmount(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    # ------------------------------------------------------------------ actions

    def action_toggle_mode(self) -> None:
        self.cumulative = not self.cumulative
        self.history.clear()  # p99 means something different per mode; don't mix the series
        self.render_all()

    def action_toggle_pause(self) -> None:
        self.paused = not self.paused
        self.update_status()

    async def action_refresh_now(self) -> None:
        await self.tick(force=True)

    # ------------------------------------------------------------------ polling

    async def tick(self, force: bool = False) -> None:
        if self._client is None:
            return
        # build info is static; fetch it once (retry each tick until it succeeds) so the header can
        # confirm which binary is deployed. Done before the pause/in-flight guards so a paused
        # viewer still shows the version.
        if self.build_info is None:
            self.build_info = await fetch_build_info(self._client, self.url, self.token)
            if self.build_info is not None:
                bi = self.build_info
                self.sub_title = f"{bi.binary_name} v{bi.version} · {bi.short_sha} ({bi.commit_branch})"
        if self._fetching or (self.paused and not force):
            return
        self._fetching = True
        try:
            result = await scrape(self._client, self.url, self.token)
        except ScrapeError as e:
            self.last_error = str(e)
            self.update_status()
            return
        finally:
            self._fetching = False

        cur = result.histograms
        now = time.monotonic()
        elapsed = (now - self.prev_at) if self.prev_at is not None else 0.0
        self.latest = {
            metric: compute_stage_stats(
                metric, self.prev.get(metric), snap, elapsed, self.cumulative
            )
            for metric, snap in cur.items()
        }
        self.row_order = ordered_metric_names(list(cur.keys()))
        for metric, st in self.latest.items():
            self.history.setdefault(metric, deque(maxlen=HISTORY)).append(st.p99)

        # runtime gauges: track run-queue depth over time + derive a poll rate from the counter
        self.rq_history.append(result.runtime.get("nym_node_tokio_runtime_global_queue_depth"))
        prev_polls = self.prev_runtime.get("nym_node_tokio_runtime_worker_poll_count")
        cur_polls = result.runtime.get("nym_node_tokio_runtime_worker_poll_count")
        if prev_polls is not None and cur_polls is not None and elapsed > 0 and cur_polls >= prev_polls:
            self.poll_rate = (cur_polls - prev_polls) / elapsed
        else:
            self.poll_rate = None
        self.runtime = result.runtime
        self.prev_runtime = result.runtime

        # node counters: keep the cumulative value + the increase over this window
        self.counter_deltas = {
            k: v - self.prev_counters[k]
            for k, v in result.counters.items()
            if k in self.prev_counters and v >= self.prev_counters[k]
        }
        self.counters = result.counters
        self.prev_counters = result.counters

        # connection gauges: keep current value + a per-gauge history for the trend sparkline
        self.network = result.network
        for metric, _ in CONNECTION_GAUGES:
            if metric in result.network:
                self.conn_history.setdefault(metric, deque(maxlen=HISTORY)).append(
                    result.network[metric]
                )

        self.prev = cur
        self.prev_at = now
        self.last_error = None
        self.last_ok_at = now
        self.render_all()

    # ------------------------------------------------------------------ render

    def render_all(self) -> None:
        self.update_status()
        self.update_table()
        self.update_trend()
        self.update_dist()

    def update_status(self) -> None:
        mode = "CUMULATIVE (since boot)" if self.cumulative else f"WINDOW (~{self.interval:g}s)"
        status = Text()
        status.append(f"{self.url}\n")
        if self.build_info is not None:
            bi = self.build_info
            status.append("build: ", style="bold")
            status.append(f"v{bi.version}  ", style="bold green")
            status.append(f"{bi.short_sha}  ", style="green")
            status.append(f"({bi.commit_branch})", style="cyan")
            status.append(f"  built {bi.built}\n", style="dim")
        status.append("mode: ", style="bold")
        status.append(mode)
        status.append("   poll: ", style="bold")
        status.append("PAUSED" if self.paused else f"{self.interval:g}s")
        total = self.latest.get(TOTAL_METRIC)
        if total is not None:
            status.append("   sampled: ", style="bold")
            status.append(f"{human_count(total.count)} ")
            status.append(f"({human_rate(total.rate)})" if total.rate is not None else "")
        # forwarder drain-batch readouts (count histograms, not latency stages). p50/p99 > 1
        # confirms batching is engaging. Absent on nodes without the metric.
        def drain_line(label, metric):
            d = self.latest.get(metric)
            if d is None:
                return
            c = lambda v: human_count(v) if v is not None else "-"  # noqa: E731
            mean = f"{d.mean:.1f}" if d.mean is not None else "-"
            status.append(f"\n{label}: ", style="bold")
            status.append(
                f"p50 {c(d.p50)}  p90 {c(d.p90)}  p99 {c(d.p99)}  "
                f"mean {mean}  ·  wakeups {human_count(d.count)} ({human_rate(d.rate)})"
            )

        drain_line("ingress drain", DRAIN_BATCH_METRIC)
        drain_line("delay drain", DELAY_DRAIN_BATCH_METRIC)
        # egress buffer fill ratio (count histogram). p99 near 100% means a peer's send buffer is
        # near full -> packets to it about to drop. Absent on nodes without the metric.
        egress = self.latest.get(EGRESS_FILL_METRIC)
        if egress is not None:
            pct = lambda v: f"{v * 100:.0f}%" if v is not None else "-"  # noqa: E731
            status.append("\negress buffer fill: ", style="bold")
            status.append(
                f"p50 {pct(egress.p50)}  p90 {pct(egress.p90)}  p99 {pct(egress.p99)}  "
                f"mean {pct(egress.mean)}  ·  sends {human_count(egress.count)}"
            )
        # tokio runtime gauges. run-queue depth > 0 = tasks waiting to be scheduled (the spike
        # signal); busy ratio + poll rate appear only on tokio_unstable builds.
        rt = self.runtime
        if rt:
            status.append("\nruntime: ", style="bold")
            bits = []
            if "nym_node_tokio_runtime_num_workers" in rt:
                bits.append(f"workers {int(rt['nym_node_tokio_runtime_num_workers'])}")
            if "nym_node_tokio_runtime_alive_tasks" in rt:
                bits.append(f"tasks {human_count(rt['nym_node_tokio_runtime_alive_tasks'])}")
            status.append("  ·  ".join(bits))
            rq = rt.get("nym_node_tokio_runtime_global_queue_depth")
            if rq is not None:
                status.append("  ·  run-queue ")
                status.append(f"{int(rq)} ", style="bold red" if rq > 0 else "bold green")
                spark = sparkline(list(self.rq_history), 16)
                if spark:
                    status.append(spark + " ", style="yellow")
            if "nym_node_tokio_runtime_busy_ratio" in rt:
                status.append(f" ·  busy {rt['nym_node_tokio_runtime_busy_ratio'] * 100:.0f}%")
            if self.poll_rate is not None:
                status.append(f"  ·  {human_rate(self.poll_rate)} polls")
        # node drop/throughput counters (cumulative since boot, + increase over the window).
        # Drops never reach the latency histograms (a dropped packet records no stage), so this
        # is the only place they surface. Red = nonzero.
        if self.counters:
            status.append("\ndrops: ", style="bold")
            shown = False
            for metric, label in DROP_COUNTERS:
                val = self.counters.get(metric)
                if val is None:
                    continue
                if shown:
                    status.append("  ·  ")
                shown = True
                status.append(f"{label} ")
                status.append(human_count(val), style="bold red" if val > 0 else "dim")
                delta = self.counter_deltas.get(metric, 0)
                if delta > 0:
                    status.append(f" (+{human_count(delta)})", style="red")
            tput = []
            for metric, label in THROUGHPUT_COUNTERS:
                v = self.counters.get(metric)
                if v is None:
                    continue
                d = self.counter_deltas.get(metric, 0)
                tput.append(f"{label} {human_count(v)}" + (f" (+{human_count(d)})" if d > 0 else ""))
            if tput:
                status.append("\nforwarded: ", style="bold")
                status.append("  ·  ".join(tput))
        # connection-count gauges (instantaneous). The trend sparkline distinguishes a leak
        # (steadily rising) from normal topology connectivity (plateau); ws should be 0 on a mixnode.
        if self.network:
            status.append("\nconns: ", style="bold")
            shown = False
            for metric, label in CONNECTION_GAUGES:
                v = self.network.get(metric)
                if v is None:
                    continue
                if shown:
                    status.append("  ·  ")
                shown = True
                status.append(f"{label} ")
                status.append(human_count(v), style="bold")
                hist = list(self.conn_history.get(metric, []))
                if any(hist):  # skip the flat-zero sparkline (e.g. ws on a mixnode)
                    spark = sparkline(hist, 16)
                    if spark:
                        status.append(" " + spark, style="cyan")
        if self.last_error:
            status.append("\nERROR: ", style="bold red")
            status.append(self.last_error, style="red")
        self.query_one("#status", Static).update(status)

    def update_table(self) -> None:
        table = self.query_one("#waterfall", DataTable)
        total = self.latest.get(TOTAL_METRIC)
        total_mean = total.mean if total else None
        for metric in self.row_order:
            st = self.latest.get(metric)
            if st is None:
                continue
            cells = [
                Text(st.label, style="bold"),
                human_seconds(st.p50),
                human_seconds(st.p90),
                human_seconds(st.p99),
                human_seconds(st.mean),
                human_pct(pct_of_total(st.mean, total_mean)),
                human_count(st.count),
                human_rate(st.rate),
                self._overflow_cell(st),
            ]
            if metric in table.rows:
                for (key, _, _), value in zip(COLUMNS, cells):
                    table.update_cell(metric, key, value)
            else:
                table.add_row(*cells, key=metric)

    @staticmethod
    def _overflow_cell(st: StageStats) -> Text:
        if st.overflow <= 0:
            return Text("0")
        return Text(human_count(st.overflow), style="bold yellow")

    def update_trend(self) -> None:
        panel = self.query_one("#trend", Static)
        if not self.row_order:
            panel.update("p99 over time: waiting for data...")
            return
        table = self.query_one("#waterfall", DataTable)
        idx = min(table.cursor_row, len(self.row_order) - 1)
        selected = self.row_order[idx]
        # always show Total, plus the selected stage (if different)
        metrics = [TOTAL_METRIC] if TOTAL_METRIC in self.history else []
        if selected not in metrics:
            metrics.append(selected)
        width = max(20, panel.size.width - 52)
        mode = "cumulative" if self.cumulative else "window"
        body = Text()
        body.append(f"p99 over time ({mode})\n", style="bold")
        for metric in metrics:
            hist = list(self.history.get(metric, []))
            vals = [v for v in hist if v is not None]
            body.append(f"{STAGE_LABELS.get(metric, metric):<17}", style="cyan")
            body.append(sparkline(hist, width) + "  ", style="green")
            if vals:
                body.append(
                    f"cur {human_seconds(vals[-1])}  "
                    f"min {human_seconds(min(vals))}  max {human_seconds(max(vals))}  n={len(vals)}"
                )
            else:
                body.append("no samples in window", style="dim")
            body.append("\n")
        panel.update(body)

    def update_dist(self) -> None:
        panel = self.query_one("#dist", Static)
        table = self.query_one("#waterfall", DataTable)
        if not self.row_order:
            panel.update("waiting for data...")
            return
        idx = min(table.cursor_row, len(self.row_order) - 1)
        metric = self.row_order[idx]
        st = self.latest.get(metric)
        if st is None:
            panel.update("")
            return
        # rebuild this stage's bucket distribution from the active snapshot/window
        snap = self.prev.get(metric)
        if snap is None:
            panel.update("")
            return
        # NOTE: in WINDOW mode we no longer hold the delta here; show the live snapshot's shape.
        # (Per-bucket window deltas would need caching the previous buckets; the cumulative shape
        #  is still a useful distribution overview. Percentiles in the table reflect the window.)
        counts = per_bucket_counts(snap.sorted_buckets())
        panel.update(self._dist_render(st.label, counts))

    @staticmethod
    def _dist_render(label: str, counts: list[tuple[float, float]]) -> Table:
        t = Table(title=f"distribution: {label} (cumulative bucket shape)", expand=True)
        t.add_column("≤ le", justify="right", style="cyan", no_wrap=True)
        t.add_column("count", justify="right", no_wrap=True)
        t.add_column("", ratio=1)
        peak = max((c for _, c in counts), default=0.0)
        for upper, c in counts:
            bound = "+Inf" if upper == float("inf") else human_seconds(upper)
            width = int((c / peak) * 40) if peak > 0 else 0
            bar = Text("█" * width, style="green" if upper != float("inf") else "yellow")
            t.add_row(bound, human_count(c), bar)
        return t

    def on_data_table_row_highlighted(self, _: DataTable.RowHighlighted) -> None:
        self.update_trend()
        self.update_dist()
