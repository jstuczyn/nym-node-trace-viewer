"""Pure (UI-free) logic: stage definitions, prometheus-histogram math, formatting.

Kept free of Textual/httpx so it can be unit-tested and reused offline.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

# The mixnet_packet_* family, in pipeline (waterfall) order. `label` is what we show;
# the key is the prometheus metric name emitted by TraceStage::as_ref() in the node.
STAGE_DEFS: list[tuple[str, str]] = [
    ("mixnet_packet_stage_unwrap_seconds", "Unwrap"),
    ("mixnet_packet_stage_replay_check_seconds", "ReplayCheck"),
    ("mixnet_packet_stage_forwarder_queue_seconds", "ForwarderQueue"),
    ("mixnet_packet_stage_delay_queue_seconds", "DelayQueue"),
    ("mixnet_packet_stage_delay_queue_overrun_seconds", "DelayQueueOverrun"),
    ("mixnet_packet_stage_egress_queue_seconds", "EgressQueue"),
    ("mixnet_packet_stage_socket_write_seconds", "SocketWrite"),
    ("mixnet_packet_total_latency_seconds", "Total"),
]

STAGE_LABELS: dict[str, str] = dict(STAGE_DEFS)

TOTAL_METRIC = "mixnet_packet_total_latency_seconds"

# non-latency histograms (not pipeline stages) - kept out of the latency waterfall, shown separately
DRAIN_BATCH_METRIC = "mixnet_packet_forwarder_drain_batch_size"
DELAY_DRAIN_BATCH_METRIC = "mixnet_packet_forwarder_delay_drain_batch_size"
EGRESS_FILL_METRIC = "mixnet_packet_egress_buffer_fill_ratio"

# nym_node_mixnet_* counters (cumulative since boot). Drops are invisible to the latency
# histograms - a dropped packet never records a stage - so these are the only way to see them.
NODE_COUNTER_PREFIX = "nym_node_mixnet_"

DROP_COUNTERS = [
    ("nym_node_mixnet_egress_forward_packets_dropped", "egress-fwd"),
    ("nym_node_mixnet_ingress_forward_packets_dropped", "ingress-fwd"),
    ("nym_node_mixnet_ingress_final_hop_packets_dropped", "final-hop"),
    ("nym_node_mixnet_ingress_replayed_packets_received", "replayed"),
    ("nym_node_mixnet_ingress_malformed_packets_received", "malformed"),
    ("nym_node_mixnet_ingress_excessive_delay_packets_received", "excess-delay"),
]

THROUGHPUT_COUNTERS = [
    ("nym_node_mixnet_ingress_forward_packets_received", "recv"),
    ("nym_node_mixnet_egress_forward_packets_sent", "sent"),
]

# nym_node_network_* connection-count gauges (instantaneous, like the nym_node_tokio_runtime_* ones).
# Watching ingress/egress mixnet counts over time distinguishes a leak (unbounded growth) from
# normal persistent connectivity to topology neighbours (plateau near the peer count).
NETWORK_GAUGE_PREFIX = "nym_node_network_"

CONNECTION_GAUGES = [
    ("nym_node_network_active_ingress_mixnet_connections", "ingress-mix"),
    ("nym_node_network_active_egress_mixnet_connections", "egress-mix"),
    ("nym_node_network_active_ingress_web_socket_connections", "ws"),
    # cumulative (not instantaneous): total ingress connections reaped by the idle timeout.
    # A rising count alongside a plateauing ingress-mix gauge = reaping is bounding accumulation.
    ("nym_node_network_idle_closed_ingress_mixnet_connections", "idle-closed"),
]

SPARK_CHARS = "▁▂▃▄▅▆▇█"


@dataclass
class Snapshot:
    """One scrape of a single histogram: cumulative-since-boot bucket counts, sum, count."""

    # le upper-bound (float, +Inf -> math.inf) -> cumulative observation count
    buckets: dict[float, float] = field(default_factory=dict)
    sum: float = 0.0
    count: float = 0.0

    def sorted_buckets(self) -> list[tuple[float, float]]:
        """(upper_bound, cumulative_count) ascending by upper bound; last is +Inf."""
        return sorted(self.buckets.items(), key=lambda kv: kv[0])


@dataclass
class StageStats:
    """Display-ready stats for one stage over the chosen window (delta or cumulative)."""

    metric: str
    label: str
    p50: float | None
    p90: float | None
    p99: float | None
    mean: float | None
    count: float          # observations in the window
    rate: float | None    # observations/sec (None in cumulative mode)
    overflow: float       # observations above the top finite bucket (> ~0.82s)


def _delta_buckets(prev: Snapshot, cur: Snapshot) -> tuple[dict[float, float], float, float]:
    """Per-le cumulative-count delta between two cumulative snapshots, plus sum/count delta.

    A monotonic-cumulative histogram stays cumulative under subtraction, so the result can be
    fed straight into `quantile`. Detects a counter reset (node restart) and falls back to the
    current snapshot as-is.
    """
    if cur.count < prev.count:  # counters went backwards -> node restarted; treat prev as zero
        return dict(cur.buckets), cur.sum, cur.count
    delta = {le: cum - prev.buckets.get(le, 0.0) for le, cum in cur.buckets.items()}
    return delta, cur.sum - prev.sum, cur.count - prev.count


def quantile(sorted_buckets: list[tuple[float, float]], q: float) -> float | None:
    """Prometheus-style histogram_quantile: linear interpolation within the matched bucket.

    `sorted_buckets` is ascending (upper_bound, cumulative_count) with +Inf last. Returns the
    top finite bound when the quantile falls in the +Inf bucket (we can't interpolate to inf).
    """
    if not sorted_buckets:
        return None
    total = sorted_buckets[-1][1]
    if total <= 0:
        return None
    rank = q * total
    prev_bound = 0.0
    prev_cum = 0.0
    for upper, cum in sorted_buckets:
        if cum >= rank:
            if math.isinf(upper):
                return prev_bound or None  # falls beyond the top finite bucket
            in_bucket = cum - prev_cum
            if in_bucket <= 0:
                return upper
            frac = (rank - prev_cum) / in_bucket
            return prev_bound + (upper - prev_bound) * frac
        prev_bound = upper
        prev_cum = cum
    return sorted_buckets[-1][0]


def overflow_count(sorted_buckets: list[tuple[float, float]]) -> float:
    """Observations that landed above the top finite bucket (i.e. in +Inf)."""
    if not sorted_buckets:
        return 0.0
    total = sorted_buckets[-1][1]
    finite_cum = 0.0
    for upper, cum in sorted_buckets:
        if math.isinf(upper):
            break
        finite_cum = cum
    return max(0.0, total - finite_cum)


def per_bucket_counts(sorted_buckets: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """De-cumulate: (upper_bound, count_in_just_this_bucket)."""
    out = []
    prev = 0.0
    for upper, cum in sorted_buckets:
        out.append((upper, max(0.0, cum - prev)))
        prev = cum
    return out


def compute_stage_stats(
    metric: str,
    prev: Snapshot | None,
    cur: Snapshot,
    elapsed: float,
    cumulative: bool,
) -> StageStats:
    """Build display stats for a stage. Window mode diffs against `prev`; cumulative uses `cur`."""
    if cumulative or prev is None:
        buckets = cur.sorted_buckets()
        count = cur.count
        total_sum = cur.sum
        rate = None
    else:
        dbuckets, total_sum, count = _delta_buckets(prev, cur)
        buckets = sorted(dbuckets.items(), key=lambda kv: kv[0])
        rate = (count / elapsed) if elapsed > 0 else None
    mean = (total_sum / count) if count > 0 else None
    return StageStats(
        metric=metric,
        label=STAGE_LABELS.get(metric, metric),
        p50=quantile(buckets, 0.50),
        p90=quantile(buckets, 0.90),
        p99=quantile(buckets, 0.99),
        mean=mean,
        count=count,
        rate=rate,
        overflow=overflow_count(buckets),
    )


# non-latency metrics that must not appear in the latency waterfall
NON_STAGE_METRICS = {DRAIN_BATCH_METRIC, DELAY_DRAIN_BATCH_METRIC, EGRESS_FILL_METRIC}


def ordered_metric_names(present: list[str]) -> list[str]:
    """Canonical latency stages first (pipeline order), then any unexpected mixnet_packet_*
    stage extras. Excludes known non-latency metrics (e.g. the drain-batch count histogram)."""
    known = [m for m, _ in STAGE_DEFS if m in present]
    extras = sorted(
        m for m in present if m not in STAGE_LABELS and m not in NON_STAGE_METRICS
    )
    return known + extras


def human_seconds(s: float | None) -> str:
    if s is None:
        return "-"
    if s < 1e-3:
        return f"{s * 1e6:.0f}µs"
    if s < 1:
        return f"{s * 1e3:.2f}ms"
    return f"{s:.3f}s"


def human_count(n: float) -> str:
    n = int(round(n))
    if n < 1000:
        return str(n)
    if n < 1_000_000:
        return f"{n / 1000:.1f}k"
    return f"{n / 1_000_000:.2f}M"


def human_rate(r: float | None) -> str:
    if r is None:
        return "-"
    if r < 1000:
        return f"{r:.1f}/s"
    return f"{r / 1000:.1f}k/s"


def pct_of_total(mean: float | None, total_mean: float | None) -> float | None:
    """A stage's share of end-to-end mean latency. Means are additive across the sequential
    stages, so these are meaningful (and roughly sum to 100% for the in-path stages)."""
    if mean is None or not total_mean:
        return None
    return mean / total_mean * 100.0


def human_pct(p: float | None) -> str:
    if p is None:
        return "-"
    if p >= 10:
        return f"{p:.0f}%"
    return f"{p:.1f}%"


def sparkline(values: list[float | None], width: int | None = None) -> str:
    """Unicode block sparkline over the (non-None) tail of `values`, scaled to its own min/max."""
    vals = [v for v in values if v is not None]
    if width is not None:
        vals = vals[-width:]
    if not vals:
        return ""
    lo, hi = min(vals), max(vals)
    if hi <= lo:  # flat line: mid-height (or floor if all zero)
        return SPARK_CHARS[0 if hi == 0 else len(SPARK_CHARS) // 2] * len(vals)
    span = hi - lo
    last = len(SPARK_CHARS) - 1
    return "".join(SPARK_CHARS[int((v - lo) / span * last)] for v in vals)
