"""Offline sanity checks for the stats math + prometheus parsing. Run: python selftest.py"""

from trace_viewer.scrape import (
    BUILD_INFO_PATH,
    BuildInfo,
    build_url,
    parse_mixnet_packet,
    parse_network_gauges,
    parse_node_counters,
    parse_runtime_gauges,
)
from trace_viewer.stats import (
    SPARK_CHARS,
    Snapshot,
    compute_stage_stats,
    overflow_count,
    pct_of_total,
    quantile,
    sparkline,
)

INF = float("inf")


def approx(a, b, tol=1e-9):
    return abs(a - b) <= tol


def test_quantile_interpolation():
    buckets = [(0.001, 0.0), (0.002, 50.0), (0.004, 100.0), (INF, 100.0)]
    assert approx(quantile(buckets, 0.50), 0.002), quantile(buckets, 0.50)
    assert approx(quantile(buckets, 0.90), 0.0036), quantile(buckets, 0.90)
    assert quantile([], 0.5) is None
    assert quantile([(0.001, 0.0), (INF, 0.0)], 0.5) is None  # empty histogram


def test_overflow_and_inf_quantile():
    buckets = [(0.001, 10.0), (0.004, 100.0), (INF, 120.0)]
    assert approx(overflow_count(buckets), 20.0)
    # p99 rank = 118.8 -> lands in +Inf bucket -> returns top finite bound
    assert approx(quantile(buckets, 0.99), 0.004), quantile(buckets, 0.99)


def test_delta_and_reset():
    prev = Snapshot(buckets={0.001: 5.0, 0.002: 10.0, INF: 10.0}, sum=0.01, count=10.0)
    cur = Snapshot(buckets={0.001: 8.0, 0.002: 20.0, INF: 20.0}, sum=0.03, count=20.0)
    st = compute_stage_stats("m", prev, cur, elapsed=2.0, cumulative=False)
    assert approx(st.count, 10.0), st.count          # 20 - 10
    assert approx(st.rate, 5.0), st.rate             # 10 / 2s
    assert approx(st.mean, 0.02 / 10.0), st.mean     # delta_sum / delta_count
    # reset: cur.count < prev.count -> treat prev as zero, use cur as-is
    st_reset = compute_stage_stats("m", cur, prev, elapsed=2.0, cumulative=False)
    assert approx(st_reset.count, prev.count), st_reset.count


def test_pct_of_total():
    assert approx(pct_of_total(0.05, 0.2), 25.0)
    assert pct_of_total(None, 0.2) is None
    assert pct_of_total(0.05, None) is None
    assert pct_of_total(0.05, 0.0) is None


def test_sparkline():
    assert sparkline([]) == ""
    assert sparkline([None, None]) == ""
    # ascending values -> first char lowest, last char highest
    s = sparkline([1.0, 2.0, 3.0, 4.0])
    assert s[0] == SPARK_CHARS[0] and s[-1] == SPARK_CHARS[-1], s
    # flat non-zero -> mid; width clamps to the tail
    assert set(sparkline([5.0, 5.0, 5.0])) == {SPARK_CHARS[len(SPARK_CHARS) // 2]}
    assert len(sparkline([1.0, 2.0, 3.0, 4.0, 5.0], width=3)) == 3
    # Nones are skipped, not rendered as a level
    assert len(sparkline([1.0, None, 2.0])) == 2


SAMPLE = """\
# HELP mixnet_packet_stage_unwrap_seconds Seconds spent unwrapping a received sphinx packet
# TYPE mixnet_packet_stage_unwrap_seconds histogram
mixnet_packet_stage_unwrap_seconds_bucket{le="0.0001"} 3
mixnet_packet_stage_unwrap_seconds_bucket{le="0.0002"} 7
mixnet_packet_stage_unwrap_seconds_bucket{le="+Inf"} 10
mixnet_packet_stage_unwrap_seconds_sum 0.0011
mixnet_packet_stage_unwrap_seconds_count 10
# HELP some_other_metric unrelated
# TYPE some_other_metric counter
some_other_metric_total 42
"""


def test_parse():
    snaps = parse_mixnet_packet(SAMPLE)
    assert set(snaps) == {"mixnet_packet_stage_unwrap_seconds"}, set(snaps)
    s = snaps["mixnet_packet_stage_unwrap_seconds"]
    assert s.count == 10.0
    assert s.buckets[INF] == 10.0
    assert s.buckets[0.0001] == 3.0


RUNTIME_SAMPLE = """\
# TYPE nym_node_tokio_runtime_global_queue_depth gauge
nym_node_tokio_runtime_global_queue_depth 3
# TYPE nym_node_tokio_runtime_busy_ratio gauge
nym_node_tokio_runtime_busy_ratio 0.42
# TYPE mixnet_packet_stage_unwrap_seconds histogram
mixnet_packet_stage_unwrap_seconds_bucket{le="+Inf"} 1
mixnet_packet_stage_unwrap_seconds_sum 0.0001
mixnet_packet_stage_unwrap_seconds_count 1
"""


def test_parse_runtime():
    rt = parse_runtime_gauges(RUNTIME_SAMPLE)
    assert rt["nym_node_tokio_runtime_global_queue_depth"] == 3.0, rt
    assert rt["nym_node_tokio_runtime_busy_ratio"] == 0.42, rt
    # the mixnet histogram must NOT leak into the runtime gauges
    assert all(k.startswith("nym_node_tokio_runtime_") for k in rt), rt


COUNTER_SAMPLE = """\
# TYPE nym_node_mixnet_egress_forward_packets_dropped gauge
nym_node_mixnet_egress_forward_packets_dropped 12
# TYPE nym_node_mixnet_egress_forward_packets_dropped_rate gauge
nym_node_mixnet_egress_forward_packets_dropped_rate 0.5
# TYPE nym_node_tokio_runtime_busy_ratio gauge
nym_node_tokio_runtime_busy_ratio 0.42
"""


def test_parse_node_counters():
    c = parse_node_counters(COUNTER_SAMPLE)
    assert c["nym_node_mixnet_egress_forward_packets_dropped"] == 12.0, c
    # the node's own _rate gauge must be excluded (we diff raw counters ourselves)
    assert "nym_node_mixnet_egress_forward_packets_dropped_rate" not in c, c
    # nothing outside the nym_node_mixnet_ family
    assert all(k.startswith("nym_node_mixnet_") for k in c), c


NETWORK_SAMPLE = """\
# TYPE nym_node_network_active_ingress_mixnet_connections gauge
nym_node_network_active_ingress_mixnet_connections 686
# TYPE nym_node_network_active_egress_mixnet_connections gauge
nym_node_network_active_egress_mixnet_connections 689
# TYPE nym_node_network_idle_closed_ingress_mixnet_connections gauge
nym_node_network_idle_closed_ingress_mixnet_connections 42
# TYPE nym_node_mixnet_egress_forward_packets_dropped gauge
nym_node_mixnet_egress_forward_packets_dropped 5
"""


def test_parse_network_gauges():
    n = parse_network_gauges(NETWORK_SAMPLE)
    assert n["nym_node_network_active_ingress_mixnet_connections"] == 686.0, n
    assert n["nym_node_network_active_egress_mixnet_connections"] == 689.0, n
    assert n["nym_node_network_idle_closed_ingress_mixnet_connections"] == 42.0, n
    # only the nym_node_network_ family (mixnet counter must not leak in)
    assert all(k.startswith("nym_node_network_") for k in n), n


def test_build_url_and_build_info():
    base = "http://node:8080"
    assert build_url(base) == "http://node:8080/api/v1/metrics/prometheus"
    assert build_url(base, BUILD_INFO_PATH) == "http://node:8080/api/v1/build-information"
    # given a full prometheus URL, still derive the build-info URL (strip the known path), idempotent
    prom = build_url(base)
    assert build_url(prom, BUILD_INFO_PATH) == "http://node:8080/api/v1/build-information"
    assert build_url(prom) == prom
    bi = BuildInfo(
        binary_name="nym-node",
        version="1.33.0",
        commit_sha="69dddbd93052c0fcd3058995471698df65e34657",
        commit_branch="feat/mixnet-improvements",
        build_timestamp="2026-06-12T07:57:50.852635590Z",
    )
    assert bi.short_sha == "69dddbd93", bi.short_sha
    assert bi.built == "2026-06-12 07:57", bi.built


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all good")
