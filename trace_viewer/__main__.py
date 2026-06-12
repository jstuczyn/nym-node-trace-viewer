"""CLI entry point: `python -m trace_viewer --url ... [--token ... | --token-file ...]`."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import httpx

from .app import TraceViewer
from .scrape import ScrapeError, build_url, fetch_build_info, scrape
from .stats import (
    CONNECTION_GAUGES,
    DELAY_DRAIN_BATCH_METRIC,
    DRAIN_BATCH_METRIC,
    DROP_COUNTERS,
    EGRESS_FILL_METRIC,
    THROUGHPUT_COUNTERS,
    TOTAL_METRIC,
    compute_stage_stats,
    human_count,
    human_pct,
    human_seconds,
    ordered_metric_names,
    pct_of_total,
)


def resolve_token(args: argparse.Namespace) -> str | None:
    if args.token:
        return args.token
    if args.token_file:
        return open(args.token_file).read().strip()
    return os.environ.get("NYM_PROMETHEUS_TOKEN")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="trace_viewer",
        description="Live TUI for a nym-node's mixnet_packet_* latency histograms.",
    )
    p.add_argument(
        "--url", default="http://127.0.0.1:8080",
        help="node base URL or full prometheus endpoint (default: %(default)s)",
    )
    p.add_argument("--token", help="prometheus bearer token (or use --token-file / $NYM_PROMETHEUS_TOKEN)")
    p.add_argument("--token-file", help="read the bearer token from this file")
    p.add_argument("--interval", type=float, default=2.0, help="poll interval seconds (default: %(default)s)")
    p.add_argument("--insecure", action="store_true", help="skip TLS verification (for https with self-signed certs)")
    p.add_argument("--once", action="store_true", help="print one cumulative snapshot to stdout and exit (no TUI)")
    return p.parse_args(argv)


async def run_once(url: str, token: str | None, verify: bool) -> int:
    async with httpx.AsyncClient(timeout=5.0, verify=verify) as client:
        build = await fetch_build_info(client, url, token)
        try:
            result = await scrape(client, url, token)
        except ScrapeError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
    if build is not None:
        print(
            f"build: v{build.version}  {build.short_sha}  ({build.commit_branch})  "
            f"built {build.built}  [{build.binary_name}]"
        )
        print()
    snaps = result.histograms
    if not snaps and not result.runtime and not result.counters:
        print("no mixnet_packet_* metrics found (is the node forwarding traffic? is sampling enabled?)")
        return 0
    stats = {
        metric: compute_stage_stats(metric, None, snaps[metric], 0.0, cumulative=True)
        for metric in ordered_metric_names(list(snaps.keys()))
    }
    total = stats.get(TOTAL_METRIC)
    total_mean = total.mean if total else None
    hdr = (
        f"{'Stage':<18} {'p50':>9} {'p90':>9} {'p99':>9} {'mean':>9} "
        f"{'%tot':>6} {'count':>9} {'>6.55s':>8}"
    )
    print(hdr)
    print("-" * len(hdr))
    for st in stats.values():
        print(
            f"{st.label:<18} {human_seconds(st.p50):>9} {human_seconds(st.p90):>9} "
            f"{human_seconds(st.p99):>9} {human_seconds(st.mean):>9} "
            f"{human_pct(pct_of_total(st.mean, total_mean)):>6} "
            f"{human_count(st.count):>9} {human_count(st.overflow):>8}"
        )
    # forwarder drain-batch histograms (counts, not latency stages) - summarise each separately
    p = lambda v: human_count(v) if v is not None else "-"  # noqa: E731
    for metric, label in ((DRAIN_BATCH_METRIC, "ingress"), (DELAY_DRAIN_BATCH_METRIC, "delay")):
        snap = snaps.get(metric)
        if snap is None:
            continue
        d = compute_stage_stats(metric, None, snap, 0.0, cumulative=True)
        mean = f"{d.mean:.1f}" if d.mean is not None else "-"
        print()
        print(
            f"forwarder {label} drain (packets/wakeup): p50 {p(d.p50)}  p90 {p(d.p90)}  "
            f"p99 {p(d.p99)}  mean {mean}  ·  wakeups {human_count(d.count)}"
        )
    # egress buffer fill ratio (count histogram, 0..1) - summarise as percentages
    egress = snaps.get(EGRESS_FILL_METRIC)
    if egress is not None:
        e = compute_stage_stats(EGRESS_FILL_METRIC, None, egress, 0.0, cumulative=True)
        pct = lambda v: f"{v * 100:.0f}%" if v is not None else "-"  # noqa: E731
        print(
            f"egress buffer fill: p50 {pct(e.p50)}  p90 {pct(e.p90)}  p99 {pct(e.p99)}  "
            f"mean {pct(e.mean)}  ·  sends {human_count(e.count)}"
        )
    rt = result.runtime
    if rt:
        bits = []
        if "nym_node_tokio_runtime_num_workers" in rt:
            bits.append(f"workers {int(rt['nym_node_tokio_runtime_num_workers'])}")
        if "nym_node_tokio_runtime_alive_tasks" in rt:
            bits.append(f"tasks {human_count(rt['nym_node_tokio_runtime_alive_tasks'])}")
        if "nym_node_tokio_runtime_global_queue_depth" in rt:
            bits.append(f"run-queue {int(rt['nym_node_tokio_runtime_global_queue_depth'])}")
        if "nym_node_tokio_runtime_busy_ratio" in rt:
            bits.append(f"busy {rt['nym_node_tokio_runtime_busy_ratio'] * 100:.0f}%")
        print()
        print("runtime: " + "  ·  ".join(bits))
    # node counters (cumulative since boot) - drops + forwarded throughput, for A/B comparison
    if result.counters:
        drops = [
            f"{label} {human_count(result.counters[m])}"
            for m, label in DROP_COUNTERS
            if m in result.counters
        ]
        tput = [
            f"{label} {human_count(result.counters[m])}"
            for m, label in THROUGHPUT_COUNTERS
            if m in result.counters
        ]
        print()
        if drops:
            print("drops: " + "  ·  ".join(drops))
        if tput:
            print("forwarded: " + "  ·  ".join(tput))
    if result.network:
        conns = [
            f"{label} {int(result.network[m])}"
            for m, label in CONNECTION_GAUGES
            if m in result.network
        ]
        if conns:
            print("conns: " + "  ·  ".join(conns))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    token = resolve_token(args)
    url = build_url(args.url)
    if args.once:
        return asyncio.run(run_once(url, token, verify=not args.insecure))
    TraceViewer(url=url, token=token, interval=args.interval, verify=not args.insecure).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
