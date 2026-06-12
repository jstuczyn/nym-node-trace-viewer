"""Fetch the node's prometheus endpoint and parse the metrics we care about."""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx
from prometheus_client.parser import text_string_to_metric_families

from .stats import NETWORK_GAUGE_PREFIX, NODE_COUNTER_PREFIX, Snapshot

DEFAULT_PATH = "/api/v1/metrics/prometheus"
BUILD_INFO_PATH = "/api/v1/build-information"

RUNTIME_PREFIX = "nym_node_tokio_runtime_"


@dataclass
class Scrape:
    """One scrape: mixnet_packet_* histograms, nym_node_tokio_runtime_* gauges, node counters, conn gauges."""

    histograms: dict[str, Snapshot] = field(default_factory=dict)
    runtime: dict[str, float] = field(default_factory=dict)
    counters: dict[str, float] = field(default_factory=dict)
    network: dict[str, float] = field(default_factory=dict)


class ScrapeError(Exception):
    """Any failure fetching or authenticating against the endpoint (carries a human message)."""


@dataclass
class BuildInfo:
    """Subset of /api/v1/build-information used to confirm which binary is deployed."""

    binary_name: str
    version: str
    commit_sha: str
    commit_branch: str
    build_timestamp: str

    @property
    def short_sha(self) -> str:
        return self.commit_sha[:9] if self.commit_sha else "?"

    @property
    def built(self) -> str:
        # "2026-06-12T07:57:50.852..Z" -> "2026-06-12 07:57"
        return self.build_timestamp[:16].replace("T", " ") if self.build_timestamp else "?"


def _base_root(base: str) -> str:
    """Reduce a base-or-full-endpoint URL to the bare host root (stripping any known API path)."""
    base = base.rstrip("/")
    for known in (DEFAULT_PATH, BUILD_INFO_PATH):
        if base.endswith(known):
            return base[: -len(known)]
    return base


def build_url(base: str, path: str = DEFAULT_PATH) -> str:
    """Accept a base (host:port) or a full known endpoint URL; return the URL for `path`."""
    return _base_root(base) + path


async def fetch_text(client: httpx.AsyncClient, url: str, token: str | None) -> str:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        resp = await client.get(url, headers=headers)
    except httpx.HTTPError as e:
        raise ScrapeError(f"connection failed: {e}") from e
    if resp.status_code == 401:
        raise ScrapeError("401 unauthorized - bearer token missing or invalid")
    if resp.status_code == 400:
        raise ScrapeError("400 - Authorization header missing")
    if resp.status_code == 500:
        raise ScrapeError("500 - node has no prometheus token configured")
    if resp.status_code != 200:
        raise ScrapeError(f"HTTP {resp.status_code}")
    return resp.text


async def fetch_build_info(
    client: httpx.AsyncClient, base: str, token: str | None
) -> BuildInfo | None:
    """Best-effort fetch of /api/v1/build-information. Returns None on any failure - it's purely
    informational (confirming the deployed binary), so it must never break the viewer."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        resp = await client.get(build_url(base, BUILD_INFO_PATH), headers=headers)
        resp.raise_for_status()
        d = resp.json()
    except (httpx.HTTPError, ValueError):
        return None
    return BuildInfo(
        binary_name=str(d.get("binary_name", "?")),
        version=str(d.get("build_version", "?")),
        commit_sha=str(d.get("commit_sha", "")),
        commit_branch=str(d.get("commit_branch", "")),
        build_timestamp=str(d.get("build_timestamp", "")),
    )


def parse_mixnet_packet(text: str) -> dict[str, Snapshot]:
    """Extract every `mixnet_packet_*` histogram into {metric_name: Snapshot}."""
    out: dict[str, Snapshot] = {}
    for family in text_string_to_metric_families(text):
        if not family.name.startswith("mixnet_packet_"):
            continue
        snap = Snapshot()
        for sample in family.samples:
            if sample.name.endswith("_bucket"):
                le = float(sample.labels["le"])  # '+Inf' -> inf
                snap.buckets[le] = sample.value
            elif sample.name.endswith("_sum"):
                snap.sum = sample.value
            elif sample.name.endswith("_count"):
                snap.count = sample.value
        out[family.name] = snap
    return out


def parse_runtime_gauges(text: str) -> dict[str, float]:
    """Extract every `nym_node_tokio_runtime_*` gauge into {metric_name: value} (one sample each)."""
    out: dict[str, float] = {}
    for family in text_string_to_metric_families(text):
        if not family.name.startswith(RUNTIME_PREFIX):
            continue
        for sample in family.samples:
            out[sample.name] = sample.value
    return out


def parse_node_counters(text: str) -> dict[str, float]:
    """Extract `nym_node_mixnet_*` counters (skipping the node's own `_rate` gauges; we diff
    the raw counters ourselves for the live window)."""
    out: dict[str, float] = {}
    for family in text_string_to_metric_families(text):
        if not family.name.startswith(NODE_COUNTER_PREFIX) or family.name.endswith("_rate"):
            continue
        for sample in family.samples:
            if not sample.name.endswith("_rate"):
                out[sample.name] = sample.value
    return out


def parse_network_gauges(text: str) -> dict[str, float]:
    """Extract `nym_node_network_*` connection-count gauges (skipping any `_rate` variants)."""
    out: dict[str, float] = {}
    for family in text_string_to_metric_families(text):
        if not family.name.startswith(NETWORK_GAUGE_PREFIX) or family.name.endswith("_rate"):
            continue
        for sample in family.samples:
            if not sample.name.endswith("_rate"):
                out[sample.name] = sample.value
    return out


async def scrape(client: httpx.AsyncClient, url: str, token: str | None) -> Scrape:
    text = await fetch_text(client, url, token)
    return Scrape(
        parse_mixnet_packet(text),
        parse_runtime_gauges(text),
        parse_node_counters(text),
        parse_network_gauges(text),
    )
