#!/usr/bin/env python3
import math
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


UNEXPOSED = "not_exposed_by_producer_perf"


@dataclass(frozen=True)
class CounterSnapshot:
    record_send_total: Optional[float]
    records_per_request_avg: Optional[float]
    produce_requests: Optional[float]
    request_size_avg: Optional[float]
    retries: Optional[float]
    errors: Optional[float]
    batch_splits: Optional[float]


def _metric(text: str, name: str) -> Optional[float]:
    pattern = rf"producer-metrics:{re.escape(name)}:\{{[^}}]*\}}\s*:\s*([-+0-9.NaInf]+)"
    match = re.search(pattern, text)
    if not match:
        return None
    try:
        value = float(match.group(1))
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def _snapshot(text: str) -> CounterSnapshot:
    record_send_total = _metric(text, "record-send-total")
    records_per_request_avg = _metric(text, "records-per-request-avg")
    produce_requests = None
    if (
        record_send_total is not None
        and records_per_request_avg is not None
        and records_per_request_avg > 0.0
    ):
        produce_requests = record_send_total / records_per_request_avg

    return CounterSnapshot(
        record_send_total=record_send_total,
        records_per_request_avg=records_per_request_avg,
        produce_requests=produce_requests,
        request_size_avg=_metric(text, "request-size-avg"),
        retries=_metric(text, "record-retry-total"),
        errors=_metric(text, "record-error-total"),
        batch_splits=_metric(text, "batch-split-total"),
    )


def _read_snapshot(path: Path) -> CounterSnapshot:
    return _snapshot(path.read_text(encoding="utf-8"))


def _number(value: Optional[float], digits: int = 3, compact: bool = True) -> str:
    if value is None:
        return UNEXPOSED
    if compact and abs(value - round(value)) < 0.0005:
        return str(int(round(value)))
    return f"{value:.{digits}f}"


def _format_fields(fields: dict[str, str]) -> str:
    return ", ".join(f"{key}={value}" for key, value in fields.items())


def format_counter_line(label: str, text: str) -> str:
    snapshot = _snapshot(text)
    fields = {
        "produce_requests": _number(snapshot.produce_requests),
        "record_batches": UNEXPOSED,
        "records_per_batch_avg": UNEXPOSED,
        "records_per_request_avg": _number(snapshot.records_per_request_avg),
        "request_size_avg": _number(snapshot.request_size_avg),
        "retries": _number(snapshot.retries),
        "errors": _number(snapshot.errors),
        "in_flight_stalls": UNEXPOSED,
        "batch_splits": _number(snapshot.batch_splits),
        "request_splits": UNEXPOSED,
    }
    return f"{label}: {_format_fields(fields)}"


def _average(values: list[Optional[float]]) -> Optional[float]:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(present) / len(present)


def _weighted_average(
    snapshots: list[CounterSnapshot],
    value_attr: str,
    weight_attr: str,
) -> Optional[float]:
    weighted_sum = 0.0
    weight_sum = 0.0
    for snapshot in snapshots:
        value = getattr(snapshot, value_attr)
        weight = getattr(snapshot, weight_attr)
        if value is None or weight is None or weight <= 0.0:
            continue
        weighted_sum += value * weight
        weight_sum += weight
    if weight_sum <= 0.0:
        return None
    return weighted_sum / weight_sum


def format_average_counter_line(paths: list[Path]) -> str:
    snapshots = [_read_snapshot(path) for path in paths]
    total_records = sum(
        snapshot.record_send_total
        for snapshot in snapshots
        if snapshot.record_send_total is not None
    )
    total_requests = sum(
        snapshot.produce_requests
        for snapshot in snapshots
        if snapshot.produce_requests is not None
    )
    records_per_request_avg = None
    if total_requests > 0.0:
        records_per_request_avg = total_records / total_requests

    fields = {
        "produce_requests": _number(
            _average([snapshot.produce_requests for snapshot in snapshots]),
            compact=False,
        ),
        "record_batches": UNEXPOSED,
        "records_per_batch_avg": UNEXPOSED,
        "records_per_request_avg": _number(records_per_request_avg, compact=False),
        "request_size_avg": _number(
            _weighted_average(snapshots, "request_size_avg", "produce_requests"),
            compact=False,
        ),
        "retries": _number(
            _average([snapshot.retries for snapshot in snapshots]),
            compact=False,
        ),
        "errors": _number(
            _average([snapshot.errors for snapshot in snapshots]),
            compact=False,
        ),
        "in_flight_stalls": UNEXPOSED,
        "batch_splits": _number(
            _average([snapshot.batch_splits for snapshot in snapshots]),
            compact=False,
        ),
        "request_splits": UNEXPOSED,
    }
    return f"java average counters: {_format_fields(fields)}"


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(
            "usage: producer_counter_metrics.py format PATH | average PATH...",
            file=sys.stderr,
        )
        return 2

    command = argv[1]
    paths = [Path(arg) for arg in argv[2:]]
    if command == "format" and len(paths) == 1:
        print(format_counter_line("java producer counters", paths[0].read_text(encoding="utf-8")))
        return 0
    if command == "average":
        print(format_average_counter_line(paths))
        return 0

    print(f"invalid command or arguments: {' '.join(argv[1:])}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
