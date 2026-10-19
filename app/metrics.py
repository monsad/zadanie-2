from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
)

_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5)
_WORK_BUCKETS = (0.5, 1, 2, 3, 4, 5, 7.5, 10, 30, 60, 300)


class Metrics:
    """Instance-scoped registry so every app (and test) starts from zero."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        r = self.registry
        self.received = Counter(
            "webhooks_received_total",
            "Incoming webhooks by outcome",
            ["source", "outcome"],
            registry=r,
        )
        self.processed = Counter(
            "webhooks_processed_total",
            "Processing attempts by result",
            ["event_type", "result"],
            registry=r,
        )
        self.queue_depth = Gauge(
            "webhooks_queue_depth",
            "Non-terminal events by status",
            ["status"],
            registry=r,
        )
        self.dispatch_lag = Gauge(
            "webhooks_dispatch_lag_seconds",
            "How long the oldest runnable event has been waiting",
            registry=r,
        )
        self.processing_seconds = Histogram(
            "webhook_processing_seconds",
            "Duration of one processing attempt",
            ["event_type"],
            buckets=_WORK_BUCKETS,
            registry=r,
        )
        self.end_to_end_seconds = Histogram(
            "webhook_end_to_end_seconds",
            "From acceptance to completion",
            buckets=_WORK_BUCKETS,
            registry=r,
        )
        self.ingest_seconds = Histogram(
            "webhook_ingest_seconds",
            "POST /webhooks latency",
            buckets=_LATENCY_BUCKETS,
            registry=r,
        )
        self.lease_recovered = Counter(
            "webhook_lease_recovered_total",
            "Leases taken back from dead or slow workers",
            ["reason"],
            registry=r,
        )
        self.lease_stale = Counter(
            "webhook_lease_stale_total",
            "Worker writes rejected because the lease was lost",
            registry=r,
        )
        self.workers_busy = Gauge(
            "workers_busy", "Events being processed now", registry=r
        )
        self.workers_alive = Gauge(
            "workers_alive",
            "1 while the dispatcher task is running, else 0",
            registry=r,
        )
        self.db_disk_free_ratio = Gauge(
            "webhook_db_disk_free_ratio",
            "Free space on the volume holding the database",
            registry=r,
        )
        self.retention_purged = Counter(
            "webhook_retention_purged_total",
            "Rows deleted by retention",
            ["status"],
            registry=r,
        )
