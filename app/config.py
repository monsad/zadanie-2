from typing import Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration read from environment variables."""

    model_config = SettingsConfigDict(frozen=True)

    db_path: str = "/data/webhooks.db"

    worker_concurrency: int = Field(64, ge=1)
    lease_seconds: float = Field(30.0, gt=0)
    poll_interval: float = Field(0.5, gt=0)
    recovery_interval: float = Field(10.0, gt=0)
    stats_interval: float = Field(1.0, gt=0)

    max_attempts: int = Field(5, ge=1)
    backoff_base: float = Field(2.0, ge=0)
    backoff_max: float = Field(300.0, ge=0)

    max_queue_depth: int = Field(10_000, ge=0)
    max_body_bytes: int = Field(256 * 1024, gt=0)

    rate_limit_rps: float = Field(50.0, gt=0)
    rate_limit_burst: int = Field(100, ge=1)
    allowed_sources: str = ""

    webhook_secrets: str = ""
    signature_tolerance: float = Field(300.0, gt=0)
    admin_token: str = ""

    retention_completed_days: float = Field(7.0, gt=0)
    retention_dead_days: float = Field(30.0, gt=0)
    retention_idempotency_days: float = Field(90.0, gt=0)
    retention_interval: float = Field(3600.0, gt=0)

    processing_min: float = Field(2.0, ge=0)
    processing_max: float = Field(5.0, ge=0)
    failure_rate: float = Field(0.0, ge=0, le=1)

    shutdown_grace: float = Field(10.0, ge=0)
    log_level: str = "INFO"

    @model_validator(mode="after")
    def _check_timing(self) -> Self:
        if self.processing_min > self.processing_max:
            raise ValueError("PROCESSING_MIN must be <= PROCESSING_MAX")
        if self.lease_seconds < 3 * self.processing_max:
            raise ValueError("LEASE_SECONDS must be >= 3 * PROCESSING_MAX")
        payload_ttl = max(
            self.retention_completed_days, self.retention_dead_days
        )
        if self.retention_idempotency_days <= payload_ttl:
            raise ValueError(
                "RETENTION_IDEMPOTENCY_DAYS must be greater than "
                "RETENTION_COMPLETED_DAYS and RETENTION_DEAD_DAYS"
            )
        return self

    @property
    def processing_timeout(self) -> float:
        return self.lease_seconds / 2


def parse_csv(value: str) -> frozenset[str]:
    return frozenset(item.strip() for item in value.split(",") if item.strip())


def parse_secrets(value: str) -> dict[str, bytes]:
    """``'shop:s3cr3t,crm:other'`` -> ``{'shop': b's3cr3t', ...}``."""
    secrets: dict[str, bytes] = {}
    for item in parse_csv(value):
        name, sep, secret = item.partition(":")
        if not sep or not name or not secret:
            raise ValueError(f"invalid WEBHOOK_SECRETS entry: {name!r}")
        secrets[name.strip()] = secret.strip().encode()
    return secrets
