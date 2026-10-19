"""JSON logs on stdout with per-task context.

Fields bound with :func:`bind` live in a ``ContextVar``. asyncio copies the
context into every new task, so one event's fields never leak into another
worker task, and :func:`bind` returns a token to undo the binding.
"""

import json
import logging
import sys
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from typing import Any

_context: ContextVar[dict[str, Any] | None] = ContextVar(
    "log_context", default=None
)


def bind(**fields: Any) -> Token[dict[str, Any]]:
    return _context.set({**(_context.get() or {}), **fields})


def unbind(token: Token[dict[str, Any]]) -> None:
    _context.reset(token)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname.lower(),
            "event": record.getMessage(),
            **(_context.get() or {}),
            **getattr(record, "fields", {}),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())


def log(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    logger.log(level, event, extra={"fields": fields})
