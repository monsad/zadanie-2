FROM python:3.15-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/srv/.venv \
    PATH=/srv/.venv/bin:$PATH

WORKDIR /srv
RUN pip install --no-cache-dir uv==0.13.1

# Exact versions from uv.lock, so the image runs what pip-audit checked.
# Dependencies first: this layer is cached until the lockfile changes.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app ./app
RUN uv sync --frozen --no-dev --no-editable \
    && useradd --uid 10001 --create-home app \
    && mkdir /data && chown app /data

USER app
VOLUME /data
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s \
    CMD python -c "import urllib.request as u; u.urlopen('http://127.0.0.1:8000/health', timeout=2)"

# One process: SQLite has one writer and the worker lives in this process.
CMD ["uvicorn", "app.main:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--timeout-keep-alive", "5", "--timeout-graceful-shutdown", "15", \
     "--no-access-log"]
