# AI-generated configuration (Codex): metadata CLI without heavy workload dependencies.
FROM python:3.14.4-slim-bookworm@sha256:fc74d22ffd0d5ac395a4b7bdda75a4539758862c49ebf3005647084631e63789 AS core
WORKDIR /app
COPY requirements.lock .
RUN python -m pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml alembic.ini ./
COPY migrations migrations
COPY src src
RUN python -m pip install --no-build-isolation --no-deps .
ENTRYPOINT ["papersearch"]

FROM core AS embedding
COPY requirements-embedding.lock .
RUN python -m pip install --no-cache-dir -r requirements-embedding.lock

FROM embedding AS review
COPY requirements-review.lock .
RUN python -m pip install --no-cache-dir -r requirements-review.lock

FROM core AS cli
