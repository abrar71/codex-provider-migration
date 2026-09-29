FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/abrar71/codex-provider-migration"
LABEL org.opencontainers.image.title="Codex model-provider migration"
LABEL org.opencontainers.image.description="Migrate legacy Codex model-provider metadata"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /opt/codex-provider-migration

COPY migrate.py migration_io.py migration_workers.py progress.py restore.py verify.py docker_entrypoint.py ./

ENTRYPOINT ["python3", "/opt/codex-provider-migration/docker_entrypoint.py"]
CMD ["migrate", "--help"]
