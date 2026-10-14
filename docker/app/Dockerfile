# Ảnh dùng chung cho các dịch vụ Python: mock-api, collector, alerter, init, tools (replay, thí nghiệm)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_ROOT=/opt/app \
    PYTHONPATH=/opt/app/src \
    DATA_DIR=/opt/app/data \
    SCHEMA_DIR=/opt/app/schemas \
    MPLCONFIGDIR=/tmp/mpl

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/app
COPY docker/app/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Mã nguồn được copy vào ảnh (chạy được độc lập) và còn được mount đè trong docker-compose khi phát triển
COPY schemas /opt/app/schemas
COPY src /opt/app/src
COPY tests /opt/app/tests

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "sportslake.tools.init_infra"]
