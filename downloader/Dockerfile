FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    aria2 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY download.sh .
RUN chmod +x download.sh

ENTRYPOINT ["/app/download.sh"]
