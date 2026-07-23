FROM oven/bun:1.3.13 AS builder

WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY tsconfig.json ./
COPY src/ ./src/
RUN bun build src/index.ts --target=bun --outdir=dist

FROM oven/bun:1.3.13-slim

# Text extraction tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    djvulibre-bin \
    unzip \
    calibre \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data/cache \
    && chown -R bun:bun /data/cache

WORKDIR /app
COPY --from=builder --chown=bun:bun /app/dist/ ./dist/

EXPOSE 3001

USER bun

CMD ["bun", "dist/index.js"]
