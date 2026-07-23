FROM oven/bun:1.3.13@sha256:87416c977a612a204eb54ab9f3927023c2a3c971f4f345a01da08ea6262ae30e AS builder

WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY tsconfig.json ./
COPY src/ ./src/
RUN bun build src/index.ts --target=bun --outdir=dist

FROM oven/bun:1.3.13-slim@sha256:7e8ed3961db1cdedf17d516dda87948cfedbd294f53bf16462e5b57ed3fff0f1

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
