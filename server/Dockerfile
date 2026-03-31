FROM oven/bun:1 AS builder

WORKDIR /app
COPY package.json bun.lock* ./
RUN bun install
COPY tsconfig.json ./
COPY src/ ./src/
RUN bun build src/index.ts --target=bun --outdir=dist

FROM oven/bun:1-slim

# Text extraction tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    djvulibre-bin \
    unzip \
    calibre \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/dist/ ./dist/
COPY --from=builder /app/node_modules/ ./node_modules/

EXPOSE 3001

CMD ["bun", "run", "dist/index.js"]
