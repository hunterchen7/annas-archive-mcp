import dns from "node:dns";
import fs from "node:fs";
import https from "node:https";
import net from "node:net";
import type { IncomingHttpHeaders, IncomingMessage } from "node:http";

const blockedAddresses = new net.BlockList();
for (const [network, prefix] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.0.2.0", 24],
  ["192.88.99.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
] as const) {
  blockedAddresses.addSubnet(network, prefix, "ipv4");
}
for (const [network, prefix] of [
  ["::", 128],
  ["::1", 128],
  ["fc00::", 7],
  ["fe80::", 10],
  ["ff00::", 8],
  ["2001:db8::", 32],
] as const) {
  blockedAddresses.addSubnet(network, prefix, "ipv6");
}

export interface SafeRequestOptions {
  allowedHosts?: ReadonlySet<string>;
  method?: "GET" | "POST";
  headers?: Readonly<Record<string, string | number>>;
  body?: string | Buffer;
  maxBytes: number;
  timeoutMs?: number;
  maxRedirects?: number;
  followRedirects?: boolean;
}

export interface SafeResponse {
  statusCode: number;
  headers: IncomingHttpHeaders;
  body: Buffer;
}

interface PinnedAddress {
  address: string;
  family: 4 | 6;
}

function normalizeMappedIpv4(address: string): string | null {
  const match = address.match(/^::ffff:(\d{1,3}(?:\.\d{1,3}){3})$/i);
  return match?.[1] ?? null;
}

export function isPublicAddress(address: string): boolean {
  const mapped = normalizeMappedIpv4(address);
  if (mapped) return net.isIP(mapped) === 4 && !blockedAddresses.check(mapped, "ipv4");
  const family = net.isIP(address);
  if (family === 4) return !blockedAddresses.check(address, "ipv4");
  if (family === 6) return !blockedAddresses.check(address, "ipv6");
  return false;
}

function parseSafeUrl(rawUrl: string | URL, allowedHosts?: ReadonlySet<string>): URL {
  let url: URL;
  try {
    url = rawUrl instanceof URL ? new URL(rawUrl) : new URL(rawUrl);
  } catch {
    throw new Error("Outbound URL is invalid");
  }
  if (url.protocol !== "https:") throw new Error("Outbound URL must use HTTPS");
  if (url.username || url.password) throw new Error("Outbound URL must not contain credentials");
  if (url.port && url.port !== "443") throw new Error("Outbound URL must use the standard HTTPS port");
  const hostname = url.hostname.toLowerCase();
  if (allowedHosts && !allowedHosts.has(hostname)) {
    throw new Error("Outbound URL host is not allowed");
  }
  return url;
}

async function resolvePublicHost(hostname: string): Promise<PinnedAddress> {
  const literalFamily = net.isIP(hostname);
  const addresses = literalFamily
    ? [{ address: hostname, family: literalFamily }]
    : await dns.promises.lookup(hostname, { all: true, verbatim: true });
  if (addresses.length === 0 || addresses.some(({ address }) => !isPublicAddress(address))) {
    throw new Error("Outbound host resolves to a non-public address");
  }
  const selected = addresses[0];
  return { address: selected.address, family: selected.family as 4 | 6 };
}

export async function assertSafeOutboundUrl(
  rawUrl: string | URL,
  allowedHosts?: ReadonlySet<string>,
): Promise<URL> {
  const url = parseSafeUrl(rawUrl, allowedHosts);
  await resolvePublicHost(url.hostname);
  return url;
}

async function openResponse(
  rawUrl: string | URL,
  options: SafeRequestOptions,
  redirectsRemaining: number,
): Promise<IncomingMessage> {
  const url = parseSafeUrl(rawUrl, options.allowedHosts);
  const pinned = await resolvePublicHost(url.hostname);
  const timeoutMs = options.timeoutMs ?? 30_000;

  const response = await new Promise<IncomingMessage>((resolve, reject) => {
    const request = https.request(url, {
      method: options.method ?? "GET",
      headers: options.headers,
      lookup: (_hostname, lookupOptions, callback) => {
        if (typeof lookupOptions === "object" && lookupOptions.all) {
          callback(null, [pinned]);
        } else {
          callback(null, pinned.address, pinned.family);
        }
      },
    }, resolve);
    const totalTimer = setTimeout(() => {
      request.destroy(new Error("Outbound request timed out"));
    }, timeoutMs);
    totalTimer.unref();
    request.once("close", () => clearTimeout(totalTimer));
    request.setTimeout(Math.min(timeoutMs, 10_000), () => {
      request.destroy(new Error("Outbound request became idle"));
    });
    request.once("error", reject);
    if (options.body !== undefined) request.write(options.body);
    request.end();
  });

  const status = response.statusCode ?? 0;
  const location = response.headers.location;
  if (
    options.followRedirects !== false &&
    status >= 300 &&
    status < 400 &&
    location
  ) {
    response.destroy();
    if (redirectsRemaining <= 0) throw new Error("Outbound request exceeded its redirect limit");
    const redirected = new URL(location, url);
    return openResponse(redirected, { ...options, method: "GET", body: undefined }, redirectsRemaining - 1);
  }
  return response;
}

function validateContentLength(response: IncomingMessage, maxBytes: number): void {
  const rawLength = response.headers["content-length"];
  if (rawLength === undefined) return;
  const contentLength = Number(rawLength);
  if (!Number.isSafeInteger(contentLength) || contentLength < 0 || contentLength > maxBytes) {
    response.destroy();
    throw new Error("Outbound response exceeds the configured size limit");
  }
}

export async function safeRequest(
  url: string | URL,
  options: SafeRequestOptions,
): Promise<SafeResponse> {
  const response = await openResponse(url, options, options.maxRedirects ?? 3);
  validateContentLength(response, options.maxBytes);
  const chunks: Buffer[] = [];
  let received = 0;
  try {
    for await (const chunk of response) {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      received += buffer.length;
      if (received > options.maxBytes) {
        response.destroy();
        throw new Error("Outbound response exceeds the configured size limit");
      }
      chunks.push(buffer);
    }
  } catch (error) {
    response.destroy();
    throw error;
  }
  return {
    statusCode: response.statusCode ?? 0,
    headers: response.headers,
    body: Buffer.concat(chunks, received),
  };
}

export async function safeDownloadToFile(
  url: string | URL,
  destination: string,
  options: Omit<SafeRequestOptions, "method" | "body">,
): Promise<void> {
  const response = await openResponse(url, { ...options, method: "GET" }, options.maxRedirects ?? 3);
  if (response.statusCode !== 200) {
    response.destroy();
    throw new Error(`Download failed with HTTP ${response.statusCode ?? "unknown"}`);
  }
  validateContentLength(response, options.maxBytes);

  let handle: fs.promises.FileHandle;
  try {
    handle = await fs.promises.open(destination, "w", 0o600);
  } catch (error) {
    response.destroy();
    throw error;
  }
  let received = 0;
  try {
    for await (const chunk of response) {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      received += buffer.length;
      if (received > options.maxBytes) {
        response.destroy();
        throw new Error("Download exceeds the configured size limit");
      }
      let offset = 0;
      while (offset < buffer.length) {
        const { bytesWritten } = await handle.write(buffer, offset);
        if (bytesWritten <= 0) throw new Error("Could not write the downloaded file");
        offset += bytesWritten;
      }
    }
  } catch (error) {
    response.destroy();
    await handle.close();
    await fs.promises.rm(destination, { force: true });
    throw error;
  }
  await handle.close();
}
