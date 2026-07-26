const HOUR_MS = 60 * 60 * 1000;
const DAY_MS = 24 * HOUR_MS;

export const RETENTION_TTL_MS = {
  session: HOUR_MS,
  days_7: 7 * DAY_MS,
  days_14: 14 * DAY_MS,
  days_30: 30 * DAY_MS,
  persistent: null,
} as const;

export type Retention = keyof typeof RETENTION_TTL_MS;

export function isRetention(value: unknown): value is Retention {
  return typeof value === "string" &&
    Object.hasOwn(RETENTION_TTL_MS, value);
}

export function retentionExpiresAt(
  retention: Retention,
  activatedAt: Date,
): Date | null {
  const ttl = RETENTION_TTL_MS[retention];
  return ttl === null ? null : new Date(activatedAt.getTime() + ttl);
}

export function retentionAllowsRefresh(retention: Retention): boolean {
  return retention !== "session";
}
