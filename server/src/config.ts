export function boundedInteger(
  rawValue: string | undefined,
  fallback: number,
  min: number,
  max: number,
): number {
  if (rawValue === undefined || !/^\d+$/.test(rawValue)) return fallback;
  const value = Number(rawValue);
  return Number.isSafeInteger(value) && value >= min && value <= max
    ? value
    : fallback;
}
