/**
 * Formatting. Every figure in this app goes through here.
 *
 * House rules:
 *   - All dates and times are IST (Asia/Kolkata), stated explicitly. The API
 *     emits +05:30; we never let the device timezone re-interpret it.
 *   - Indian digit grouping for counts and rupees (12,34,567).
 *   - A signed percentage always shows its sign, so -0.03 never reads as 0.03.
 *   - Nothing here invents a number: no rounding-up of a loss, no annualising,
 *     no projecting. Format only.
 */
import type { DateRange, Unit } from '@/api/types';

const IST = 'Asia/Kolkata';

const intFmt = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 0 });
const inrFmt = new Intl.NumberFormat('en-IN', {
  style: 'currency',
  currency: 'INR',
  maximumFractionDigits: 0,
});

/** "-" is the honest rendering of an absent number. Never 0, never "--". */
export const EMPTY = '—';

const MINUS = '−'; // true minus sign, aligns with tabular numerals

function signed(value: number, digits: number): string {
  const fixed = Math.abs(value).toFixed(digits);
  if (value > 0) return `+${fixed}`;
  if (value < 0) return `${MINUS}${fixed}`;
  return fixed;
}

/** A percentage that carries its sign: +0.24% / -0.11% / 0.00% */
export function pct(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) return EMPTY;
  return `${signed(value, digits)}%`;
}

/** An unsigned magnitude, for drawdowns and win rates where the sign is implied. */
export function pctAbs(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) return EMPTY;
  return `${Math.abs(value).toFixed(digits)}%`;
}

export function count(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return EMPTY;
  return intFmt.format(value);
}

export function inr(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return EMPTY;
  return inrFmt.format(value);
}

export function ratio(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) return EMPTY;
  return `${value.toFixed(digits)}×`;
}

/**
 * Render a Fact value in its declared unit. The unit decides, never the caller.
 *
 * Sign policy: a negative ALWAYS shows its minus. A positive shows a `+` only
 * where the sign carries information — i.e. in a labelled metric, where the
 * comparison to zero is the whole point. Inline in prose it does not: the
 * sentence "a worst drawdown of 12.7%" is correct, and "+12.7%" reads as a gain.
 * So the default here is `auto`, and the Metric components ask for `always`.
 */
export function factValue(value: number | string, unit: Unit, sign: 'auto' | 'always' = 'auto'): string {
  if (typeof value === 'string') return value;
  const showPlus = sign === 'always';
  switch (unit) {
    case 'pct_per_trade':
      return showPlus || value < 0 ? pct(value) : `${value.toFixed(2)}%`;
    case 'pct': {
      const digits = Math.abs(value) >= 1 ? 1 : 2;
      return showPlus || value < 0 ? pct(value, digits) : `${value.toFixed(digits)}%`;
    }
    case 'bps':
      return showPlus || value < 0 ? `${signed(value, 0)} bps` : `${value.toFixed(0)} bps`;
    case 'count':
      return count(value);
    case 'ratio':
    case 'x':
      return ratio(value);
    case 'days':
      return `${count(value)} ${Math.abs(value) === 1 ? 'day' : 'days'}`;
    case 'sessions':
      return `${count(value)} ${Math.abs(value) === 1 ? 'session' : 'sessions'}`;
    case 'inr':
      return inr(value);
    default:
      return String(value);
  }
}

/** A short unit hint for the provenance sheet ("per trade", "sessions", ...). */
export function unitHint(unit: Unit): string {
  switch (unit) {
    case 'pct_per_trade':
      return 'per trade, net of costs';
    case 'pct':
      return 'percent';
    case 'bps':
      return 'basis points';
    case 'ratio':
    case 'x':
      return 'ratio';
    case 'count':
      return 'count';
    case 'inr':
      return 'rupees (virtual)';
    default:
      return unit;
  }
}

function istParts(iso: string): Intl.DateTimeFormatOptions & { timeZone: string } {
  void iso;
  return { timeZone: IST };
}

/** 8 Sep 2026 */
export function dateShort(iso: string | null | undefined): string {
  if (!iso) return EMPTY;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return EMPTY;
  return new Intl.DateTimeFormat('en-IN', {
    ...istParts(iso),
    day: 'numeric',
    month: 'short',
    year: 'numeric',
  }).format(d);
}

/** Sep 2026 -- for range endpoints where the day is noise. */
export function monthYear(iso: string | null | undefined): string {
  if (!iso) return EMPTY;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return EMPTY;
  return new Intl.DateTimeFormat('en-IN', {
    ...istParts(iso),
    month: 'short',
    year: 'numeric',
  }).format(d);
}

/** 8 Sep 2026, 4:05 pm IST */
export function dateTimeIST(iso: string | null | undefined): string {
  if (!iso) return EMPTY;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return EMPTY;
  const s = new Intl.DateTimeFormat('en-IN', {
    ...istParts(iso),
    day: 'numeric',
    month: 'short',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  }).format(d);
  return `${s} IST`;
}

/** 4:05 pm IST */
export function timeIST(iso: string | null | undefined): string {
  if (!iso) return EMPTY;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return EMPTY;
  const s = new Intl.DateTimeFormat('en-IN', {
    ...istParts(iso),
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  }).format(d);
  return `${s} IST`;
}

/** Jan 2018 - Dec 2025 */
export function rangeLabel(range: DateRange | null | undefined): string {
  if (!range) return EMPTY;
  return `${monthYear(range.start)} – ${monthYear(range.end)}`;
}

/** Jan 2018 - Dec 2025 (8.0 yrs), or a single date when the window is one day. */
export function rangeWithSpan(range: DateRange | null | undefined): string {
  if (!range) return EMPTY;
  if (range.start === range.end) return `${dateShort(range.start)} (single session)`;
  const a = new Date(range.start).getTime();
  const b = new Date(range.end).getTime();
  if (Number.isNaN(a) || Number.isNaN(b)) return rangeLabel(range);
  const days = Math.max(0, Math.round((b - a) / 86_400_000));
  const span = days >= 365 ? `${(days / 365).toFixed(1)} yrs` : `${days} days`;
  return `${rangeLabel(range)} (${span})`;
}

/** "5 days ago" -- relative to now, computed in absolute time so IST is safe. */
export function relative(iso: string | null | undefined): string {
  if (!iso) return EMPTY;
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return EMPTY;
  const mins = Math.round((Date.now() - then) / 60_000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins} min ago`;
  const hrs = Math.round(mins / 60);
  if (hrs < 24) return `${hrs} hr${hrs === 1 ? '' : 's'} ago`;
  const days = Math.round(hrs / 24);
  if (days < 31) return `${days} day${days === 1 ? '' : 's'} ago`;
  const months = Math.round(days / 30.44);
  if (months < 12) return `${months} month${months === 1 ? '' : 's'} ago`;
  return `${Math.round(months / 12)} yr${Math.round(months / 12) === 1 ? '' : 's'} ago`;
}

/** snake_case enum -> "Snake case", for statuses, causes and trigger types. */
export function humanise(token: string): string {
  const s = token.replace(/_/g, ' ');
  return s.charAt(0).toUpperCase() + s.slice(1);
}
