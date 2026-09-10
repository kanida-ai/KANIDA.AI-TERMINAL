/**
 * ONE Expo codebase -> iOS app + Android app + responsive web (desktop + mobile web).
 *
 * The mockups are the phone bar. The same system SCALES UP on tablet and desktop:
 * the cards, type and colour do not change — only the column count, the content
 * width and where navigation lives.
 */
import { useWindowDimensions } from 'react-native';

export const breakpoints = {
  /** phone */
  sm: 0,
  /** large phone / small tablet */
  md: 700,
  /** tablet / small desktop — nav moves to the top rail, grids go 2-up */
  lg: 980,
  /** desktop — story column + supporting column */
  xl: 1340,
} as const;

export type Breakpoint = keyof typeof breakpoints;

export type Layout = {
  width: number;
  bp: Breakpoint;
  /** phone-shaped: bottom tab bar, single column */
  isCompact: boolean;
  /** >= lg: top nav rail, multi-column */
  isWide: boolean;
  /** max width of the reading column */
  contentWidth: number;
  /** horizontal page padding */
  gutter: number;
  /** columns for card grids */
  columns: number;
};

export function useLayout(): Layout {
  const { width } = useWindowDimensions();
  const bp: Breakpoint =
    width >= breakpoints.xl ? 'xl' : width >= breakpoints.lg ? 'lg' : width >= breakpoints.md ? 'md' : 'sm';
  const isWide = bp === 'lg' || bp === 'xl';
  return {
    width,
    bp,
    isCompact: !isWide,
    isWide,
    contentWidth: bp === 'xl' ? 1180 : bp === 'lg' ? 940 : 640,
    gutter: bp === 'sm' ? 16 : 24,
    columns: bp === 'xl' ? 3 : bp === 'lg' ? 2 : bp === 'md' ? 2 : 1,
  };
}
