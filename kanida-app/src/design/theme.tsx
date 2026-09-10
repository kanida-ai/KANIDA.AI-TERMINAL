/**
 * Theme access. Dark is the primary theme (the mockups); light is supported and
 * must stay accessible (FRONTEND_SPEC § States).
 *
 * `useTheme()` is the ONLY way a component gets a colour.
 */
import { createContext, useContext, useMemo, type ReactNode } from 'react';
import { useColorScheme } from 'react-native';

import { palettes, type Palette } from './tokens';

type ThemeValue = { scheme: 'dark' | 'light'; c: Palette };

const ThemeContext = createContext<ThemeValue>({ scheme: 'dark', c: palettes.dark });

export function ThemeProvider({ children }: { children: ReactNode }) {
  const system = useColorScheme();
  const scheme: 'dark' | 'light' = system === 'light' ? 'light' : 'dark';
  const value = useMemo(() => ({ scheme, c: palettes[scheme] }), [scheme]);
  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme(): ThemeValue {
  return useContext(ThemeContext);
}
