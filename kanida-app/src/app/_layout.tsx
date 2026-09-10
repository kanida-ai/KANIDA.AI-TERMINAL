import {
  DarkTheme as NavDark,
  DefaultTheme as NavLight,
  ThemeProvider as NavThemeProvider,
  Stack,
} from 'expo-router';
import { StatusBar } from 'expo-status-bar';
import { useColorScheme } from 'react-native';
import { SafeAreaProvider } from 'react-native-safe-area-context';

import { ThemeProvider } from '@/design/theme';
import { palettes } from '@/design/tokens';

/**
 * ONE Expo codebase -> iOS app + Android app + responsive web.
 *
 * There is no platform fork anywhere below this file: the same routes, the same
 * components and the same styles serve the phone app, the tablet, the desktop
 * browser and mobile web.
 */
export default function RootLayout() {
  const system = useColorScheme();
  const scheme: 'dark' | 'light' = system === 'light' ? 'light' : 'dark';
  const c = palettes[scheme];

  // Navigation's own theme, so the frame behind our screens is never white.
  const navTheme = {
    ...(scheme === 'dark' ? NavDark : NavLight),
    colors: {
      ...(scheme === 'dark' ? NavDark : NavLight).colors,
      background: c.bg,
      card: c.bg,
      text: c.text,
      border: c.border,
      primary: c.pathfinder,
    },
  };

  return (
    <SafeAreaProvider>
      <ThemeProvider>
        <NavThemeProvider value={navTheme}>
          <StatusBar style={scheme === 'dark' ? 'light' : 'dark'} />
          <Stack screenOptions={{ headerShown: false, contentStyle: { backgroundColor: c.bg } }}>
            <Stack.Screen name="(tabs)" />
            <Stack.Screen name="pathfinder" />
          </Stack>
        </NavThemeProvider>
      </ThemeProvider>
    </SafeAreaProvider>
  );
}
