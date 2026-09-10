import { Stack } from 'expo-router';

import { useTheme } from '@/design/theme';

export default function PathfinderLayout() {
  const { c } = useTheme();
  return (
    <Stack screenOptions={{ headerShown: false, contentStyle: { backgroundColor: c.bg } }} />
  );
}
