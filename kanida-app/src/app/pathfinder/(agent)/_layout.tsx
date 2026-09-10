import { Slot } from 'expo-router';
import { View } from 'react-native';

import { AgentHeader, SubTabs, type SubTab } from '@/components/agent';
import { useTheme } from '@/design/theme';

/**
 * The Pathfinder agent shell: header + sub-tabs, shared by Overview,
 * Experiments, Learnings and Strategy Lab.
 *
 * Experiment DETAIL deliberately sits outside this group (it is a pushed screen
 * with its own header), which is why the four tabs live in a route group.
 */
const TABS: SubTab[] = [
  { key: 'overview', label: 'Overview', href: '/pathfinder/overview' },
  { key: 'experiments', label: 'Experiments', href: '/pathfinder/experiments' },
  { key: 'learnings', label: 'Learnings', href: '/pathfinder/learnings' },
  { key: 'lab', label: 'Strategy Lab', href: '/pathfinder/lab' },
];

export default function PathfinderAgentLayout() {
  const { c } = useTheme();
  return (
    <View style={{ flex: 1, backgroundColor: c.bg }}>
      <AgentHeader
        name="Pathfinder Agent"
        mandate="Discover repeatable market edges · Test → Validate → Evolve"
      />
      <SubTabs tabs={TABS} />
      <Slot />
    </View>
  );
}
