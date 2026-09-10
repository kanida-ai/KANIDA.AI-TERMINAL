import { Redirect } from 'expo-router';

/** /pathfinder lands on the Overview tab. */
export default function PathfinderIndex() {
  return <Redirect href="/pathfinder/overview" />;
}
