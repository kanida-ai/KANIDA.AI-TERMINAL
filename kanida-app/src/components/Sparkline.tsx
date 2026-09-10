/**
 * The mockups' small area sparkline: faint zero line, soft fill, emphasised
 * endpoint, green/red by direction.
 *
 * It plots `Spark.points` -- cumulative % to date. It is a HISTORY, never a
 * projection: the line stops at `as_of` and nothing is extrapolated past it.
 */
import { useMemo } from 'react';
import { View } from 'react-native';
import Svg, { Circle, Defs, LinearGradient, Path, Stop, Line as SvgLine } from 'react-native-svg';

import type { Spark } from '@/api/types';
import { useTheme } from '@/design/theme';

export function Sparkline({
  spark,
  width = 96,
  height = 34,
  /** grey the line out when the sample says the number is not evidence yet */
  muted = false,
  strokeWidth = 1.75,
}: {
  spark: Spark;
  width?: number;
  height?: number;
  muted?: boolean;
  strokeWidth?: number;
}) {
  const { c } = useTheme();
  const pts = spark.points;

  const geometry = useMemo(() => {
    const n = pts.length;
    const min = Math.min(...pts, 0);
    const max = Math.max(...pts, 0);
    const span = max - min || 1;
    const padY = 3;
    const x = (i: number) => (n === 1 ? 0 : (i / (n - 1)) * width);
    const y = (v: number) => padY + (1 - (v - min) / span) * (height - padY * 2);
    const line = pts.map((v, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(2)},${y(v).toFixed(2)}`).join(' ');
    const area = `${line} L${width},${height} L0,${height} Z`;
    return { line, area, zeroY: y(0), lastX: x(n - 1), lastY: y(pts[n - 1]) };
  }, [pts, width, height]);

  const last = pts[pts.length - 1] ?? 0;
  const stroke = muted ? c.textGreyed : last >= 0 ? c.positive : c.negative;
  const gradId = `spark-${Math.round(width)}-${last >= 0 ? 'p' : 'n'}-${muted ? 'm' : 'a'}`;

  return (
    <View accessibilityLabel="Cumulative virtual result to date" style={{ width, height }}>
      <Svg width={width} height={height}>
        <Defs>
          <LinearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
            <Stop offset="0" stopColor={stroke} stopOpacity={0.26} />
            <Stop offset="1" stopColor={stroke} stopOpacity={0} />
          </LinearGradient>
        </Defs>
        <SvgLine
          x1={0}
          x2={width}
          y1={geometry.zeroY}
          y2={geometry.zeroY}
          stroke={c.borderStrong}
          strokeWidth={1}
          strokeDasharray="2 3"
        />
        <Path d={geometry.area} fill={`url(#${gradId})`} />
        <Path
          d={geometry.line}
          stroke={stroke}
          strokeWidth={strokeWidth}
          fill="none"
          strokeLinejoin="round"
          strokeLinecap="round"
        />
        <Circle cx={geometry.lastX} cy={geometry.lastY} r={2.75} fill={stroke} />
      </Svg>
    </View>
  );
}
