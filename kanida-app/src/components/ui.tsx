/**
 * The primitives every Pathfinder screen is built from.
 *
 * Plain React Native + StyleSheet -- which react-native-web renders on the web
 * target unchanged. That is what makes ONE codebase actually mean one codebase:
 * there is no `.web.tsx` fork anywhere in this app.
 */
import { forwardRef, type ReactNode } from 'react';
import {
  Platform,
  Pressable,
  StyleSheet,
  Text,
  View,
  type PressableProps,
  type StyleProp,
  type TextProps,
  type TextStyle,
  type ViewProps,
  type ViewStyle,
} from 'react-native';

import { useTheme } from '@/design/theme';
import { elevation, fontFamily, radius, space, tabularNums, type as typeScale } from '@/design/tokens';

type Variant = keyof typeof typeScale;
type Tone =
  | 'default'
  | 'secondary'
  | 'muted'
  | 'greyed'
  | 'positive'
  | 'negative'
  | 'caution'
  | 'pathfinder'
  | 'inherit';

export type TxtProps = TextProps & {
  variant?: Variant;
  tone?: Tone;
  /** tabular numerals -- set on anything containing a figure */
  numeric?: boolean;
  mono?: boolean;
  color?: string;
  children?: ReactNode;
};

/** The only text component. Every string in the app goes through it. */
export function Txt({
  variant = 'body',
  tone = 'default',
  numeric = false,
  mono = false,
  color,
  style,
  ...rest
}: TxtProps) {
  const { c } = useTheme();
  const toneColor =
    color ??
    (tone === 'secondary'
      ? c.textSecondary
      : tone === 'muted'
        ? c.textMuted
        : tone === 'greyed'
          ? c.textGreyed
          : tone === 'positive'
            ? c.positive
            : tone === 'negative'
              ? c.negative
              : tone === 'caution'
                ? c.caution
                : tone === 'pathfinder'
                  ? c.pathfinder
                  : tone === 'inherit'
                    ? undefined
                    : c.text);
  return (
    <Text
      {...rest}
      style={[
        { fontFamily: mono ? fontFamily.mono : fontFamily.sans },
        typeScale[variant] as TextStyle,
        toneColor ? { color: toneColor } : null,
        numeric ? tabularNums : null,
        style,
      ]}
    />
  );
}

/** Small UPPERCASE label with letter-spacing -- the mockups' section markers. */
export function Label({ children, tone = 'muted', style }: { children: ReactNode; tone?: Tone; style?: StyleProp<TextStyle> }) {
  return (
    <Txt variant="label" tone={tone} style={style}>
      {children}
    </Txt>
  );
}

export type CardProps = ViewProps & {
  /** a nested card sitting on another card */
  raised?: boolean;
  /** left edge accent, e.g. the beat colour on a story block */
  accent?: string;
  padded?: boolean;
};

export const Card = forwardRef<View, CardProps>(function Card(
  { raised = false, accent, padded = true, style, ...rest },
  ref,
) {
  const { c } = useTheme();
  return (
    <View
      ref={ref}
      {...rest}
      style={[
        {
          backgroundColor: raised ? c.surfaceRaised : c.surface,
          borderRadius: radius.card,
          borderWidth: StyleSheet.hairlineWidth * 2,
          borderColor: c.border,
          padding: padded ? space.xl : 0,
          overflow: 'hidden',
        },
        raised ? null : (elevation.card as ViewStyle),
        accent ? { borderLeftWidth: 3, borderLeftColor: accent } : null,
        style,
      ]}
    />
  );
});

/** A pill: status badges, n-flags, level chips. */
export function Pill({
  children,
  fg,
  bg,
  bordered = false,
  style,
}: {
  children: ReactNode;
  fg: string;
  bg?: string;
  bordered?: boolean;
  style?: StyleProp<ViewStyle>;
}) {
  return (
    <View
      style={[
        {
          alignSelf: 'flex-start',
          paddingHorizontal: space.sm + 2,
          paddingVertical: 4,
          borderRadius: radius.pill,
          backgroundColor: bg ?? 'transparent',
          borderWidth: bordered ? StyleSheet.hairlineWidth * 2 : 0,
          borderColor: fg,
        },
        style,
      ]}>
      <Txt variant="label" color={fg}>
        {children}
      </Txt>
    </View>
  );
}

export function Divider({ style, inset = 0 }: { style?: StyleProp<ViewStyle>; inset?: number }) {
  const { c } = useTheme();
  return (
    <View
      style={[
        { height: StyleSheet.hairlineWidth * 2, backgroundColor: c.border, marginLeft: inset },
        style,
      ]}
    />
  );
}

export function Row({ style, gap = space.sm, ...rest }: ViewProps & { gap?: number }) {
  return <View {...rest} style={[{ flexDirection: 'row', alignItems: 'center', gap }, style]} />;
}

export function Stack({ style, gap = space.md, ...rest }: ViewProps & { gap?: number }) {
  return <View {...rest} style={[{ gap }, style]} />;
}

/**
 * Pressable with a visible focus ring on web/keyboard (FRONTEND_SPEC § States:
 * accessible focus in both themes) and a calm press state on touch.
 */
export function Touchable({ style, children, ...rest }: PressableProps & { children: ReactNode }) {
  const { c } = useTheme();
  return (
    <Pressable
      {...rest}
      style={(state) => [
        { opacity: state.pressed ? 0.72 : 1 },
        // `focused` is provided by react-native-web and RN 0.7x on native
        (state as { focused?: boolean }).focused
          ? Platform.select({
              web: { outlineStyle: 'solid', outlineWidth: 2, outlineColor: c.focus, outlineOffset: 2 } as object,
              default: { borderColor: c.focus },
            })
          : null,
        typeof style === 'function' ? style(state) : style,
      ]}>
      {children}
    </Pressable>
  );
}

/** A definition row: small label above, value below. Used all over the detail screen. */
export function Field({
  label,
  children,
  style,
}: {
  label: string;
  children: ReactNode;
  style?: StyleProp<ViewStyle>;
}) {
  return (
    <View style={[{ gap: 4 }, style]}>
      <Label>{label}</Label>
      {typeof children === 'string' ? <Txt variant="body">{children}</Txt> : children}
    </View>
  );
}

export const styles = StyleSheet.create({
  center: { alignItems: 'center', justifyContent: 'center' },
  fill: { flex: 1 },
  spread: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between' },
  wrap: { flexWrap: 'wrap' },
});
