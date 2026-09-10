/**
 * Who wrote a narrative. Not a footnote -- the core principle made visible: the
 * engine computes, a model may narrate, and the reader can always tell which
 * they are looking at.
 */
import type { Narrative, StoryLine } from '@/api/types';
import { useTheme } from '@/design/theme';
import { space } from '@/design/tokens';
import { timeIST } from '@/lib/format';

import { Pill, Row, Txt } from './ui';

/** Model ids are internal; the reader sees a plain name. */
export function modelName(model: string | null | undefined): string {
  if (!model) return '';
  if (model.startsWith('claude-sonnet')) return 'Claude Sonnet';
  if (model.startsWith('claude-haiku')) return 'Claude Haiku';
  if (model.startsWith('claude-opus')) return 'Claude Opus';
  return model;
}

export function Attribution({ line, compact = false }: { line: Pick<Narrative | StoryLine, 'produced_by' | 'model' | 'at'>; compact?: boolean }) {
  const { c } = useTheme();
  if (line.produced_by === 'engine') {
    return (
      <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
        <Pill fg={c.textMuted} bordered>
          Engine-narrated
        </Pill>
        {!compact ? (
          <Txt variant="caption" tone="muted">
            Computed, then templated. No model in the loop. {timeIST(line.at)}
          </Txt>
        ) : null}
      </Row>
    );
  }
  if (line.produced_by === 'human') {
    return (
      <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
        <Pill fg={c.caution} bordered>
          Human decision
        </Pill>
        {!compact ? (
          <Txt variant="caption" tone="muted">
            {timeIST(line.at)}
          </Txt>
        ) : null}
      </Row>
    );
  }
  return (
    <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
      <Pill fg={c.pathfinder} bg={c.pathfinderSoft}>
        {modelName(line.model)}
      </Pill>
      {!compact ? (
        <Txt variant="caption" tone="muted">
          Narrated the engine{'’'}s numbers under the engine{'’'}s headline · every figure is computed, not
          written · {timeIST(line.at)}
        </Txt>
      ) : null}
    </Row>
  );
}
