// THE WORKSPACE DESIGN SYSTEM — one scale for type, one for chrome. Every size on the workspace comes from here;
// nothing picks its own size to look different (docs/WORKSPACE_UX_AUDIT.md §5).
//
// Type: six roles, one family (Inter), one display face for the one page title (Manrope). Each step exists for a
// job, and the steps are close on purpose — a trading surface is read in scans, so hierarchy comes from weight,
// case and colour before it comes from size.
//
//   page      the workspace name, once per page                          Manrope 700 · 20/28
//   title     every widget title, identical in every header             Inter 600   · 13/18
//   label     section labels inside a widget (WHAT CHANGED, CALLS…)      Inter 500   · 10/14 · caps · +0.6
//   body      the reading text                                           Inter 400   · 12/18
//   helper    as-of lines, definitions, captions                          Inter 400   · 11/16 · muted
//   metric    a figure that IS the point of its row (never decoration)   Inter 600   · 15/20 · tabular
//
// The label scale is the Derivative tab's own `head` (frame.tsx), so a reused block's section labels and the
// workspace's are the same line of type.
import {C} from '../ui';

export const TYPE={
 page:{fontFamily:'ManropeBold',fontSize:20,lineHeight:28,letterSpacing:-0.3,color:C.ink},
 title:{fontFamily:'InterSemi',fontSize:13,lineHeight:18,color:C.ink},
 label:{fontFamily:'InterMedium',fontSize:10,lineHeight:14,letterSpacing:0.6,textTransform:'uppercase' as const,color:C.muted},
 body:{fontFamily:'Inter',fontSize:12,lineHeight:18,color:C.ink},
 helper:{fontFamily:'Inter',fontSize:11,lineHeight:16,color:C.muted},
 metric:{fontFamily:'InterSemi',fontSize:15,lineHeight:20,color:C.ink,fontVariant:['tabular-nums'] as any},
} as const;

// Chrome: identical on every widget.
export const CHROME={
 header:40,        // header bar height
 pad:12,           // inner padding
 gap:8,            // gap between blocks inside a widget
 radius:12,        // corner radius (the Derivative tab's own panels use 12)
 border:1,         // one hairline, C.line
 gutter:12,        // space between widgets, both axes
 control:28,       // square header control
 icon:14,          // header icon glyph
} as const;
