// "A WIDGET FITS ITS BOX" — the workspace's scrolling rule (docs/WORKSPACE_UX_AUDIT.md §5b).
//
// Benchmarked on TrendSpider's dashboard (22 Sep 2026): chart and map panels never scroll (11 of 11); list and table
// panels do, but you never see it — tables hide the bar entirely, lists show a 5px hairline — and no panel ever
// shows a horizontal bar. That is most of why the surface reads as calm.
//
// KANIDA's version, applied to everything inside the workspace, the reused Derivative blocks included (no file of
// theirs is touched — this is one stylesheet scoped to the workspace root):
//   · no scrollbar track, ever; a 4px thumb appears only while the pointer is over the scrolling area
//   · no horizontal scrollbar inside a widget (content that is wider still pans with a trackpad)
//   · charts, figures and summaries are laid out to FIT their tile (widgets.tsx FIT) — only lists scroll
import {Platform} from 'react-native';

const ID='kanida-workspace-scroll';
const CSS=`
[data-kw-root] *{scrollbar-width:thin;scrollbar-color:transparent transparent}
[data-kw-root] *:hover{scrollbar-color:#2B3C47 transparent}
[data-kw-root] *::-webkit-scrollbar{width:4px;height:0;background:transparent}
[data-kw-root] *::-webkit-scrollbar-track{background:transparent}
[data-kw-root] *::-webkit-scrollbar-thumb{background:transparent;border-radius:4px}
[data-kw-root] *:hover::-webkit-scrollbar-thumb{background:#2B3C47}
[data-kw-root] [data-kw-tile] *{overscroll-behavior:contain}
`;

/** Install the workspace scrolling rule once; returns the remover (the page unmounts → the rule goes). */
export function installScrollRule(){
 if(Platform.OS!=='web'||typeof document==='undefined')return ()=>{};
 if(document.getElementById(ID))return ()=>{};
 const el=document.createElement('style');el.id=ID;el.textContent=CSS;document.head.appendChild(el);
 return ()=>{el.remove();};
}
