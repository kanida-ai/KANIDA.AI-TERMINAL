import {visualPattern,isResearchPattern,drawingEnd} from './researchDrawing';
export type Pivot={index:number;value:number;kind?:'high'|'low'};
const at=(line:any,index:number)=>{const [a,b]=[line.points[0],line.points[line.points.length-1]];return a.value+(b.value-a.value)*(index-a.index)/(b.index-a.index||1)};
export function patternGeometry(found:any,bars:any[]){
 const lines=found?.lines||[];const boundaries=lines.filter((l:any)=>l.role==='boundary'&&l.points.length>1);
 const top=boundaries.find((l:any)=>/resistance/i.test(l.label)),bottom=boundaries.find((l:any)=>/support/i.test(l.label));
 const first=Math.max(found?.start_index||0,...boundaries.map((l:any)=>l.points[0].index));
 const last=drawingEnd(found,bars.length),visual=visualPattern(found?.pattern);
 let points:Pivot[]=lines.filter((l:any)=>l.role==='anchors').flatMap((l:any)=>l.points.map((p:Pivot)=>({...p,kind:/lower/i.test(l.label)?'low':'high'})));
 const family=['symmetrical_triangle','descending_triangle','falling_wedge','rising_wedge','channel','flag_pole','horizontal_breakout'].includes(visual);
 // Only published anchors describe the detected structure; never infer extra chart pivots.
 points=points.filter(p=>Number.isFinite(p.value)&&p.index>=first&&p.index<=last-3).sort((a,b)=>a.index-b.index);
 const swings:Pivot[]=[];
 for(const point of points){const previous=swings[swings.length-1];
  if(previous?.index===point.index)continue; // OHLC cannot order two turns inside one candle.
  if(previous?.kind===point.kind){if(point.kind==='high'?point.value>previous.value:point.value<previous.value)swings[swings.length-1]=point}
  else swings.push(point);
 }
 let envelope:Pivot[]=[],projection:Pivot[][]=[],apex:Pivot|null=null;
 if(top&&bottom){
  const bounded=Number.isFinite(found?.end_index),end=bounded?Math.min(last,top.points[top.points.length-1].index,bottom.points[bottom.points.length-1].index):last;
  envelope=end>=first?[{index:first,value:at(top,first)},{index:end,value:at(top,end)},{index:end,value:at(bottom,end)},{index:first,value:at(bottom,first)}]:[];
  const gap=at(top,last)-at(bottom,last),closing=(at(top,last)-at(top,last-1))-(at(bottom,last)-at(bottom,last-1));
  const index=closing<0?last-gap/closing:NaN;
  if((!bounded||last===bars.length-1)&&visual!=='channel'&&visual!=='flag_pole'&&Number.isFinite(index)&&index>last&&index-last<=Math.max(12,(last-first+10)*.7)){
   apex={index,value:at(top,index)};
   projection=[[{index:last,value:at(top,last)},apex],[{index:last,value:at(bottom,last)},apex]];
  }
 }
 const shapeLabels=lines.filter((l:any)=>l.role==='label').map((l:any)=>({label:l.label,point:l.points[0]}));
 return {swings:family?swings:[],envelope,projection,apex,shapeLabels};
}
// Legend readout (Phase 0.3). Presentation only: values come from the server's detector lines, never from new statistics.
export type BoundaryValue={label:string;value:number|null};
// Piecewise-linear value of one detector line at a bar index. null outside the line's own span (no back-filling, no extension).
export function lineValueAt(line:any,index:number):number|null{
 const pts=(line?.points||[]).filter((p:any)=>Number.isFinite(p?.index)&&Number.isFinite(p?.value));
 if(!pts.length||!Number.isFinite(index))return null;
 if(pts.length===1)return pts[0].index===index?pts[0].value:null;
 if(index<pts[0].index||index>pts[pts.length-1].index)return null;
 for(let i=1;i<pts.length;i++){const a=pts[i-1],b=pts[i];if(index<=b.index)return b.index===a.index?b.value:a.value+(b.value-a.value)*(index-a.index)/(b.index-a.index)}
 return null;
}
// Q4 point-in-time guard for the hover readout. Detector lines are fitted on the whole snapshot (market_scanner/detectors.py: boundary segments span [start, n-1], pivots need 3 later closed bars)
// and the match is emitted with end_index = n-1, so a line value at an earlier bar is hindsight. The confirmation index is the LATEST of every line point index and the match's end_index
// (whichever the server sent); null when neither exists (then no boundary values are shown at all).
export function boundaryConfirmIndex(found:any):number|null{
 const idx:number[]=(found?.lines||[]).flatMap((l:any)=>(l?.points||[]).map((p:any)=>p?.index)).filter((v:any)=>Number.isFinite(v));
 if(Number.isFinite(found?.end_index))idx.push(found.end_index);
 return idx.length?Math.max(...idx):null;
}
// Boundary values of one /api/chart match (`{lines:[{label,role,points:[{index,value}]}]}`) at a bar index: every 'boundary' line
// (Resistance, Support, Rim resistance, Neckline, Flag resistance/support) plus the cup-and-handle handle low, which exists only
// from the handle's own low bar onward. Values outside a line's span are null, shown as "n/a".
export function boundaryValuesAt(found:any,index:number):BoundaryValue[]{
 const lines=found?.lines||[];const out:BoundaryValue[]=lines.filter((l:any)=>l?.role==='boundary'&&l.points?.length>1).map((l:any)=>({label:String(l.label),value:lineValueAt(l,index)}));
 const handle=lines.find((l:any)=>l?.label==='Handle'&&l.points?.length>=3);
 if(handle){const low=handle.points[1];out.push({label:'Handle low',value:Number.isFinite(low?.value)&&Number.isFinite(index)&&index>=low.index&&index<=handle.points[handle.points.length-1].index?low.value:null})}
 return out;
}
