// Presentation helpers. Detector geometry remains the source of all structural points.
const LEGACY:Record<string,string>={CH01:'cup_handle',CH02:'horizontal_breakout',CH03:'flag_pole',CH04:'symmetrical_triangle',CH05:'falling_wedge',CH06:'rising_wedge',CH07:'channel',CH08:'descending_triangle',CH09:'head_shoulders',CH10:'inverse_head_shoulders'};
export const visualPattern=(id:string)=>LEGACY[id]||id;
export const isResearchPattern=(id:string)=>/^(CH\d{2}|HA\d{2}|PA\d{2}|CDL[A-Z0-9]+)$/.test(id||'');
export const isOriginalChart=(id:string)=>!!LEGACY[id]||Object.values(LEGACY).includes(id);
export function drawingEnd(found:any,length:number){return Number.isFinite(found?.end_index)?Math.max(0,Math.min(length-1,found.end_index)):length-1;}
export function visibleDrawingValues(found:any,start:number,end:number):number[]{
 return (found?.lines||[]).flatMap((line:any)=>(line.points||[]).filter((p:any)=>Number.isFinite(p.index)&&p.index>=start&&p.index<=end&&Number.isFinite(p.value)).map((p:any)=>p.value));
}
export function candleRanges(found:any){
 return (found?.lines||[]).filter((l:any)=>l.role==='candle_range'&&l.points?.length===2&&l.points.every((p:any)=>Number.isFinite(p.index)&&Number.isFinite(p.value))).map((l:any)=>({label:l.label,first:Math.min(l.points[0].index,l.points[1].index),last:Math.max(l.points[0].index,l.points[1].index),low:Math.min(l.points[0].value,l.points[1].value),high:Math.max(l.points[0].value,l.points[1].value)}));
}
export function drawingColor(line:any){return /failure|invalid|stop/i.test(line.label)?'#F17D87':/trigger|confirm/i.test(line.label)?'#EBC66B':/target|PRZ/i.test(line.label)?'#7FB7FF':line.role==='curve'?'#B4E6FF':line.role==='boundary'?'#68F2C2':'#DFE8AA';}
export function hasDrawing(found:any){return !!found?.lines?.some((l:any)=>l.points?.length>0);}
export type DrawingLabel={label:string;fullLabel:string;x:number;y:number;width:number;anchorX:number;anchorY:number;moved:boolean;hidden:boolean};
// Conservative bounds for 9px SVG text, including a small margin on either side.
const labelWidth=(text:string)=>6+[...text].reduce((n,c)=>n+(/[MW@%]/.test(c)?8.5:/[ilI1.,: '\/]/.test(c)?3:/[A-Z0-9]/.test(c)?6.5:5.5),0);
const shortLabel=(text:string)=>text==='Pattern recognized'?'Recognized':text==='Confirmation'?'Confirmed':text.replace('Left shoulder','L shoulder').replace('Right shoulder','R shoulder');
/** Place text rectangles once, nearest their immutable price anchors. Candidate rows never
 * oscillate; every accepted rectangle is tested against all earlier text. Hidden overflow
 * retains its fullLabel for an accessible text fallback when a plot is too small to fit it. */
export function placedLabels(labels:any[],x:(i:number)=>number,y:(v:number)=>number,left:number,right:number,top:number,bottom=top+Math.max(140,labels.length*16)){
 const placed:DrawingLabel[]=[],gap=4,rowHeight=14,minY=top+11,maxY=bottom-3,available=right-left;
 for(const l of labels){if(!l.point||!Number.isFinite(l.point.index)||!Number.isFinite(l.point.value))continue;
  const anchorX=x(l.point.index),anchorY=y(l.point.value);if(!Number.isFinite(anchorX)||!Number.isFinite(anchorY))continue;
  const fullLabel=String(l.label),label=shortLabel(fullLabel),width=labelWidth(label),minX=left+width/2,maxX=right-width/2;
  const targetX=Math.max(minX,Math.min(maxX,anchorX)),targetY=Math.max(minY,Math.min(maxY,anchorY-10));
  const base={label,fullLabel,width,anchorX,anchorY},visible=placed.filter(p=>!p.hidden);
  let best:{x:number;y:number;distance:number}|undefined;
  if(width<=available&&maxY>=minY){
   // Existing rectangle edges are the only extra x positions needed to find the nearest free gap.
   const xs=[targetX,minX,maxX,...visible.flatMap(p=>[p.x-(p.width+width)/2-gap,p.x+(p.width+width)/2+gap])].filter(px=>px>=minX&&px<=maxX);
   const ys=[targetY,...Array.from({length:Math.floor((maxY-minY)/rowHeight)+1},(_,i)=>minY+i*rowHeight),...visible.flatMap(p=>[p.y-rowHeight,p.y+rowHeight])].filter(py=>py>=minY&&py<=maxY);
   for(const py of ys)for(const px of xs){
    if(visible.some(p=>Math.abs(p.x-px)<(p.width+width)/2+gap-.001&&Math.abs(p.y-py)<rowHeight-.001))continue;
    const distance=(px-targetX)**2+(py-targetY)**2;
    if(!best||distance<best.distance)best={x:px,y:py,distance};
   }
  }
  if(best)placed.push({...base,x:best.x,y:best.y,moved:Math.abs(best.x-anchorX)>4||Math.abs(best.y-(anchorY-10))>4,hidden:false});
  else placed.push({...base,x:anchorX,y:anchorY,moved:false,hidden:true});
 }return placed;
}

// Published path coordinates pass through unchanged, including the detector's fitted cup.
export const drawingLines=(found:any)=>(found?.lines||[]).filter((line:any)=>!['anchors','candle_range'].includes(line.role)&&line.points?.length>1);
