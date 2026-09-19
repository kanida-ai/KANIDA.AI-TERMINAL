export type ChartWindow = 'pattern' | 'recent' | 'all';

// Windowing affects presentation only; detectors and evidence still use all bars.
export function chartStart(length:number, mode:ChartWindow, patternStart?:number, anchors:number[]=[]){
 if(length<=0 || mode==='all')return 0;
 if(mode==='recent')return Math.max(0,length-40);
 const valid=[patternStart,...anchors].filter((i):i is number=>Number.isFinite(i)&&Number(i)>=0&&Number(i)<length);
 const first=valid.length?Math.floor(Math.min(...valid)):length-50;
 return Math.max(0,Math.min(length-50,first-8));
}
