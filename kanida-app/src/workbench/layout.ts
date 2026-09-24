// THE LAYOUT ENGINE — pure, no React, no DOM. It turns an ordered list of widgets into composed rows.
//
// The model is the one the best terminals use (benchmarked against TrendSpider's dashboard, 22 Sep 2026): the
// workspace is ROWS, and the widgets of a row SHARE its width by weight. A row never wraps, so adding a widget
// never drops it onto an empty line of its own; the row's widgets give up space instead, or — when they would
// fall below their minimum width — the whole list is re-partitioned into balanced rows.
//
//   size → weight    S 1 · M 2 · L 3   (a share of the row, not a pixel width)
//                    F = the row to itself
//   size → minimum   S 250 · M 300 · L 400 px — below that a widget is not usable, so the engine adds a row
//
// PARTITION. The widgets keep their order. The engine picks the FEWEST rows that respect every widget's minimum
// and the column cap, and among those the split whose rows carry the most EVEN weight (the classic linear-
// partition, solved exactly — the list is at most 24 long). So 4 widgets become 2 + 2, not 3 + 1; 6 become 3 + 3;
// 5 become 3 + 2 with the larger row first.
//
// HEIGHT. When every row fits the screen at a usable height, the rows share the screen: nobody has to scroll
// because one more widget was added. Past that, rows take one standard height and the page scrolls — rows are
// never squeezed below a readable minimum.
export type LSize='S'|'M'|'L'|'F';
/** `min` is the widget's OWN minimum when its content needs more than its size class gives (an option chain's
 *  columns, a strike grid); the engine never lays it out narrower than the larger of the two. */
export type LItem={id:string;size:LSize;tall?:boolean;min?:number};
const minOf=(it:LItem)=>Math.max(MIN_W[it.size],it.min||0);
export type LRow={items:{id:string;size:LSize;weight:number;min:number}[];height:number};
export type LOptions={width:number;viewport:number;gap?:number;columns?:number|'auto';phone?:boolean};

export const WEIGHT:Record<Exclude<LSize,'F'>,number>={S:1,M:2,L:3};
export const MIN_W:Record<LSize,number>={S:250,M:300,L:400,F:400};
export const ROW={min:320,standard:440,tall:520,max:760,phone:420,phoneTall:560} as const;
const SIZES:Exclude<LSize,'F'>[]=['S','M','L'];

function weightOf(s:LSize){return s==='F'?1:WEIGHT[s];}

/** The fewest, most even rows for one run of widgets (no Full widget inside it). */
export function partition(items:LItem[],width:number,gap:number,cap:number):LItem[][]{
 const n=items.length;
 if(!n)return [];
 const fits=(i:number,j:number)=>{   // items[i..j) in one row?
  const k=j-i;if(k>cap)return false;
  let min=0;for(let x=i;x<j;x++)min+=minOf(items[x]);
  return min+gap*(k-1)<=width||k===1;
 };
 const w=items.map(it=>weightOf(it.size));
 const pre=[0];for(const x of w)pre.push(pre[pre.length-1]+x);
 const total=pre[n];
 for(let rows=1;rows<=n;rows++){
  const target=total/rows;
  // best[r][j]: min cost splitting items[0..j) into r rows; cost = Σ (rowWeight − target)²
  const INF=Number.POSITIVE_INFINITY;
  const best:number[][]=Array.from({length:rows+1},()=>Array(n+1).fill(INF));
  const from:number[][]=Array.from({length:rows+1},()=>Array(n+1).fill(-1));
  best[0][0]=0;
  for(let r=1;r<=rows;r++)for(let j=r;j<=n;j++)for(let i=r-1;i<j;i++){
   if(best[r-1][i]===INF||!fits(i,j))continue;
   const d=pre[j]-pre[i]-target;
   // ties go to the split that puts MORE widgets in the earlier row (the top of the page fills first)
   const cost=best[r-1][i]+d*d-(j-i)*1e-6;
   if(cost<best[r][j]){best[r][j]=cost;from[r][j]=i;}
  }
  if(best[rows][n]===INF)continue;
  const out:LItem[][]=[];let j=n;
  for(let r=rows;r>=1;r--){const i=from[r][j];out.unshift(items.slice(i,j));j=i;}
  return out;
 }
 return items.map(it=>[it]);
}

/** How many widgets a row may hold at this width: the user's choice, or as many as fit at the smallest size. */
export function capacity(width:number,gap:number,columns:number|'auto'|undefined){
 const auto=Math.max(1,Math.floor((width+gap)/(MIN_W.S+gap)));
 return columns&&columns!=='auto'?Math.max(1,Math.min(columns,auto)):Math.min(auto,4);
}

export function layout(items:LItem[],o:LOptions):LRow[]{
 const gap=o.gap??12;
 if(o.phone){
  return items.map(it=>({items:[{id:it.id,size:it.size,weight:1,min:0}],height:it.tall||it.size==='L'||it.size==='F'?ROW.phoneTall:ROW.phone}));
 }
 const cap=capacity(o.width,gap,o.columns);
 const groups:LItem[][]=[];let run:LItem[]=[];
 for(const it of items){
  if(it.size==='F'){groups.push(...partition(run,o.width,gap,cap));run=[];groups.push([it]);}
  else run.push(it);
 }
 groups.push(...partition(run,o.width,gap,cap));
 const count=groups.length;
 // share the screen when every row can have a usable height; otherwise one standard height and scroll
 const shared=count?Math.floor((o.viewport-gap*(count-1))/count):0;
 const fit=count>0&&shared>=ROW.min;
 return groups.map(g=>{
  const tall=g.some(x=>x.tall);
  const height=fit?Math.min(ROW.max,Math.max(shared,ROW.min)):(tall?ROW.tall:ROW.standard);
  return {height,items:g.map(x=>({id:x.id,size:x.size,weight:g.length===1?1:weightOf(x.size),min:g.length===1?0:minOf(x)}))};
 });
}

/** Resizing by the edge between two neighbours: the pair of sizes whose split is nearest the dragged one. */
export function snapPair(leftPx:number,rightPx:number):[Exclude<LSize,'F'>,Exclude<LSize,'F'>]{
 const want=leftPx/Math.max(1,leftPx+rightPx);
 let best:[Exclude<LSize,'F'>,Exclude<LSize,'F'>]=['M','M'],d=Infinity;
 for(const a of SIZES)for(const b of SIZES){
  const got=WEIGHT[a]/(WEIGHT[a]+WEIGHT[b]);
  // equal splits are preferred on a tie (M+M over S+S over L+L): the calmest layout wins
  const pen=Math.abs(got-want)+(a===b&&a!=='M'?1e-3:0);
  if(pen<d){d=pen;best=[a,b];}
 }
 return best;
}
