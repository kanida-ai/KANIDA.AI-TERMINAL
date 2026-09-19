// Discover deep links (spec §3.7): /discover?b=<blockKey>&a=<strategyKey>&bb=<strategyKey>&sel=<A|B>&s=<SYMBOL>. Pure; no imports.
export type DiscoverDeepLink={block:string;a:string;bb:string;sel:'A'|'B';symbol:string;sig:string};
const KEY_RE=/^[A-Za-z0-9_.:-]{1,80}$/,SYM_RE=/^[A-Z0-9][A-Z0-9&._-]{0,39}$/,TF_RE=/^[0-9]{1,3}[A-Z]{1,2}$/;
const one=(v:unknown)=>{const x=Array.isArray(v)?v[0]:v;return typeof x==='string'?x.trim():''};
const key=(v:unknown)=>{const x=one(v);return KEY_RE.test(x)?x:''};
/** Invalid parts are dropped (never guessed). null when nothing usable is present. `sig` identifies one link so it is applied once. */
export function parseDiscoverParams(p:Record<string,unknown>|null|undefined):DiscoverDeepLink|null{
 const block=key(p?.b),a=key(p?.a),bb=key(p?.bb),symRaw=one(p?.s).toUpperCase(),symbol=SYM_RE.test(symRaw)?symRaw:'',sel=one(p?.sel).toUpperCase()==='B'?'B':'A';
 if(!block&&!a&&!bb&&!symbol)return null;
 return {block,a,bb,sel,symbol,sig:[block,a,bb,sel,symbol].join('|')};
}
export const paramsSig=(p:Record<string,unknown>|null|undefined)=>['b','a','bb','sel','s'].map(k=>one(p?.[k])).join('|');
export function discoverHref(l:{block?:string;a?:string;bb?:string;sel?:'A'|'B';symbol?:string}){const q=[['b',l.block],['a',l.a],['bb',l.bb],['sel',l.sel],['s',l.symbol]].filter(([,v])=>!!v).map(([k,v])=>`${k}=${encodeURIComponent(v as string)}`);return '/discover'+(q.length?'?'+q.join('&'):'');}
/** Full chart workspace link (W4 route): /chart?s=&tf=&m=[&tab=]. Empty string when symbol/timeframe are invalid. */
export function chartHref(symbol:string,timeframe:string,matchId?:string,tab?:string,detectionId?:string|null){
 const s=(symbol||'').toUpperCase(),tf=(timeframe||'').toUpperCase();if(!SYM_RE.test(s)||!TF_RE.test(tf))return '';
 return `/chart?s=${encodeURIComponent(s)}&tf=${encodeURIComponent(tf)}${matchId?`&m=${encodeURIComponent(matchId)}`:''}${tab?`&tab=${encodeURIComponent(tab)}`:''}${matchId&&detectionId?`&d=${encodeURIComponent(detectionId)}`:''}`;
}
