import React,{useEffect,useState} from 'react';
import {View,ScrollView,TextInput,useWindowDimensions} from 'react-native';
import {router,useLocalSearchParams} from 'expo-router';
import {C,T,Button,Chip,Badge,s} from '../ui';
import {api} from '../model';
import {useKeyedValue} from '../activeSymbol';
import {WidgetError} from '../layout';
import {Skeleton} from '../discover/parts';
import {HistoryPanel} from './HistoryPanel';
import {query,stateLabel,variantLabel,date} from './logic';
import type {Catalogue,Stocks,Selection} from './types';
const one=(value:unknown)=>typeof value==='string'?value:Array.isArray(value)?String(value[0]||''):'';
export function PatternHistoryPage(){
 const params=useLocalSearchParams(),{width}=useWindowDimensions();
 const [pattern,setPattern]=useState(one(params.pattern_id)||'CH16'),[variant,setVariant]=useState(one(params.variant)||'canonical'),[state,setState]=useState(one(params.phase)||one(params.state)||'confirmed');
 const [symbol,setSymbol]=useState(one(params.symbol).toUpperCase()),[bound,setBound]=useState(true),[search,setSearch]=useState(''),[q,setQ]=useState(''),[offset,setOffset]=useState(0);
 useEffect(()=>{const timer=setTimeout(()=>{setQ(search.trim());setOffset(0)},200);return()=>clearTimeout(timer)},[search]);
 const catalog=useKeyedValue<Catalogue>('pattern-history-catalogue',()=>api('/api/pattern-history/catalogue'));
 const stockUrl='/api/pattern-history/stocks?'+query({symbol:'',pattern_id:pattern,variant,state},{q,limit:40,offset});
 const stocks=useKeyedValue<Stocks>(catalog.value?stockUrl:'',()=>api(stockUrl));
 const selectedSymbol=symbol||stocks.value?.stocks[0]?.symbol||'';
 const selection:Selection={symbol:selectedSymbol,pattern_id:pattern,variant,state,...(bound&&one(params.strategy_key)?{strategy_key:one(params.strategy_key)}:{}),...(bound&&one(params.detection_id)?{detection_id:one(params.detection_id)}:{})};
 useEffect(()=>{if(!symbol&&stocks.value?.stocks[0])setSymbol(stocks.value.stocks[0].symbol)},[symbol,stocks.value]);
 useEffect(()=>{if(selectedSymbol)router.setParams({symbol:selectedSymbol,pattern_id:pattern,variant,phase:state,state:undefined,strategy_key:selection.strategy_key,detection_id:selection.detection_id})},[selectedSymbol,pattern,variant,state,selection.strategy_key,selection.detection_id]);
 const choosePattern=(id:string)=>{setPattern(id);setVariant('canonical');setState('confirmed');setBound(false);setOffset(0)};
 const variants=catalog.value?.patterns.filter(p=>p.pattern_id===pattern)||[],current=variants.find(p=>p.variant===variant),wide=width>=950;
 return <View style={{padding:width<700?14:26,gap:22,maxWidth:1500,width:'100%',alignSelf:'center'}}>
  <View style={[s.between,{flexWrap:'wrap'}]}><View style={{gap:5}}><Badge label="Pattern history · pilot" tone="neutral"/><T role="heading" aria-level={1} style={s.title}>See what followed.</T><T style={{color:C.muted}}>One stock. One pattern. Every measured outcome.</T></View><Button label="Back to Discover" kind="outline" icon="arrow-left" onPress={()=>router.push('/discover')}/></View>
  {catalog.error?<WidgetError title="Pattern history unavailable" message={catalog.error} onRetry={catalog.reload}/>:!catalog.value?<Skeleton lines={5}/>:<>
   <View style={[s.card,{padding:18,gap:14}]}>
    <View style={[s.row,{flexWrap:'wrap'}]}><Chip label="Double Bottom · chart pattern" active={pattern==='CH16'} onPress={()=>choosePattern('CH16')}/><Chip label="Bullish Engulfing · candlesticks" active={pattern==='CDLENGULFING'} onPress={()=>choosePattern('CDLENGULFING')}/><Badge label="Daily candles" tone="neutral"/></View>
    <T style={{fontSize:13,color:C.muted}}>{pattern==='CH16'?'Two troughs with a recovery between them. Forming history starts when the detector recognizes the structure; confirmed history starts after its neckline-break rule is met.':'A bullish candle body engulfs the previous bearish body. Forming / setup starts when the two-candle shape is recognized; confirmed history waits for the existing detector’s subsequent confirmation rule.'}</T>
    <View style={[s.row,{flexWrap:'wrap'}]}>{(current?.states||['setup','confirmed']).map(v=><Chip key={v} label={stateLabel(v)} active={state===v} onPress={()=>{setState(v);setBound(false);setOffset(0)}}/>)}</View>
    {variants.length>1&&<View style={[s.row,{flexWrap:'wrap'}]}>{variants.map(v=><Chip key={v.variant} label={variantLabel(v.variant)} active={variant===v.variant} onPress={()=>{setVariant(v.variant);setBound(false);setOffset(0)}}/>)}</View>}
    <T style={{fontSize:11,color:C.muted}}>The same frozen detector rules are used throughout. Each variant and state has its own history.</T>
    {bound&&!!selection.detection_id&&<T style={{color:C.amber,fontSize:12}}>Linked to your selected scanner detection. Changing the pattern, stock or state opens standalone historical research.</T>}
   </View>
   <View style={{flexDirection:wide?'row':'column',gap:22,alignItems:'stretch'}}>
    <View style={[s.card,{width:wide?280:undefined,padding:16,gap:12,alignSelf:wide?'flex-start':undefined}]}><T style={{fontFamily:'InterSemi'}}>Choose a stock</T><TextInput accessibilityLabel="Search historical stocks" placeholder="Search symbol" placeholderTextColor={C.muted} value={search} onChangeText={setSearch} autoCapitalize="characters" style={[s.input,{minHeight:42,fontSize:13}]}/>
     {stocks.error?<WidgetError title="Stocks unavailable" message={stocks.error} onRetry={stocks.reload}/>:stocks.loading?<Skeleton lines={5}/>:<><T style={{fontSize:11,color:C.muted}}>{stocks.value?.total??0} stocks · ordered by latest occurrence</T><ScrollView style={{maxHeight:wide?510:190}} contentContainerStyle={{gap:4}}>{stocks.value?.stocks.map(stock=><Button key={stock.symbol} label={`${stock.symbol} · ${stock.occurrence_count}`} kind={stock.symbol===selectedSymbol?'soft':'ghost'} accessibilityState={{selected:stock.symbol===selectedSymbol}} accessibilityLabel={`${stock.symbol}, ${stock.occurrence_count} occurrences, last seen ${date(stock.last_seen)}`} onPress={()=>{setSymbol(stock.symbol);setBound(false)}} style={{justifyContent:'flex-start'}}/>)}{!stocks.value?.stocks.length&&<T style={{color:C.muted}}>No stocks match this search.</T>}</ScrollView><View style={s.between}><Button kind="ghost" label="Previous" disabled={offset===0} onPress={()=>setOffset(v=>Math.max(0,v-40))}/><Button kind="ghost" label="Next" disabled={offset+40>=(stocks.value?.total||0)} onPress={()=>setOffset(v=>v+40)}/></View></>}
    </View>
    <View style={[s.card,{flex:1,minWidth:0,padding:wide?24:16}]}>{selectedSymbol?<><View style={{gap:4}}><T role="heading" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:24,lineHeight:32}}>{selectedSymbol} · {current?.name||pattern}</T><T style={{fontSize:12,color:C.muted}}>{stateLabel(state)} · {variantLabel(variant)} · Daily</T></View><HistoryPanel key={query(selection)} selection={selection} initialHorizon={Number(one(params.horizon))||5} onHorizonChange={h=>router.setParams({horizon:String(h)})}/></>:<T style={{color:C.muted}}>Select a stock to explore its historical occurrences.</T>}</View>
   </View>
  </>}
 </View>;
}
