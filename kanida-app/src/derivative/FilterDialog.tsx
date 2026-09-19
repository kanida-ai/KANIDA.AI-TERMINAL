// The "Customize (N filters)…" popup — the benchmark's filter builder: rows of Column ▾ · Operator ▾ · Value ▾ with
// an ✕ each, and ADD FILTER · CLEAR ALL · APPLY underneath.
//
// The one rule that shapes every choice offered here: a rule exists only if the pilot can answer it. The column list,
// the operators inside each column and the values are all taken from what /api/derivatives/* already validates
// (logic.FILTER_COLUMNS), so what the table shows is always exactly what the server returned. Nothing is filtered in
// the browser — a quietly narrowed list would sit under another list's as-of time and liquidity floors.
import React,{useMemo,useRef,useState} from 'react';
import {View,ScrollView,Pressable,TextInput} from 'react-native';
import {C,T,Icon,Button,Sheet,s} from '../ui';
import {Popover} from '../layout';
import {HeaderChip,RadioMenu,webOnly} from '../discover/parts';
import {BUILDUP_VALUES,DTE_VALUES,FILTER_COLUMNS,MARKET_VALUES,MONEYNESS_VALUES,OI_CHANGE_VALUES,OPERATOR_LABELS,
 PREMIUM_VALUES,RUPEE,TIMES,VOLUME_RATIO_VALUES,VOLUME_TO_OI_VALUES,buildupLabel,dteText,expiriesFor,freeColumns,
 newRule,ruleText,sanitizeRules,stamp,withColumn,type FilterColumn,type FilterRule} from './logic';
import type {FilterData} from './types';
type Choice={value:string;label:string;detail?:string};
/** The values each column offers, built from what the store actually holds. */
export function valueChoices(column:FilterColumn,data:FilterData|null,rules:FilterRule[]):Choice[]{
 if(column==='underlying')return (data?.underlyings||[]).map(u=>({value:u.underlying,label:u.underlying,
  detail:`${u.options} option contracts${u.futures?` · ${u.futures} futures`:''}${u.lot_size?` · lot ${u.lot_size}`:''}`}));
 if(column==='expiry'){
  const chosen=rules.find(r=>r.column==='underlying')?.value||'';
  return expiriesFor(data?.expiries||[],chosen).map(e=>({value:e.expiry,
   label:stamp(e.expiry)?.date||e.expiry,detail:dteText(e.days_to_expiry)}));
 }
 if(column==='optionType')return [{value:'CE',label:'CE (call)'},{value:'PE',label:'PE (put)'}];
 if(column==='dte')return DTE_VALUES.map(v=>({value:String(v),label:`${v} day${v===1?'':'s'}`}));
 if(column==='premium')return PREMIUM_VALUES.map(v=>({value:String(v),label:`${RUPEE}${v} cr`}));
 if(column==='watchlist')return (data?.watchlists||[]).map(w=>({value:w.key,label:w.label}));
 // the screener's seven. Each one is a §3 signal, so each value carries what the signal is measured against.
 if(column==='volumeRatio')return VOLUME_RATIO_VALUES.map(v=>({value:String(v),
  label:`${v}${TIMES} its own median`,detail:'Cumulative volume by this time of day, against the median of the last 10 sessions'}));
 if(column==='volumeToOi')return VOLUME_TO_OI_VALUES.map(v=>({value:String(v),label:`${v}${TIMES}`,
  detail:v===1?'Day volume equal to the whole standing position':'Day volume against yesterday\'s closing open interest'}));
 if(column==='oiChange15m'||column==='oiChangeDay')return OI_CHANGE_VALUES.map(v=>({value:String(v),label:`${v}%`,
  detail:column==='oiChange15m'?'Open-interest change over the last 15-min reading':'Open-interest change since the previous close'}));
 if(column==='buildup')return BUILDUP_VALUES.map(v=>({value:v,label:buildupLabel(v),detail:BUILDUP_DETAIL[v]}));
 if(column==='moneyness')return MONEYNESS_VALUES.map(m=>({value:m.value,label:m.label}));
 if(column==='market')return MARKET_VALUES.map(m=>({value:m.value,label:m.label,
  detail:m.value==='index'?'NIFTY, BANKNIFTY and the other index chains':'Single-stock chains'}));
 return [];
}
/** §3.1 in one line each, so a reader picking a build-up knows exactly which pair of moves it is. */
const BUILDUP_DETAIL:Record<string,string>={long_buildup:'Price up, open interest up',
 short_buildup:'Price down, open interest up',short_covering:'Price up, open interest down',
 long_unwinding:'Price down, open interest down'};
/** One rule's value in the words the popup used, so the header line and the screen reader agree with the menus. */
export function ruleValueLabel(rule:FilterRule,data:FilterData|null,rules:FilterRule[]){
 const found=valueChoices(rule.column,data,rules).find(c=>c.value===rule.value);
 return found?found.label:rule.value;
}
export const ruleLabeller=(data:FilterData|null,rules:FilterRule[])=>(rule:FilterRule)=>ruleValueLabel(rule,data,rules);
/** A long list (216 underlyings) needs a search box; the short ones stay a plain radio menu. */
function PickList({label,value,options,onPick,onClose,note}:{label:string;value:string;options:Choice[];
 onPick:(v:string)=>void;onClose:()=>void;note?:string}){
 const [term,setTerm]=useState('');
 const shown=useMemo(()=>{const q=term.trim().toUpperCase();
  return q?options.filter(o=>o.value.toUpperCase().includes(q)||o.label.toUpperCase().includes(q)):options},[term,options]);
 return <View style={{maxHeight:360}}>
  <View style={{paddingHorizontal:10,paddingTop:8,paddingBottom:4}}>
   <TextInput value={term} onChangeText={setTerm} placeholder={`Search ${label.toLowerCase()}`} placeholderTextColor={C.muted}
    accessibilityLabel={`Search ${label}`} style={[s.input,{minHeight:36,fontSize:13,paddingHorizontal:10}]}/>
  </View>
  <ScrollView style={{maxHeight:280}} keyboardShouldPersistTaps="handled">
   <View role="menu" aria-label={label}>
    {shown.map(option=>{const on=option.value===value;
     return <Pressable key={option.value} role="menuitemradio" aria-checked={on} accessibilityState={{checked:on}}
      accessibilityLabel={option.label} {...webOnly({tabIndex:0})} onPress={()=>{onClose();if(!on)onPick(option.value)}}
      style={(st:any)=>[s.row,{minHeight:38,paddingHorizontal:12,gap:8,backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}>
      <View style={{width:14}}>{on&&<Icon name="check" size={14} color={C.green}/>}</View>
      <View style={{flex:1,minWidth:0}}><T numberOfLines={1} style={{fontSize:13,color:on?C.green:C.ink}}>{option.label}</T>
       {!!option.detail&&<T numberOfLines={1} style={{fontSize:11,color:C.muted}}>{option.detail}</T>}</View>
     </Pressable>})}
    {!shown.length&&<T style={{fontSize:12,color:C.muted,padding:12}}>Nothing matches &quot;{term}&quot;.</T>}
   </View>
  </ScrollView>
  {!!note&&<T style={{fontSize:11,color:C.muted,paddingHorizontal:12,paddingVertical:8}}>{note}</T>}
 </View>;
}
type Field='column'|'operator'|'value';
export type FilterDialogProps={visible:boolean;onClose:()=>void;name:string;data:FilterData|null;
 rules:FilterRule[];onApply:(rules:FilterRule[])=>void};
export function FilterDialog({visible,onClose,name,data,rules,onApply}:FilterDialogProps){
 // The dialog edits a DRAFT: closing it without APPLY leaves the table exactly as it was.
 const [draft,setDraft]=useState<FilterRule[]>(rules);
 const [menu,setMenu]=useState<{row:number;field:Field}|null>(null);
 const anchors=useRef<Record<string,{current:any}>>({});
 const anchor=(key:string)=>{const map=anchors.current;if(!map[key])map[key]={current:null};return map[key]};
 React.useEffect(()=>{if(visible){setDraft(rules);setMenu(null)}},[visible,rules]);
 const set=(i:number,next:FilterRule)=>setDraft(list=>list.map((r,j)=>j===i?next:r));
 const remove=(i:number)=>setDraft(list=>list.filter((_,j)=>j!==i));
 const add=()=>setDraft(list=>{const rule=newRule(list);return rule?[...list,rule]:list});
 const clean=sanitizeRules(draft);
 const canAdd=freeColumns(draft).length>0;
 const close=()=>{setMenu(null);onClose()};
 const apply=()=>{setMenu(null);onApply(clean);onClose()};
 const floor=data?.floors?.premium_cr??2;
 return <Sheet visible={visible} onClose={close} title={`Filter ${name} data`}
  subtitle="Every filter below is a query the pilot validates before it answers, so the rows on screen are exactly the rows it returned."
  footer={<View style={[s.row,{flexWrap:'wrap',gap:10}]}>
   <Button label="ADD FILTER" icon="plus" kind="outline" disabled={!canAdd} onPress={add}
    accessibilityLabel="Add a filter row"/>
   <Button label="CLEAR ALL" icon="x-circle" kind="outline" disabled={!draft.length} onPress={()=>setDraft([])}
    accessibilityLabel="Clear every filter row"/>
   <View style={{flex:1}}/>
   <Button label="APPLY" icon="check" onPress={apply} accessibilityLabel={`Apply ${clean.length} filters`}/>
  </View>}>
  <View style={{gap:10}}>
   {!draft.length&&<T style={{fontSize:13,lineHeight:19,color:C.muted}}>
    No filter yet. ADD FILTER starts a row: a column, an operator, and a value.
   </T>}
   {draft.map((rule,i)=>{
    const column=FILTER_COLUMNS.find(c=>c.key===rule.column)!;
    const columns=freeColumns(draft,rule.column);
    const choices=valueChoices(rule.column,data,draft);
    const chosen=choices.find(c=>c.value===rule.value);
    const long=rule.column==='underlying'||rule.column==='expiry';
    const chip=(field:Field,label:string,a11y:string,tone?:string)=>
     <HeaderChip ref={anchor(`${i}:${field}`) as any} label={label} a11y={a11y} expanded={menu?.row===i&&menu?.field===field}
      tone={tone} onPress={()=>setMenu(m=>m?.row===i&&m?.field===field?null:{row:i,field})} style={{flexShrink:1,minWidth:0}}/>;
    return <View key={`${rule.column}-${i}`} role="group" aria-label={`Filter ${i+1}: ${ruleText(rule,chosen?.label)}`}
     style={[s.row,{flexWrap:'wrap',gap:8,padding:10,borderRadius:10,borderWidth:1,borderColor:C.line,
      backgroundColor:C.paper}]}>
     {chip('column',column.label,`Column: ${column.label}. Change the column`)}
     {chip('operator',OPERATOR_LABELS[rule.operator],`Operator: ${OPERATOR_LABELS[rule.operator]}. Change the operator`)}
     {chip('value',chosen?.label||rule.value||'Choose a value',
      `Value: ${chosen?.label||rule.value||'not chosen'}. Change the value`,rule.value?undefined:C.amber)}
     {/* A filter this pilot has no parameter for is still offered and still sent — what it must never do is
         read as though it worked. The screener's own `applied` list is the only thing that says it did, and
         this row says so here rather than letting a reader assume it. */}
     {!!column.pending&&<T style={{width:'100%',fontSize:11,lineHeight:16,color:C.muted}}>
      Answered by the screener only. It is shown as active there when — and only when — the server says it
      applied it.
     </T>}
     <View style={{flex:1,minWidth:8}}/>
     <Pressable accessibilityRole="button" accessibilityLabel={`Remove filter ${i+1}`} onPress={()=>remove(i)}
      style={(st:any)=>[{width:32,height:32,alignItems:'center',justifyContent:'center',borderRadius:8,
       backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}><Icon name="x" size={15} color={C.muted}/></Pressable>
     <Popover open={menu?.row===i&&menu?.field==='column'} onClose={()=>setMenu(null)} anchor={anchor(`${i}:column`) as any}
      label="Column" role="none" width={230}>
      <RadioMenu label="Column" value={rule.column} onClose={()=>setMenu(null)}
       onPick={v=>set(i,withColumn(rule,v as FilterColumn))}
       options={columns.map(c=>({value:c.key,label:c.label}))}
       note="One rule per column: the pilot answers each parameter once."/>
     </Popover>
     <Popover open={menu?.row===i&&menu?.field==='operator'} onClose={()=>setMenu(null)} anchor={anchor(`${i}:operator`) as any}
      label="Operator" role="none" width={230}>
      <RadioMenu label="Operator" value={rule.operator} onClose={()=>setMenu(null)}
       onPick={v=>set(i,{...rule,operator:v as any})}
       options={column.operators.map(op=>({value:op,label:OPERATOR_LABELS[op]}))}
       note={column.operators.length>1?undefined:'This column has one operator the pilot can answer.'}/>
     </Popover>
     <Popover open={menu?.row===i&&menu?.field==='value'} onClose={()=>setMenu(null)} anchor={anchor(`${i}:value`) as any}
      label="Value" role="none" width={long?300:240}>
      {long?<PickList label={column.label} value={rule.value} onClose={()=>setMenu(null)}
        onPick={v=>set(i,{...rule,value:v})} options={choices}
        note={choices.length?undefined:'Nothing of this kind has been captured yet.'}/>
       :<RadioMenu label={column.label} value={rule.value} onClose={()=>setMenu(null)}
        onPick={v=>set(i,{...rule,value:v})} options={choices.map(c=>({value:c.value,label:c.label,detail:c.detail}))}
        note={column.key==='premium'?`The ${RUPEE}${floor} cr floor always applies; this can only raise it.`:
         column.key==='dte'?'Expiry-day volume is not unusual activity; this keeps the two apart (§3).':undefined}/>}
     </Popover>
    </View>;
   })}
   {!!draft.length&&draft.length!==clean.length&&<T style={{fontSize:12,lineHeight:18,color:C.amber}}>
    A row without a value is not a filter and is left out when you apply.
   </T>}
   {!canAdd&&<T style={{fontSize:12,lineHeight:18,color:C.muted}}>
    Every column the pilot can filter on is already in use.
   </T>}
   <T style={{fontSize:12,lineHeight:18,color:C.muted}}>
    Applied: {clean.length?clean.map(r=>ruleText(r,ruleValueLabel(r,data,draft))).join(' · '):'nothing — every contract over the liquidity floors'}.
   </T>
   <T style={{fontSize:11,lineHeight:17,color:C.muted}}>
    Columns without a funnel are not filterable: the pilot has no parameter for them, and this popup does not pretend
    otherwise by hiding rows in the browser.
   </T>
  </View>
 </Sheet>;
}
