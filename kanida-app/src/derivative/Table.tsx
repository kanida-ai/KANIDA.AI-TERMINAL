// The dense data table every Derivative widget is built from — the benchmark's wide grid: a compact header bar of
// sortable column labels (an arrow on the sorted one, a funnel on the ones the Customize popup can filter), then
// tight scrollable rows that re-target the chart tile when clicked.
//
// Two rules this file must not break:
//  1. Sorting re-orders the rows the SERVER returned; it never re-queries and never hides a row. The as-of line and
//     the floors printed under the table therefore still describe exactly the rows on screen (§4, §5).
//  2. A value that was not captured sorts to the bottom in both directions (logic.compareValues) — a dash is not a
//     small number.
import React from 'react';
import {View,ScrollView,Pressable} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {web} from '../layout/shared';
import {head,webOnly} from './frame';
import type {SortState} from './logic';
export type Align='left'|'right'|'center';
export type Column<R>={
 key:string;
 label:string;
 /** Fixed width in pixels; with `grow` it becomes the minimum width and the column takes the slack. */
 width:number;
 grow?:boolean;
 align?:Align;
 /** The captured value this column sorts on. A column without one is not sortable. */
 value?:(row:R)=>unknown;
 /** Opens the Customize popup on this column; shows the funnel in the header. */
 filter?:()=>void;
 render:(row:R)=>React.ReactNode;
};
export type TableItem<R>={kind:'group';key:string;label:string;detail?:string}|{kind:'row';key:string;row:R};
export type TableProps<R>={
 label:string;
 columns:Column<R>[];
 items:TableItem<R>[];
 sort:SortState;
 onSort?:(key:string)=>void;
 onRowPress?:(row:R)=>void;
 selected?:(row:R)=>boolean;
 /** The row is ABOUT the thing the pointer is on somewhere else on the tab - the same strike, lit here too.
  *  It is a highlight and nothing more: it selects nothing and it changes no query. */
 lit?:(row:R)=>boolean;
 /** Pointing at a row tells the tab which row that is, so the same subject can be lit wherever else it is. */
 onRowHover?:(row:R|null)=>void;
 rowLabel:(row:R)=>string;
 /** Narrow screens: the first column stays put while the rest scroll sideways (web; a plain column elsewhere). */
 pinFirst?:boolean;
 rowHeight?:number;
};
const justify=(a:Align|undefined)=>a==='right'?'flex-end':a==='center'?'center':'flex-start';
/** The sticky first column is a web affordance; RN native keeps a normal column and the table simply scrolls. */
const stick=(on:boolean,background:string)=>on&&web?{position:'sticky',left:0,zIndex:2,backgroundColor:background} as any:null;
export function Table<R>({label,columns,items,sort,onSort,onRowPress,selected,lit,onRowHover,rowLabel,pinFirst,rowHeight=34}:TableProps<R>){
 const total=columns.reduce((sum,c)=>sum+c.width,0);
 const cell=(column:Column<R>,i:number,background:string)=>[{width:column.width,paddingHorizontal:6,
  justifyContent:'center',alignItems:justify(column.align)} as any,
  column.grow?{flex:1,minWidth:column.width,width:undefined}:null,stick(!!pinFirst&&i===0,background)];
 // flexGrow makes the content container as tall as the widget body, so the inner vertical list is BOUNDED and
 // scrolls; without it a long table would simply overflow and the rows past the fold would be unreachable.
 return <ScrollView horizontal showsHorizontalScrollIndicator style={{flex:1}}
  contentContainerStyle={{minWidth:'100%',flexGrow:1}}>
  <View style={{flex:1,minWidth:total}}>
   <View role="row" style={[s.row,{gap:0,minHeight:26,borderBottomWidth:1,borderColor:C.line,
    backgroundColor:C.paper,paddingHorizontal:6}]}>
    {columns.map((column,i)=>{
     const on=sort?.key===column.key;
     const sortable=!!column.value&&!!onSort;
     // A header label that outgrows its column must CLIP inside it, never run over its neighbour: two labels
     // printed on the same pixels is a table that cannot be read at all. `minWidth:0` is what lets the text
     // shrink inside the row on web, and `overflow:hidden` is what makes numberOfLines actually bite there.
     // The widths are chosen to fit the labels (check-derivative.cjs holds them to it); this is the floor
     // under that, so a longer label added later degrades to a clipped word instead of a collision.
     const inner=<View style={[s.row,{gap:3,minWidth:0,overflow:'hidden',justifyContent:justify(column.align)}]}>
      <T numberOfLines={1} style={[head,{flexShrink:1,minWidth:0,color:on?C.ink:C.muted}]}>{column.label}</T>
      {on&&<Icon name={sort?.dir==='asc'?'arrow-up':'arrow-down'} size={11} color={C.green}/>}
      {!!column.filter&&<Icon name="filter" size={10} color={C.muted}/>}
     </View>;
     const a11ySort=on?(sort?.dir==='asc'?'ascending':'descending'):sortable?'none':undefined;
     if(!sortable)return <View key={column.key} role="columnheader" {...webOnly({'aria-sort':a11ySort})}
      style={cell(column,i,C.paper)}>{inner}</View>;
     return <Pressable key={column.key} role="columnheader" accessibilityRole="button"
      accessibilityLabel={`Sort by ${column.label}`} {...webOnly({'aria-sort':a11ySort,tabIndex:0})}
      onPress={()=>onSort?.(column.key)}
      style={(st:any)=>[cell(column,i,C.paper),{backgroundColor:st.hovered||st.focused?C.soft:C.paper}]}>{inner}</Pressable>;
    })}
   </View>
   <ScrollView style={{flex:1}} contentContainerStyle={{paddingBottom:6}}>
    <View {...webOnly({role:'rowgroup'})}>
     {items.map(item=>{
      if(item.kind==='group')return <View key={item.key} role="row" style={[s.row,{minHeight:26,paddingHorizontal:12,
       gap:8,backgroundColor:C.bg,borderTopWidth:1,borderBottomWidth:1,borderColor:C.line}]}>
       <T numberOfLines={1} style={{fontSize:11,lineHeight:15,fontFamily:'InterSemi',color:C.mint}}>{item.label}</T>
       {!!item.detail&&<T numberOfLines={1} style={{flex:1,fontSize:10,lineHeight:14,color:C.muted}}>{item.detail}</T>}
      </View>;
      const row=item.row,on=!!selected?.(row),glow=!on&&!!lit?.(row);
      const background=on?C.soft:glow?C.dark:C.paper;
      const body=columns.map((column,i)=><View key={column.key} style={cell(column,i,background)}>{column.render(row)}</View>);
      if(!onRowPress)return <View key={item.key} role="row" style={[s.row,{gap:0,minHeight:rowHeight,
       paddingHorizontal:6,borderBottomWidth:1,borderColor:'#0F1B22'}]}>{body}</View>;
      // The left rail says which of three states this row is in, and a keyboard lands on exactly the same one
      // the mouse does: chosen (green), lit because the same subject is under the pointer elsewhere (mint),
      // or neither. Focus is as loud as hover - a reader on the keyboard must never have to guess where they are.
      return <Pressable key={item.key} role="row" aria-selected={on} accessibilityState={{selected:on}}
       accessibilityLabel={rowLabel(row)} {...webOnly({tabIndex:0})} onPress={()=>onRowPress(row)}
       onHoverIn={()=>onRowHover?.(row)} onHoverOut={()=>onRowHover?.(null)}
       onFocus={()=>onRowHover?.(row)} onBlur={()=>onRowHover?.(null)}
       style={(st:any)=>[s.row,{gap:0,minHeight:rowHeight,paddingHorizontal:3,borderBottomWidth:1,
        borderLeftWidth:3,borderLeftColor:on||st.focused?C.green:glow?C.mint:'transparent',
        borderColor:'#0F1B22',backgroundColor:on?C.soft:glow?C.dark:st.hovered||st.focused?'#0F1F28':'transparent'}]}>{body}</Pressable>;
     })}
    </View>
   </ScrollView>
  </View>
 </ScrollView>;
}
/** One cell of plain tabular text, the shape almost every column wants. */
export function Cell({text,color,bold,size=11}:{text:string;color?:string;bold?:boolean;size?:number}){
 return <T numberOfLines={1} style={[{fontVariant:['tabular-nums'] as any},{fontSize:size,lineHeight:size+4,
  color:color||C.ink,fontFamily:bold?'InterSemi':'Inter'}]}>{text}</T>;
}
