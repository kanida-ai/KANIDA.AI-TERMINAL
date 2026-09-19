import React from 'react';
import {ScrollView} from 'react-native';
import {C,T} from '../ui';
import {CardShell} from '../discover/parts';
import {LinkedHeader} from '../discover/ChartCard';
import type {BlockLink} from '../discover/link';
import type {Selection} from './types';
import {HistoryPanel} from './HistoryPanel';
import {query,stateLabel} from './logic';
export function PatternHistoryCard({link,selection,style,color}:{link:BlockLink;selection:Selection;style?:any;color?:string}){
 return <CardShell label={`Pattern history · ${link.symbol}`} style={style} header={<LinkedHeader title={`History · ${link.symbol} · Daily`} slot={link.slot} color={color} detail={`${link.patternName} · ${stateLabel(selection.state)}`} sourceA11y={`${link.patternName} daily history from scanner ${link.slot}`}/>} footer={<T style={{color:C.muted,fontSize:11}}>Two-pattern pilot · historical observations</T>}><ScrollView contentContainerStyle={{padding:12}}><HistoryPanel key={query(selection)} selection={selection} compact/></ScrollView></CardShell>;
}
