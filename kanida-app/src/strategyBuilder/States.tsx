// Shared resource states (GTM audit P08): loading, error with Retry, and never an endless spinner under an error.
import React from 'react';
import {ScrollView,View} from 'react-native';
import {Button,C,Loading,T,s} from '../ui';

export function LoadState({loading,error,onRetry,children,what='This'}:{loading:boolean;error?:string;onRetry?:()=>void;children?:React.ReactNode;what?:string}){
 if(error)return <ErrorRetry error={error} onRetry={onRetry} what={what}/>;
 if(loading)return <Loading/>;
 return <>{children}</>;
}

export function ErrorRetry({error,onRetry,what='This'}:{error:string;onRetry?:()=>void;what?:string}){
 return <View accessibilityRole="alert" style={[s.row,{flexWrap:'wrap',gap:10,backgroundColor:'#2A1519',borderRadius:10,padding:12}]}>
  <T style={{color:C.red,fontSize:12,flex:1,minWidth:200}}>{`${what} could not be loaded: ${error}`}</T>
  {onRetry&&<Button label="Retry" icon="refresh-cw" kind="outline" onPress={onRetry}/>}
 </View>;
}

/** Wide tables on a phone: a bounded horizontal scroll instead of squeezed, overlapping columns (GTM audit P12). */
export function Scrollable({narrow,min=640,children}:{narrow:boolean;min?:number;children:React.ReactNode}){
 if(!narrow)return <>{children}</>;
 return <ScrollView horizontal showsHorizontalScrollIndicator contentContainerStyle={{minWidth:min}} accessibilityLabel="Table - scroll sideways for more columns">
  <View style={{width:min}}>{children}</View></ScrollView>;
}
