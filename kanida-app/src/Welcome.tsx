import React from 'react';
import {View,Pressable} from 'react-native';
import {router} from 'expo-router';
import {LinearGradient} from 'expo-linear-gradient';
import {useProduct} from './context';
import {C,T,s,Button,Icon,Badge,Loading,useLayoutMode} from './ui';
import {Match,dateText} from './model';
import {PatternCanvas} from './PatternCanvas';
import {AgentMark} from './TraderShell';
import {useAuth} from './Auth';
import Svg,{Path,Line,Circle,Text as SvgText} from 'react-native-svg';

function PreviewChart(){return <View style={{gap:16}}><View style={s.between}><T style={{fontFamily:'ManropeBold',fontSize:23}}>The structure becomes clear.</T></View><T style={{fontSize:14,color:C.muted}}>An illustrative triangle · not a stock or forecast</T><Svg width="100%" height={230} viewBox="0 0 500 230" accessibilityLabel="Illustrative chart showing narrowing swings inside a triangle"><Line x1={20} y1={190} x2={480} y2={190} stroke="#28403C"/>{[60,105,150].map(y=><Line key={y} x1={20} y1={y} x2={480} y2={y} stroke="#172F2C"/>)}<Path d="M 24 178 L 58 139 L 78 150 L 112 51 L 174 171 L 230 76 L 290 146 L 340 101 L 382 128 L 410 119" stroke="#C0E9DC" fill="none" strokeWidth={2}/><Path d="M 112 51 L 435 123 M 174 171 L 435 123" stroke="#39E5A3" fill="none" strokeWidth={2.5} strokeDasharray="6 4"/>{[[112,51],[174,171],[230,76],[290,146],[340,101],[382,128]].map(([x,y],i)=><Circle key={i} cx={x} cy={y} r={4} fill="#39E5A3"/>)}<SvgText x={22} y={219} fill="#90A2AD" fontSize={13}>Earlier swings</SvgText><SvgText x={350} y={219} fill="#90A2AD" fontSize={13}>Tighter structure</SvgText></Svg><T style={{fontSize:14,lineHeight:24,color:C.muted}}>Open a real setup to watch your agent draw its boundaries, connect the internal swings and explain the risk zones.</T></View>}

export function Welcome(){
 const auth=useAuth();
 // Shared layout modes (3.3): wide = desktop (>=1050, unchanged); phone = <760 (was <700); the feature/status rows go horizontal from tablet (>=760, was >=800).
 const mode=useLayoutMode(),wide=mode==='desktop',phone=mode==='phone';
 const {matches,state,loading,error,refresh,setDetail}=useProduct();
 const example:Match|undefined=matches.find((m:Match)=>m.symbol==='MUTHOOTFIN'&&m.pattern==='symmetrical_triangle'&&m.timeframe==='1D')||matches.find((m:Match)=>m.pattern==='symmetrical_triangle')||matches[0];
 function openExample(){if(example){router.push('/chart');setDetail(example)}}
 return <View style={{width:'100%',maxWidth:1440,alignSelf:'center',paddingHorizontal:phone?20:42,paddingTop:phone?28:54,paddingBottom:48,gap:wide?42:30}}>
  <View style={{flexDirection:wide?'row':'column',gap:wide?56:28,alignItems:wide?'center':'stretch'}}>
   <View style={{flex:wide?.85:undefined,gap:22}}>
    <View style={s.row}><View style={{width:22,height:1,backgroundColor:C.green}}/><T style={{fontSize:11,letterSpacing:2,color:C.green,fontFamily:'InterSemi'}}>MEET YOUR CHART AGENT</T></View>
    <T accessibilityRole="header" style={{fontFamily:'ManropeBold',fontSize:phone?40:wide?53:48,lineHeight:phone?48:62,letterSpacing:-1.8}}>See the setup.{ '\n'}Understand the move.</T>
    <T style={{fontSize:16,lineHeight:27,color:C.muted,maxWidth:510}}>A chart that shows its thinking. KANIDA finds the structure, draws the pattern, and brings that stock’s own history into the decision.</T>
    <View style={{gap:10,alignItems:wide?'flex-start':'stretch'}}><Button label={auth.user?'Open my workspace':'Enter the private pilot'} icon="arrow-right" onPress={()=>router.push(auth.user?'/':'/signin')} style={{minHeight:54,paddingHorizontal:24}}/><T style={{fontSize:14,color:C.muted}}>Invitation only · your research, plans and broker connection</T></View>
    <View style={{borderTopWidth:1,borderColor:C.line,paddingTop:18,flexDirection:'row',gap:22}}>{[['01','Discover'],['02','See the evidence'],['03','Make a plan']].map(([n,label])=><View key={n} style={{flex:1,gap:7}}><T style={{fontSize:11,color:C.green,fontFamily:'InterSemi'}}>{n}</T><T style={{fontSize:12,lineHeight:18,color:C.muted}}>{label}</T></View>)}</View>
   </View>
   <LinearGradient colors={['#10332F','#0A171F','#0B151C']} start={{x:0,y:0}} end={{x:1,y:1}} style={{flex:wide?1.15:undefined,borderRadius:24,borderWidth:1,borderColor:'#244740',padding:phone?14:24,gap:18}}>
    <View style={[s.between,{alignItems:'flex-start'}]}><View style={[s.row,{flex:1}]}><AgentMark size={34}/><View style={{flex:1,gap:3}}><T style={{fontSize:15,fontFamily:'InterSemi'}}>Let me show you how.</T><T style={{fontSize:13,color:C.muted}}>{example?'An example from your stored scan':'Your chart agent, in action'}</T></View></View><Badge label={example?'RESEARCH':'ILLUSTRATION'} tone="amber"/></View>
    {loading?<Loading/>:example?<><View style={[s.between,{flexWrap:'wrap'}]}><View style={{gap:4}}><T style={{fontFamily:'ManropeBold',fontSize:23}}>{example.symbol}</T><T style={{fontSize:14,color:C.muted}}>{example.pattern_name} · {example.timeframe}</T></View><T style={{fontSize:13,color:C.muted}}>{dateText(example.candle_end)}</T></View><PatternCanvas key={example.id} match={example} compact/><Button label={`Understand ${example.symbol}`} icon="arrow-up-right" kind="outline" onPress={openExample}/></>:<PreviewChart/>}
   </LinearGradient>
  </View>
  <View style={{flexDirection:phone?'column':'row',gap:14}}>{[
   ['aperture','The pattern, made visible','Watch the agent trace the boundaries and inner swings on actual candles.'],
   ['bar-chart-2','Evidence for this stock','Past return per trade, win rate and holding time stay tied to the exact pattern and timeframe.'],
   ['shield','A plan you can inspect','Review capital, entry, stop and target. Then test the order journey with a virtual account.'],
  ].map(([icon,title,body])=><View key={title} style={{flex:1,padding:22,gap:12,backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:16}}><Icon name={icon} color={C.green} size={22}/><T style={{fontFamily:'InterSemi',fontSize:15}}>{title}</T><T style={{fontSize:13,lineHeight:22,color:C.muted}}>{body}</T></View>)}</View>
  <View style={{borderTopWidth:1,borderColor:C.line,paddingTop:24,gap:17}}><View style={[s.between,{flexWrap:'wrap'}]}><T style={{fontFamily:'InterSemi',fontSize:17}}>Your workspace, at a glance</T><T style={{fontSize:12,color:C.muted}}>{auth.user?'Research through '+dateText(state?.source_latest):'Your private research workspace'}</T></View><View style={{flexDirection:phone?'column':'row',gap:12}}>{[
   ['Research',!auth.user?'Sign in to explore':error?'Connection interrupted':loading?'Loading':state?'Available':'Unavailable','Inspect stored setups and stock-specific backtests.',!!state&&!error],
   ['Kite connection','Secure broker login','Connect your own account inside your private workspace.',false],
   ['AutoTrade','Virtual workflow','Test order handling. Live execution remains disabled.',false],
  ].map(([title,status,body,available])=><View key={String(title)} style={{flex:1,gap:7,borderLeftWidth:2,borderColor:available?C.green:C.line,paddingLeft:14,paddingVertical:6}}><View style={s.between}><T style={{fontSize:13,fontFamily:'InterSemi'}}>{title}</T><T style={{fontSize:11,color:available?C.green:C.amber}}>{status}</T></View><T style={{fontSize:12,color:C.muted,lineHeight:20}}>{body}</T></View>)}</View></View>
  <Pressable accessibilityRole="link" accessibilityLabel="Preview KANIDA on iPhone" onPress={()=>router.push('/connect')} style={{minHeight:48,flexDirection:'row',gap:9,alignItems:'center',justifyContent:'center'}}><Icon name="smartphone" size={16} color={C.green}/><T style={{fontSize:13,color:C.green}}>Take KANIDA to your iPhone</T><Icon name="arrow-right" size={16} color={C.green}/></Pressable>
 </View>;
}
