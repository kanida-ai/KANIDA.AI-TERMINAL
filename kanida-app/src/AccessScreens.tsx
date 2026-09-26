import React,{useState} from 'react';
import {View,TextInput,Platform} from 'react-native';
import {router,useLocalSearchParams} from 'expo-router';
import {useAuth,openExternal} from './Auth';
import {C,T,s,Button,Badge,Checkbox} from './ui';
import {api} from './model';
import {TIMEFRAMES} from './constants';
export function FormPage({title,detail,children}:any){const auth=useAuth();return <View style={{width:'100%',maxWidth:560,alignSelf:'center',padding:24,paddingTop:40,paddingBottom:50,gap:22}}><View style={{gap:10}}><Badge label={auth.config?.invitation_required===false?"PILOT":"PRIVATE PILOT"}/><T accessibilityRole="header" style={{fontFamily:'ManropeBold',fontSize:32,lineHeight:40,letterSpacing:-.8}}>{title}</T>{detail&&<T style={{fontSize:16,lineHeight:26,color:C.muted}}>{detail}</T>}</View>{children}</View>}
export function Field({label,value,onChangeText,secure=false,...props}:any){return <View style={{gap:8}}><T style={{fontSize:14,color:C.muted}}>{label}</T><TextInput accessibilityLabel={label} value={value} onChangeText={onChangeText} secureTextEntry={secure} autoCapitalize="none" autoCorrect={false} style={[s.input,{minHeight:52,fontSize:16}]} {...props}/></View>}
export function Consent({value,onChange,children,label}:any){return <Checkbox checked={!!value} onChange={onChange} label={label||(typeof children==='string'?children:'I agree')} style={{minHeight:48,paddingVertical:10}}><T style={{flex:1,fontSize:14,lineHeight:23}}>{children}</T></Checkbox>}
function safeReturn(value:any,fallback:string){return typeof value==='string'&&value.startsWith('/')&&!value.startsWith('//')&&!value.includes('\\')?value:fallback}
export function SignIn({signup=false}:any){
 const auth=useAuth();const params=useLocalSearchParams();const [email,setEmail]=useState(''),[name,setName]=useState(''),[password,setPassword]=useState(''),[invite,setInvite]=useState(String(params.invite||'')),[consent,setConsent]=useState(false),[busy,setBusy]=useState(false),[error,setError]=useState(params.error?'Sign-in was not completed. Please try again.':'');
 const gated=auth.config?.invitation_required!==false;
 async function submit(){setBusy(true);setError('');try{const data={email,password,name,invite,policy_version:consent?auth.config?.policy_version:undefined};if(signup)await auth.signUp(data);else await auth.signIn(data);router.replace(safeReturn(params.returnTo,signup?'/onboarding':'/') as any)}catch(e:any){setError(e.message)}finally{setBusy(false)}}
 async function device(){setBusy(true);setError('');try{await auth.finishDevice();router.replace('/')}catch(e:any){setError(e.message)}finally{setBusy(false)}}
 return <FormPage title={signup?'Your agent starts here.':'Welcome back.'} detail={signup?`Create your ${gated?'invited ':''}KANIDA account. Your research, watchlist and plans stay together.`:'Sign in to continue your research and plans.'}>
  {!!error&&<T accessibilityRole="alert" style={{color:C.red,fontSize:14}}>{error}</T>}
  {signup&&<Field label="Your name" value={name} onChangeText={setName} autoCapitalize="words" autoComplete="name"/>}
  <Field label="Email" value={email} onChangeText={setEmail} keyboardType="email-address" autoComplete="email" textContentType="emailAddress"/>
  <Field label={signup?'Password · at least 12 characters':'Password'} value={password} onChangeText={setPassword} secure autoComplete={signup?'new-password':'current-password'} textContentType={signup?'newPassword':'password'} onSubmitEditing={submit}/>
  {signup&&<>{(gated||!!invite)&&<Field label="Private invitation code" value={invite} onChangeText={setInvite}/>}<Consent value={consent} onChange={setConsent}>I agree to the private pilot terms and understand that research and synthetic simulations do not promise returns.</Consent><View style={s.row}><Button label="Read pilot terms" kind="ghost" onPress={()=>router.push('/terms')}/><Button label="Privacy" kind="ghost" onPress={()=>router.push('/privacy')}/></View></>}
  <Button label={signup?'Create my account':'Sign in'} loading={busy} disabled={!email||!password||(signup&&(!name||(gated&&!invite)||!consent||!auth.config))} onPress={submit} style={{minHeight:54}}/>
  {Platform.OS==='web'?<Button label="Continue with Google" kind="outline" disabled={!auth.config?.google||busy} onPress={()=>auth.google(invite)}/>:<Button label="Sign in through my browser" icon="external-link" kind="outline" onPress={()=>auth.connectBrowser().catch((e:any)=>setError(e.message))}/>}
  {Platform.OS==='web'&&!auth.config?.google&&<T style={{fontSize:13,color:C.muted}}>Google sign-in will be available after the owner completes its setup.</T>}
  {auth.device&&<Button label="I’ve approved this device" kind="soft" loading={busy} onPress={device}/>}
  <Button label={signup?'Already have an account? Sign in':gated?'Have an invitation? Create an account':'New to KANIDA? Create an account'} kind="ghost" onPress={()=>router.replace({pathname:signup?'/signin':'/signup',params:{...(params.returnTo?{returnTo:String(params.returnTo)}:{})}})}/>
  {!signup&&<T style={{fontSize:13,color:C.muted}}>Forgot your password? Ask the pilot owner for an account recovery link. Broker passwords never belong here.</T>}
  {gated&&<RequestAccess/>}
 </FormPage>
}
/** No invitation yet? Ask for one (the owner sees it in Admin → Access requests). Never reveals whether an account exists. */
function RequestAccess(){
 const [open,setOpen]=useState(false);const [email,setEmail]=useState('');const [name,setName]=useState('');const [note,setNote]=useState('');
 const [busy,setBusy]=useState(false);const [done,setDone]=useState(false);const [error,setError]=useState('');
 if(done)return <T accessibilityRole="alert" style={{fontSize:14,color:C.green}}>Thanks - your request is in. You'll receive an invitation link if it's approved.</T>;
 if(!open)return <Button label="No invitation? Request access" kind="ghost" onPress={()=>setOpen(true)}/>;
 return <View style={{gap:10,borderTopWidth:1,borderColor:C.line,paddingTop:12}}>
  <T style={{fontFamily:'InterSemi',fontSize:15}}>Request access</T>
  <Field label="Your name" value={name} onChangeText={setName} autoCapitalize="words"/>
  <Field label="Email" value={email} onChangeText={setEmail} keyboardType="email-address" autoComplete="email"/>
  <Field label="How do you trade options? (optional)" value={note} onChangeText={setNote}/>
  {!!error&&<T accessibilityRole="alert" style={{color:C.red,fontSize:13}}>{error}</T>}
  <Button label="Send request" loading={busy} disabled={!email} onPress={async()=>{setBusy(true);setError('');try{await api('/api/access/request',{email,name,note});setDone(true);}catch(e:any){setError(e.message);}finally{setBusy(false);}}}/>
 </View>;
}
export function Onboarding(){const auth=useAuth();const [name,setName]=useState(auth.user?.name||''),[frames,setFrames]=useState<string[]>([...TIMEFRAMES]),[consent,setConsent]=useState(false),[busy,setBusy]=useState(false),[error,setError]=useState('');async function save(){setBusy(true);try{await api('/api/account/onboarding',{name,timeframes:frames,acknowledge_pilot:consent,policy_version:auth.config?.policy_version});await auth.refresh();router.replace('/account?welcome=1')}catch(e:any){setError(e.message)}finally{setBusy(false)}}return <FormPage title="Make it your workspace." detail="Choose your chart horizons. Discover starts with these timeframes, and you can explore research before connecting a broker.">
 <Field label="What should we call you?" value={name} onChangeText={setName}/><T style={{fontSize:14,color:C.muted}}>Timeframes to follow</T><View style={[s.row,{flexWrap:'wrap'}]}>{TIMEFRAMES.map(tf=><Button key={tf} label={tf} kind={frames.includes(tf)?'soft':'outline'} onPress={()=>setFrames(frames.includes(tf)?frames.filter(f=>f!==tf):[...frames,tf])}/>)}</View>
 <View style={s.card}><T style={{fontFamily:'InterSemi',fontSize:16}}>You stay in control.</T><T style={{fontSize:14,lineHeight:24,color:C.muted}}>Your account begins with research and a ₹1,00,000 virtual account. Simulations use artificial price scenarios. Connecting Kite does not enable live orders.</T></View><Consent value={consent} onChange={setConsent}>I understand the private pilot limits and accept the current pilot terms.</Consent><Button label="Read pilot terms" kind="ghost" onPress={()=>router.push('/terms')}/>{!!error&&<T style={{color:C.red}}>{error}</T>}<Button label="Open my workspace" loading={busy} disabled={!consent||!name||!frames.length} onPress={save}/></FormPage>}
export function DeviceConnect(){const auth=useAuth(),params=useLocalSearchParams();const [done,setDone]=useState(false),[error,setError]=useState(''),[busy,setBusy]=useState(false);const code=String(params.code||'');async function approve(){setBusy(true);try{await api('/api/auth/device/approve',{code,confirm_device:true});setDone(true)}catch(e:any){setError(e.message)}finally{setBusy(false)}}return <FormPage title={done?'Your app is ready to connect.':'Connect your KANIDA app.'} detail={done?'Return to the app and tap “I’ve approved this device”.':'Only approve this if you just started sign-in from your own KANIDA mobile app.'}>{!!error&&<T style={{color:C.red}}>{error}</T>}{!done&&(auth.user?<><T>Connect as {auth.user.email}</T><Button label="Connect my mobile app" loading={busy} disabled={!code} onPress={approve}/></>:<Button label="Sign in to connect" onPress={()=>router.push({pathname:'/signin',params:{returnTo:'/device?code='+code}})}/>)}<T style={{fontSize:13,color:C.muted}}>The request expires after 10 minutes. Your app proves it initiated the request before receiving a session.</T></FormPage>}
export function PilotLegal({privacy=false}:any){return <FormPage title={privacy?'Your privacy in the pilot.':'Private pilot terms.'} detail="Version private-pilot-v1 · pilot testing">
 {(privacy?[
 ['What is stored','Your account identity, encrypted broker authorization, subscription references, saved setups, plans and activity records. Passwords are stored as one-way hashes.'],
 ['Who processes information','Google verifies your identity if you choose Google sign-in. Razorpay handles test checkout. Kite handles broker authorization and any separately activated brokerage functions. KANIDA never asks for your broker password or TOTP seed.'],
 ['Your controls','Disconnect Kite, sign out of all devices or request account closure through the pilot owner. Financial and audit records may need retention before deletion. This local pilot has no advertising trackers.'],
 ]:[
 ['Research is evidence, not a promise','Historical averages describe the supplied research dataset. Small samples, assumed costs, unadjusted prices and instrument-policy limitations can affect results. A pattern score is not a win probability.'],
 ['Simulation is artificial','The virtual account uses named synthetic scenarios to test order handling. Its profit, loss and success rates are not investment results or forecasts. No broker prices feed the simulator.'],
 ['Broker authorization is separate','Connecting Kite permits the configured account connection. Live order submission is disabled in this pilot release and requires a separately reviewed activation. Do not rely on this pilot to manage existing broker positions.'],
 ['Payments and access','Razorpay is restricted to test mode. Registration follows the current access setting; the owner has included pilot access. The test checkout does not purchase a public production subscription.'],
 ['Pilot use','Use your own account. Keep invitations private, review each action, and report unexpected behavior to the owner. Public availability and store release require further review.'],
 ]).map(([title,text])=><View key={title} style={{gap:8}}><T style={{fontFamily:'InterSemi',fontSize:17}}>{title}</T><T style={{fontSize:15,lineHeight:25,color:C.muted}}>{text}</T></View>)}<Button label="Go back" kind="outline" onPress={()=>router.canGoBack()?router.back():router.replace('/welcome')}/></FormPage>}
