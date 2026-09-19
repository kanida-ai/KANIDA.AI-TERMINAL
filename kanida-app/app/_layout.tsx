import React from 'react';
import {View} from 'react-native';
import {SafeAreaProvider} from 'react-native-safe-area-context';
import {useFonts} from 'expo-font';
import {Manrope_700Bold} from '@expo-google-fonts/manrope/700Bold';
import {Inter_400Regular} from '@expo-google-fonts/inter/400Regular';
import {Inter_500Medium} from '@expo-google-fonts/inter/500Medium';
import {Inter_600SemiBold} from '@expo-google-fonts/inter/600SemiBold';
import {ProductProvider} from '../src/context';
import {C,T} from '../src/ui';
import {AuthProvider} from '../src/Auth';
import {PilotShell as Shell} from '../src/PilotShell';
export default function Layout(){const [loaded,error]=useFonts({ManropeBold:Manrope_700Bold,Inter:Inter_400Regular,InterMedium:Inter_500Medium,InterSemi:Inter_600SemiBold});if(!loaded&&!error)return <View style={{flex:1,backgroundColor:C.bg,alignItems:'center',justifyContent:'center'}}><T style={{fontSize:22,letterSpacing:5}}>KANIDA</T></View>;return <SafeAreaProvider><AuthProvider><ProductProvider><Shell/></ProductProvider></AuthProvider></SafeAreaProvider>;}
