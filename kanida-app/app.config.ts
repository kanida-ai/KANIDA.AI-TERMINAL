import base from './app.json';
export default () => {
 const api = process.env.EXPO_PUBLIC_API_URL || '';
 if (process.env.EAS_BUILD && !api.startsWith('https://')) {
  throw new Error('Cloud mobile builds require EXPO_PUBLIC_API_URL to point to the deployed HTTPS pilot. A local address must not ship.');
 }
 return {...base.expo,ios:{...base.expo.ios,infoPlist:{ITSAppUsesNonExemptEncryption:false}},extra:{...((base.expo as any).extra||{})}};
};
