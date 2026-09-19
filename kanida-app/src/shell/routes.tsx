// Workspace routes (Phase 0 wave 0B; moved to /chart by FALCON_DISCOVER_SPEC §1). "/chart", "/simulate", "/watch", "/autotrade" and "/activity" are all served by ONE persistent MainWorkspace that PilotShell mounts,
// so a dock tab switch (which rewrites the URL with router.replace) never remounts the chart, the sidebar or a dock tab's draft. These route files exist only so expo-router keeps the URLs; they render nothing.
// "/" is Falcon (app/index.tsx) and "/discover" is Discover Strategies: both are full-height pages under the shell header, not workspace routes.
export type DockTabKey='discover'|'evidence'|'simulate'|'autotrade'|'activity'|'watch';
export const CHART_PATH='/chart';
export const WORKSPACE_PATHS=[CHART_PATH,'/simulate','/watch','/autotrade','/activity'];
/** Deep link → dock tab (opened full screen). "/chart" is not listed: it opens Discover or Evidence/Replay (?tab=evidence). */
export const ROUTE_TAB:Record<string,DockTabKey>={'/simulate':'simulate','/watch':'watch','/autotrade':'autotrade','/activity':'activity'};
/** Dock tab → the existing route it maps to. Discover and Evidence/Replay have no route of their own and live on "/chart" (with the store's ?s=&tf=&m=). */
export const TAB_ROUTE:Record<DockTabKey,string>={discover:CHART_PATH,evidence:CHART_PATH,simulate:'/simulate',autotrade:'/autotrade',activity:'/activity',watch:'/watch'};
export const isWorkspacePath=(path:string)=>WORKSPACE_PATHS.includes(path);
/** Pages that manage their own scrolling: the shell gives them a flex:1 container instead of its ScrollView. */
export const FULL_HEIGHT_PATHS=['/','/discover','/derivative'];
export function WorkspaceRoute(){return null;}
// Main navigation (§1). [route,label,short label (tablet),icon]. Simulate and Activity live inside AutoTrade's workspace dock, so they highlight AutoTrade; /chart highlights nothing.
export type NavItem={route:string;label:string;short:string;icon:string};
// "/derivative" is the F&O tab (docs/DERIVATIVES_SPEC.md §4), next to Discover Strategies: a full-height member
// page like "/discover", not a workspace route.
export const NAV_ITEMS:NavItem[]=[{route:'/',label:'Falcon',short:'Falcon',icon:'cpu'},{route:'/discover',label:'Discover Strategies',short:'Discover',icon:'grid'},{route:'/derivative',label:'Derivative',short:'Derivative',icon:'layers'},{route:'/watch',label:'Watchlist',short:'Watchlist',icon:'eye'},{route:'/autotrade',label:'AutoTrade',short:'AutoTrade',icon:'zap'}];
export function navActive(path:string):string|null{if(path==='/'||path==='/discover'||path==='/derivative'||path==='/watch'||path==='/autotrade')return path;if(path==='/simulate'||path==='/activity')return '/autotrade';return null;}
