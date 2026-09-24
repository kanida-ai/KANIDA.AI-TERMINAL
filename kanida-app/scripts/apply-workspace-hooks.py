"""The after-close hooks for the Options Screener and the Workspace — OWNER-RUN, after 15:30 IST, with the owner's OK.

    .pilot-venv\\Scripts\\python.exe scripts\\apply-workspace-hooks.py            # dry run: shows every change, writes nothing
    .pilot-venv\\Scripts\\python.exe scripts\\apply-workspace-hooks.py --apply    # writes them

Three files, four small insertions, each anchored on text that must occur EXACTLY ONCE (the script refuses
otherwise) and each skipped if already applied, so running it twice is harmless:

  1. server/kanida_pilot/app.py     mount the screener and the workspace at the end of create_app()
  2. src/shell/routes.tsx           the Workspace and Options Screener nav entries, and their active state
  3. src/derivative/frame.tsx       BlockBareContext: a block inside a workspace tile drops its own title row
                                    (the tile header already names it). Nothing changes on the Derivative tab,
                                    which never sets the context.

Then rebuild and restart the pilot (scripts/start-pilot.ps1 -Build) and, for alerts, run
scripts/register-screener-task.ps1.
"""
from __future__ import annotations
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
EDITS=[
 ('server/kanida_pilot/app.py','mount_workspace(app,settings)',
  "  return FileResponse(file)\n return app\n",
  "  return FileResponse(file)\n"
  " # Options Screener + Workspace (docs/OPTIONS_SCREENER_RESULT.md, docs/WORKSPACE_RESULT.md): each mounts its routes\n"
  " # ahead of the catch-alls above, and one that fails to start is logged and skipped. Screener first: the\n"
  " # workspace's Greeks widget reuses its IV cache.\n"
  " from .screener import mount as mount_screener\n"
  " from .workspace import mount as mount_workspace\n"
  " mount_screener(app,settings)\n"
  " mount_workspace(app,settings)\n"
  " return app\n"),
 ('src/shell/routes.tsx',"route:'/workspace'",
  "{route:'/derivative',label:'Derivative',short:'Derivative',icon:'layers'},",
  "{route:'/derivative',label:'Derivative',short:'Derivative',icon:'layers'},"
  "{route:'/workspace',label:'Workspace',short:'Workspace',icon:'layout'},"
  "{route:'/screener',label:'Options Screener',short:'Screener',icon:'filter'},"),
 ('src/shell/routes.tsx',"path==='/workspace'",
  "if(path==='/'||path==='/discover'||path==='/derivative'||",
  "if(path==='/'||path==='/discover'||path==='/derivative'||path==='/workspace'||path==='/screener'||"),
 ('src/derivative/frame.tsx','BlockBareContext',
  "export function Block({title,subtitle,asOf,badge,linked,headline,info,actions,stacked,height,idle,\n chart,content}:BlockProps){\n",
  "/** Set by the Workspace around a reused block: the tile header already names it, so the block drops its own\n"
  " *  title row. The Derivative tab never sets it, so nothing there changes. */\n"
  "export const BlockBareContext=React.createContext(false);\n"
  "export function Block({title,subtitle,asOf,badge,linked,headline,info,actions,stacked,height,idle,\n chart,content}:BlockProps){\n"
  " const bare=React.useContext(BlockBareContext);\n"),
 ('src/derivative/frame.tsx','{!bare&&<View style={{gap:2,flexShrink:1,minWidth:0}}>',
  "   <View style={{gap:2,flexShrink:1,minWidth:0}}>\n    <T role=\"heading\" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:19,lineHeight:26,\n"
  "     letterSpacing:-0.3}}>{title}</T>\n    {!!subtitle&&<T style={{fontSize:12,lineHeight:17,color:C.muted}}>{subtitle}</T>}\n   </View>\n",
  "   {!bare&&<View style={{gap:2,flexShrink:1,minWidth:0}}>\n    <T role=\"heading\" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:19,lineHeight:26,\n"
  "     letterSpacing:-0.3}}>{title}</T>\n    {!!subtitle&&<T style={{fontSize:12,lineHeight:17,color:C.muted}}>{subtitle}</T>}\n   </View>}\n"),
 # --- inside a workspace tile, the block's figures and frames take the workspace's one scale (tokens.ts) -------
 # The headline figure is the workspace's metric (Inter 600 15/20) instead of the tab's 26px display figure.
 ('src/derivative/frame.tsx','const bareH=React.useContext(BlockBareContext);',
  " const missing=!value||value===HEADLINE_DASH;\n",
  " const missing=!value||value===HEADLINE_DASH;\n const bareH=React.useContext(BlockBareContext);\n"),
 ('src/derivative/frame.tsx',"bareH?{fontFamily:'InterSemi'",
  "    <T numberOfLines={1} style={{fontFamily:'ManropeBold',fontSize:26,lineHeight:32,letterSpacing:-0.6,",
  "    <T numberOfLines={1} style={{...(bareH?{fontFamily:'InterSemi',fontSize:15,lineHeight:20}:{fontFamily:'ManropeBold',fontSize:26,lineHeight:32,letterSpacing:-0.6}),"),
 # A panel that IS the tile's body (not one panel of a two-panel block) drops its own border and its uppercase name:
 # the tile already frames and names it. A panel inside a block keeps both — its name tells the two panels apart.
 ('src/derivative/frame.tsx','const bareF=React.useContext(BlockBareContext)&&!inBlock;',
  " const missing=missingText(body?.missing,showsSignals||[]);\n",
  " const missing=missingText(body?.missing,showsSignals||[]);\n const bareF=React.useContext(BlockBareContext)&&!inBlock;\n"),
 ('src/derivative/frame.tsx','borderWidth:bareF?0:1',
  " return <View role=\"region\" aria-label={name} style={[{backgroundColor:C.paper,borderWidth:1,borderColor:C.line,\n  borderRadius:12,overflow:'hidden',minWidth:0},style]}>\n",
  " return <View role=\"region\" aria-label={name} style={[{backgroundColor:C.paper,borderWidth:bareF?0:1,borderColor:C.line,\n  borderRadius:bareF?0:12,overflow:'hidden',minWidth:0},style]}>\n"),
 ('src/derivative/frame.tsx','{!bareF&&<T role="heading" aria-level={3}',
  "    <T role=\"heading\" aria-level={3} numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:11,lineHeight:16,\n     letterSpacing:1,textTransform:'uppercase'}}>{name}</T>\n",
  "    {!bareF&&<T role=\"heading\" aria-level={3} numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:11,lineHeight:16,\n     letterSpacing:1,textTransform:'uppercase'}}>{name}</T>}\n"),
]


def main(apply):
 texts={}
 plan=[]
 for rel,done_marker,old,new in EDITS:
  path=ROOT/rel
  text=texts.get(rel)
  if text is None:text=texts[rel]=path.read_text(encoding='utf-8')
  if done_marker in text:
   plan.append((rel,'already applied'));continue
  n=text.count(old)
  if n!=1:
   print(f'REFUSED {rel}: the anchor occurs {n} times (expected exactly 1). Nothing was written.');return 2
  texts[rel]=text.replace(old,new,1)
  plan.append((rel,f'+{new.count(chr(10))-old.count(chr(10))} lines'))
 for rel,what in plan:print(f'{"APPLY" if apply else "would change"}  {rel}: {what}')
 if apply:
  for rel,text in texts.items():(ROOT/rel).write_text(text,encoding='utf-8')
  print('Written. Next: scripts/start-pilot.ps1 -Build, then scripts/register-screener-task.ps1.')
 else:print('Dry run only — nothing was written. Re-run with --apply after 15:30 IST.')
 return 0


if __name__=='__main__':
 sys.exit(main('--apply' in sys.argv))
