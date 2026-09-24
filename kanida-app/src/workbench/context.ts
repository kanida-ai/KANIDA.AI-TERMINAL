// The workspace's shared state, handed to every widget: the ONE selected instrument (and the focus a scanner match
// carried with it), the reading the page is on, the strike under the pointer, and the few actions a widget may take
// on the workspace. A widget never resolves its own instrument when it follows the workspace.
import React from 'react';
import type {Selected,Widget} from './types';

export type WorkspaceCtx={
 selected:Selected;
 select:(next:Selected)=>void;
 /** Bumps when a new 15-min reading lands: every widget re-reads, once, through the hub. */
 seq:number;
 asOf:string|null;
 /** The strike under the pointer anywhere on the workspace — the Derivative tab's own cross-panel highlight. */
 strike:number|null;
 setStrike:(v:number|null)=>void;
 widgets:Widget[];
 /** Point a Screener results widget at a scanner (adding one if the workspace has none). */
 openScanner:(scannerId:string)=>void;
};
export const Ctx=React.createContext<WorkspaceCtx|null>(null);
export function useWorkspace(){const v=React.useContext(Ctx);if(!v)throw new Error('outside a workspace');return v;}

/** The instrument a widget reads: the workspace's when it follows, its own when pinned. */
export function instrumentOf(w:Widget,selected:Selected){return w.follow_workspace?(selected.underlying||''):(w.instrument||'');}
export function expiryOf(w:Widget,selected:Selected){
 if(w.expiry)return w.expiry;
 return w.follow_workspace?(selected.expiry||''):'';
}
