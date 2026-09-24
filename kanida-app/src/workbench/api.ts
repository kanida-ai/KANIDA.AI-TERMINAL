// The workspace's own calls. Every widget's data comes through the app's `api`, and so through the page's hub.
import {api} from '../model';
import type {Definition,Registry,Summary,Template,Workspace} from './types';

export const wsApi={
 registry:()=>api('/api/workspace/registry') as Promise<Registry>,
 templates:()=>api('/api/workspace/templates') as Promise<{templates:Template[]}>,
 list:()=>api('/api/workspace/workspaces') as Promise<{workspaces:Workspace[]}>,
 create:(template:string,name?:string)=>api('/api/workspace/workspaces',{template,name}) as Promise<Workspace>,
 save:(id:string,definition:Definition,version:number)=>api(`/api/workspace/workspaces/${id}`,{definition,version}) as Promise<Workspace>,
 duplicate:(id:string)=>api(`/api/workspace/workspaces/${id}/duplicate`,{}) as Promise<Workspace>,
 remove:(id:string)=>api(`/api/workspace/workspaces/${id}/delete`,{}) as Promise<{ok:boolean;workspaces:Workspace[]}>,
 summary:(body:{underlying:string;sources:string[];scope:string;focus:any})=>api('/api/workspace/summary',body) as Promise<Summary>,
};
