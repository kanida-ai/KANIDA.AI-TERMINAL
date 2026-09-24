// The KANIDA Workspace (docs/WORKSPACE_SPEC.md) — a normal member page under the shell header. Its nav entry is a
// one-line hook in src/shell/routes.tsx NAV_ITEMS, applied by the owner after close; until then it is reached at
// /workspace.
import React from 'react';
import {WorkbenchTab} from '../src/workbench';
export default function WorkspaceRoute(){return <WorkbenchTab/>;}
