// Phase 0 (wave 0B) workspace parts. The shell (PilotShell / MainWorkspace) mounts these; src/Workspace.tsx composes them for the current "/" route.
export {ChartCentre,SetupSummary,matchKey,openSimulate,type ChartCentreProps} from './ChartCentre';
export {DiscoverPanel,useDiscoverRows,useWorkspaceMatch,resolveSymbolMatch,historyFor,discoverSort,verdictTone,type DiscoverPanelProps,type DiscoverRows,type WorkspaceMatch} from './DiscoverPanel';
export {EvidencePanel} from './EvidencePanel';
