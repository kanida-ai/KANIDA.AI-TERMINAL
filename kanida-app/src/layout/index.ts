// KANIDA layout primitives (Phase 0, task 0.2). Presentational only: no data fetching, no app state.
export {Dock,DOCK_TAB_BAR,DOCK_HANDLE,DOCK_DEFAULT_HEIGHT,PanelVisible,usePanelVisible,type DockTab,type DockProps,type DockStore} from './Dock';
export {Sidebar,SIDEBAR_DEFAULT_WIDTH,type SidebarProps,type SidebarWidget,type SidebarRailItem,type SidebarStore} from './Sidebar';
export {Widget,WidgetError,type WidgetProps,type WidgetMenuItem,type WidgetErrorState,type WidgetStatusLink} from './Widget';
export {Popover,MenuList,useDismiss,type PopoverProps,type PopoverAnchor,type PopoverRect,type PopoverPlacement,type MenuItem,type DismissOptions} from './Popover';
export {BottomSheet,SHEET_SNAPS,type SheetSnap,type BottomSheetProps} from './BottomSheet';
export {TopBar,DataAgePill,dataAgeDays,dataAgeTone,formatDataDate,type TopBarProps,type DataAgePillProps,type DataAgeTone} from './TopBar';
export {DataStatusPanel,DataStatusPopover,type DataStatusPanelProps,type DataStatusPopoverProps} from './DataStatusPanel';
// F&O capture health, the feed the data pill above does NOT describe (src/layout/fnoCapture.ts).
export {FNO_POLL_MS,FNO_SEPARATE_TEXT,fnoCaptureView,useFnoCapture,resetFnoCapture,type FnoCapture,type FnoCaptureView} from './fnoCapture';
export {dataStatusView,dataStatusTone,pillContent,connectionView,RECONNECT_GRACE_SECONDS,barLagSeconds,formatIst,agoText,inText,istMs,type DataStatus,type DataStatusView,type DataStatusRow,type DataStatusTone,type DataPillContent,type CacheProvenance,type ConnectionView,type ConnectionMode} from './dataStatus';
export {ToolRail,type ToolRailProps,type ToolRailItem} from './ToolRail';
export {IconButton,useDragResize,useTitle,useRoving,readStore,writeStore,storeKey,type IconButtonProps,type DragResizeOpts} from './shared';
