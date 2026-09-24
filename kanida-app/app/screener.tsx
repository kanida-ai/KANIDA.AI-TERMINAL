// Options Screener (docs/OPTIONS_SCREENER_SPEC.md). A member page under the shell header. Its nav entry is a
// one-line hook in src/shell/routes.tsx NAV_ITEMS, applied by the owner; until then it is reached at /screener.
import React from 'react';
import {ScreenerTab} from '../src/screener';
export default function ScreenerRoute(){return <ScreenerTab/>;}
