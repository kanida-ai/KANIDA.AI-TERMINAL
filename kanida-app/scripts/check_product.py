"""Exercise the rendered product against a temporary, loopback-only plan store."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(APP.parent))
from market_scanner import product

with tempfile.TemporaryDirectory(prefix='kanida-product-check-') as temporary:
    product.DB=Path(temporary)/'plans.sqlite3'
    product.initialize()
    server=ThreadingHTTPServer(('127.0.0.1',0),product.make_handler(0))
    thread=threading.Thread(target=server.serve_forever,daemon=True)
    thread.start()
    environment=dict(os.environ,KANIDA_TEST_URL=f'http://127.0.0.1:{server.server_port}',KANIDA_TEST_SAVE='1')
    try:
        result=subprocess.run(['node',str(APP/'scripts'/'preview-check.cjs')],cwd=APP,env=environment,
                              capture_output=True,encoding='utf-8',errors='replace',creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
        print(result.stdout,flush=True)
        print(result.stderr,flush=True)
        if result.returncode==0:
            state=product.list_product()
            assert len(state['plans'])==3 and all(p['status']=='archived' for p in state['plans'])
            assert len(state['events'])==15
            assert state['watchlist']==[]
            print('Verified 3 saved plans and 15 persisted plan/watch events in the isolated test database.')
    finally:
        server.shutdown()
        server.server_close()
    sys.exit(result.returncode)
