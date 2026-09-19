import os
import uvicorn
if __name__=='__main__':
 uvicorn.run('kanida_pilot.app:create_app',factory=True,host=os.getenv('PILOT_BIND','127.0.0.1'),port=int(os.getenv('PILOT_PORT','8083')),
  access_log=False,proxy_headers=True,forwarded_allow_ips='127.0.0.1')
