from __future__ import annotations
from contextlib import contextmanager
import time, uuid
from sqlalchemy import (create_engine,MetaData,Table,Column,String,Integer,BigInteger,Float,Boolean,Text,JSON,ForeignKey,UniqueConstraint,Index,select,event)

meta=MetaData()
def ident(): return uuid.uuid4().hex
def now(): return int(time.time())
users=Table('pilot_users',meta,
 Column('id',String(32),primary_key=True),Column('email',String(254),unique=True,nullable=False),
 Column('name',String(80),nullable=False),Column('password_hash',Text),Column('google_sub',String(128),unique=True),
 Column('role',String(20),nullable=False),Column('active',Boolean,nullable=False,default=True),
 Column('onboarded',Boolean,nullable=False,default=False),Column('policy_version',String(50)),
 Column('preferences',JSON,nullable=False,default=dict),Column('created',BigInteger,nullable=False))
invites=Table('pilot_invites',meta,Column('hash',String(64),primary_key=True),Column('email',String(254),nullable=False),
 Column('role',String(20),nullable=False),Column('expires',BigInteger,nullable=False),Column('used',BigInteger))
sessions=Table('pilot_sessions',meta,Column('hash',String(64),primary_key=True),Column('user_id',String(32),ForeignKey(users.c.id),nullable=False),
 Column('csrf',String(64),nullable=False),Column('kind',String(12),nullable=False),Column('created',BigInteger,nullable=False),
 Column('expires',BigInteger,nullable=False),Column('revoked',Boolean,nullable=False,default=False),Column('label',String(120)))
oauth=Table('pilot_oauth',meta,Column('hash',String(64),primary_key=True),Column('provider',String(20),nullable=False),
 Column('user_id',String(32)),Column('binding',String(64),nullable=False),Column('nonce',String(64)),Column('verifier',Text),
 Column('expires',BigInteger,nullable=False),Column('used',Boolean,nullable=False,default=False),Column('return_path',String(200)))
audit=Table('pilot_activity',meta,Column('id',String(32),primary_key=True),Column('user_id',String(32),ForeignKey(users.c.id)),
 Column('created',BigInteger,nullable=False),Column('kind',String(40),nullable=False),Column('title',String(150),nullable=False),
 Column('detail',Text,nullable=False),Column('object_id',String(64)),Column('data',JSON,nullable=False,default=dict))
subscriptions=Table('pilot_subscriptions',meta,Column('id',String(64),primary_key=True),Column('user_id',String(32),ForeignKey(users.c.id),nullable=False),
 Column('provider_id',String(80),unique=True),Column('plan_id',String(80)),Column('status',String(30),nullable=False),
 Column('current_end',BigInteger),Column('cancel_at_end',Boolean,nullable=False,default=False),Column('last_event',BigInteger,nullable=False,default=0),
 Column('created',BigInteger,nullable=False),Column('updated',BigInteger,nullable=False),Column('checkout_url',Text))
webhooks=Table('pilot_webhooks',meta,Column('id',String(128),primary_key=True),Column('provider',String(20),nullable=False),Column('created',BigInteger,nullable=False),Column('digest',String(64),nullable=False))
brokers=Table('pilot_brokers',meta,Column('user_id',String(32),ForeignKey(users.c.id),primary_key=True),Column('broker',String(20),nullable=False),
 Column('account_hash',String(64),unique=True),Column('masked_id',String(30)),Column('encrypted_token',Text),Column('expires',BigInteger),
 Column('status',String(30),nullable=False),Column('updated',BigInteger,nullable=False))
watches=Table('pilot_watches',meta,Column('user_id',String(32),ForeignKey(users.c.id),primary_key=True),Column('id',String(150),primary_key=True),Column('payload',JSON,nullable=False),Column('created',BigInteger,nullable=False))
plans=Table('pilot_plans',meta,Column('id',String(32),primary_key=True),Column('user_id',String(32),ForeignKey(users.c.id),nullable=False),
 Column('request_id',String(100),nullable=False),Column('payload_hash',String(64),nullable=False),Column('payload',JSON,nullable=False),Column('status',String(20),nullable=False),
 Column('created',BigInteger,nullable=False),Column('updated',BigInteger,nullable=False),UniqueConstraint('user_id','request_id'))
wallets=Table('pilot_wallets',meta,Column('user_id',String(32),ForeignKey(users.c.id),primary_key=True),Column('initial_paise',BigInteger,nullable=False),
 Column('cash_paise',BigInteger,nullable=False),Column('reserved_paise',BigInteger,nullable=False),Column('realized_paise',BigInteger,nullable=False),Column('paused',Boolean,nullable=False,default=False))
orders=Table('pilot_orders',meta,Column('id',String(32),primary_key=True),Column('user_id',String(32),ForeignKey(users.c.id),nullable=False),
 Column('plan_id',String(32)),Column('request_id',String(100),nullable=False),Column('request_hash',String(64),nullable=False),Column('mode',String(20),nullable=False),
 Column('symbol',String(60),nullable=False),Column('status',String(30),nullable=False),Column('product',String(10),nullable=False),
 Column('payload',JSON,nullable=False),Column('broker_id',String(80)),Column('filled_qty',Integer,nullable=False,default=0),
 Column('average_paise',BigInteger,nullable=False,default=0),Column('realized_paise',BigInteger,nullable=False,default=0),
 Column('created',BigInteger,nullable=False),Column('updated',BigInteger,nullable=False),UniqueConstraint('user_id','request_id'))
rate_limits=Table('pilot_rate_limits',meta,Column('key',String(128),primary_key=True),Column('window',BigInteger,nullable=False),Column('count',Integer,nullable=False))
# Strategy registry (docs/FALCON_DISCOVER_SPEC.md §6). Block `kind` and strategy `source_type`/`source_config` are open strings/JSON,
# so quant, results and options blocks can be added later without a schema change.
strategy_blocks=Table('pilot_strategy_blocks',meta,Column('key',String(40),primary_key=True),Column('title',String(80),nullable=False),
 Column('description',String(300),nullable=False,default=''),Column('kind',String(20),nullable=False),Column('position',Integer,nullable=False,default=0),
 Column('enabled',Boolean,nullable=False,default=False),Column('created',BigInteger,nullable=False),Column('updated',BigInteger,nullable=False))
strategies=Table('pilot_strategies',meta,Column('key',String(64),primary_key=True),Column('block_key',String(40),ForeignKey(strategy_blocks.c.key),nullable=False),
 Column('name',String(100),nullable=False),Column('description',String(500),nullable=False,default=''),Column('tags',JSON,nullable=False,default=list),
 Column('source_type',String(30),nullable=False),Column('source_config',JSON,nullable=False,default=dict),Column('audience',String(10),nullable=False),
 Column('min_trades',Integer,nullable=False,default=10),Column('default_slot',String(1)),Column('position',Integer,nullable=False,default=0),
 Column('enabled',Boolean,nullable=False,default=False),Column('created_by',String(32)),Column('created',BigInteger,nullable=False),Column('updated',BigInteger,nullable=False))
# Access administration (cloud preview): shareable invite CODES (stored hashed; shown once), and the request-access waitlist.
invite_codes=Table('pilot_invite_codes',meta,Column('id',String(32),primary_key=True),Column('hash',String(64),unique=True,nullable=False),
 Column('last4',String(8),nullable=False),Column('uses_max',Integer,nullable=False,default=1),Column('uses',Integer,nullable=False,default=0),
 Column('expires',BigInteger),Column('revoked',BigInteger),Column('note',String(200),nullable=False,default=''),Column('created_by',String(32)),Column('created',BigInteger,nullable=False))
access_requests=Table('pilot_access_requests',meta,Column('id',String(32),primary_key=True),Column('email',String(254),nullable=False),
 Column('name',String(80),nullable=False,default=''),Column('note',String(500),nullable=False,default=''),Column('status',String(12),nullable=False,default='pending'),
 Column('created',BigInteger,nullable=False),Column('decided',BigInteger),Column('decided_by',String(32)),Column('invite_url_issued',Boolean,nullable=False,default=False))
Index('idx_pilot_access_requests_status',access_requests.c.status,access_requests.c.created)
schema=Table('pilot_schema',meta,Column('version',Integer,primary_key=True),Column('applied',BigInteger,nullable=False))
for table in (sessions,audit,subscriptions,plans,orders):
 Index('idx_'+table.name+'_user',table.c.user_id)

class Database:
 def __init__(self,url):
  self.engine=create_engine(url,pool_pre_ping=True,**({'connect_args':{'check_same_thread':False,'timeout':30}} if url.startswith('sqlite') else {}))
  if self.engine.dialect.name=='sqlite':
   @event.listens_for(self.engine,'connect')
   def configure(conn,_):
    conn.execute('PRAGMA foreign_keys=ON');conn.execute('PRAGMA busy_timeout=30000');conn.execute('PRAGMA journal_mode=WAL')
 def migrate(self):
  meta.create_all(self.engine)
  with self.tx() as c:
   if not c.execute(select(schema.c.version).where(schema.c.version==1)).first():c.execute(schema.insert().values(version=1,applied=now()))
 @contextmanager
 def tx(self):
  with self.engine.connect() as c:
   if self.engine.dialect.name=='sqlite': c.exec_driver_sql('BEGIN IMMEDIATE')
   else:c.begin()
   try:yield c;c.commit()
   except BaseException:c.rollback();raise
 def close(self):self.engine.dispose()

def record(c,user_id,kind,title,detail,object_id=None,data=None):
 c.execute(audit.insert().values(id=ident(),user_id=user_id,kind=kind,title=title,detail=detail,object_id=object_id,data=data or {},created=now()))
def row(c,query):
 value=c.execute(query).mappings().first()
 return dict(value) if value else None
