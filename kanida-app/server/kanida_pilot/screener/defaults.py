"""The KANIDA default scanners. DATA, not logic: each is an ordinary definition, validated by the same
`definition.normalize` a user's scanner goes through, and seeded into var/screener.db on start. Adding one is
adding an entry here — nothing else in the screener knows these exist.

Premium surge reads "Expanding rapidly", which is the block's own pace rule (the latest move more than 1.25x the
one before, at every reading of the window), not a fixed percentage (owner decision Q1).
"""
from __future__ import annotations

ATM5={'kind':'atm','below':5,'above':5}


def _c(metric,state,side=None,minutes=45,join=None,window=None):
 c={'metric':metric,'state':state,'window':window or {'kind':'minutes','value':minutes}}
 if side:c['side']=side
 if join:c['join']=join
 return c


DEFAULTS=[
 ('call-oi-building','Call OI building','Call open interest has risen at every reading for the last 45 minutes.',
  {'strikes':ATM5,'conditions':[_c('oi','up_cont','CE')]}),
 ('put-oi-building','Put OI building','Put open interest has risen at every reading for the last 45 minutes.',
  {'strikes':ATM5,'conditions':[_c('oi','up_cont','PE')]}),
 ('call-iv-expanding','Call IV expanding','Call implied volatility has expanded at every reading for the last 45 minutes.',
  {'strikes':ATM5,'conditions':[_c('iv','up_cont','CE')]}),
 ('put-iv-expanding','Put IV expanding','Put implied volatility has expanded at every reading for the last 45 minutes.',
  {'strikes':ATM5,'conditions':[_c('iv','up_cont','PE')]}),
 ('call-premium-surge','Call premium surge','Call premium rising at every reading of the last 30 minutes, and speeding up.',
  {'strikes':ATM5,'conditions':[_c('premium','up_rapid','CE',30)]}),
 ('put-premium-surge','Put premium surge','Put premium rising at every reading of the last 30 minutes, and speeding up.',
  {'strikes':ATM5,'conditions':[_c('premium','up_rapid','PE',30)]}),
 ('max-pain-higher','Max pain moving higher','The max-pain strike has shifted higher since the open.',
  {'conditions':[_c('maxpain','up',window={'kind':'open'})]}),
 ('max-pain-lower','Max pain moving lower','The max-pain strike has shifted lower since the open.',
  {'conditions':[_c('maxpain','down',window={'kind':'open'})]}),
 ('pcr-building','PCR building','The put-call ratio has risen at every reading for the last 45 minutes.',
  {'conditions':[_c('pcr','up_cont')]}),
 ('pcr-falling','PCR falling','The put-call ratio has fallen at every reading for the last 45 minutes.',
  {'conditions':[_c('pcr','down_cont')]}),
 ('broad-oi-buildup','Broad OI build-up','OI is being added across more nearby strikes than 30 minutes ago, not at one strike.',
  {'strikes':ATM5,'conditions':[_c('spread','broadening','either',30)]}),
 ('unusual-near-atm','Unusual activity near ATM','Contracts within two strikes of ATM trading unusually heavily right now.',
  {'strikes':{'kind':'atm','below':2,'above':2},'conditions':[_c('volume','unusual','either',15)]}),
 ('call-writing-near-atm','Call writing near ATM','Call premium falling while call OI builds, over the last 45 minutes.',
  {'strikes':{'kind':'atm','below':3,'above':3},'conditions':[_c('flow','writing','CE')]}),
 ('put-writing-near-atm','Put writing near ATM','Put premium falling while put OI builds, over the last 45 minutes.',
  {'strikes':{'kind':'atm','below':3,'above':3},'conditions':[_c('flow','writing','PE')]}),
]


def seed(store):
 from .definition import digest,normalize
 for position,(slug,name,description,raw) in enumerate(DEFAULTS):
  d=normalize(raw)
  store.upsert_default(slug,name,d,digest(d),position,description)
