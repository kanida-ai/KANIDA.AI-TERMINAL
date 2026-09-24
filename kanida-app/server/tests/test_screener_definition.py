"""The scanner definition, the natural-language parser and the lifecycle — the parts with no store."""
import pytest
from kanida_pilot.screener import definition as D,lifecycle as L,nl,vocab as V
from kanida_pilot.screener.defaults import DEFAULTS

UNDERLYINGS=['NIFTY','BANKNIFTY','RELIANCE','IDEA','HDFCBANK']


def test_normalize_fills_defaults_and_is_canonical():
 d=D.normalize({'conditions':[{'metric':'oi','side':'CE','state':'up_cont'}]})
 assert d['universe']=={'kind':'all'} and d['expiry']=='nearest'
 assert d['strikes']=={'kind':'atm','below':5,'above':5}
 assert d['conditions'][0]['window']=={'kind':'minutes','value':45}
 assert D.digest(d)==D.digest(D.normalize(d))


@pytest.mark.parametrize('raw,needle',[
 ({'conditions':[]},'at least one condition'),
 ({'conditions':[{'metric':'oi','state':'unusual'}]},'OI cannot be'),
 ({'conditions':[{'metric':'pcr','state':'up','window':{'kind':'minutes','value':20}}]},'whole number of 15-min'),
 ({'conditions':[{'metric':'oi','side':'both'}]},'cannot be read on'),
 ({'conditions':[{'metric':'nope'}]},'Unknown metric'),
 ({'strikes':{'kind':'atm','below':30,'above':1},'conditions':[{'metric':'oi'}]},'at most 10'),
 ({'conditions':[{'metric':'iv','window':{'kind':'custom','from':'11:30','to':'10:15'}}]},'end after'),
])
def test_normalize_refuses_with_a_sentence(raw,needle):
 with pytest.raises(D.DefinitionError) as e:D.normalize(raw)
 assert needle in str(e.value)


def test_and_binds_tighter_than_or():
 d=D.normalize({'conditions':[{'metric':'oi'},{'metric':'iv','join':'and'},{'metric':'pcr','join':'or'}]})
 assert [[c['metric'] for c in g] for g in D.groups(d)]==[['oi','iv'],['pcr']]


def test_grain():
 one=D.normalize({'conditions':[{'metric':'oi','side':'CE'},{'metric':'iv','side':'CE'},{'metric':'pcr'}]})
 assert D.grain(one)=='contract'
 cross=D.normalize({'conditions':[{'metric':'oi','side':'CE'},{'metric':'oi','side':'PE','state':'down'}]})
 assert D.grain(cross)=='book'
 either=D.normalize({'conditions':[{'metric':'oi','side':'CE'},{'metric':'iv','side':'either'}]})
 assert D.grain(either)=='contract'
 assert D.grain(D.normalize({'conditions':[{'metric':'maxpain'}]}))=='book'


def test_reads_as_is_plain_words():
 d=D.normalize({'universe':'stocks','conditions':[{'metric':'oi','side':'CE','state':'up_cont'},
  {'metric':'iv','side':'CE','state':'up','join':'and','window':{'kind':'minutes','value':30}}]})
 text=D.reads_as(d)
 assert text.startswith('Call OI increasing continuously over the last 45 min AND call IV expanding over the last 30 min')
 assert 'ATM ±5 of the nearest expiry, across F&O stocks' in text
 for banned in ('buy','sell','good','bad','best'):assert banned not in text.lower().split()


def test_reads_as_keeps_acronyms_and_drops_the_range_for_book_scanners():
 d=D.normalize({'universe':'indices','conditions':[{'metric':'maxpain','state':'up','window':{'kind':'open'}},
  {'metric':'pcr','state':'up','join':'and','window':{'kind':'minutes','value':30}}]})
 assert D.reads_as(d)==('Max pain shifting higher since market open AND PCR rising over the last 30 min, '
  'for the nearest expiry, across indices.')


def test_every_default_is_a_valid_definition():
 slugs=set()
 for slug,name,desc,raw in DEFAULTS:
  d=D.normalize(raw);assert D.reads_as(d);slugs.add(slug)
 assert len(slugs)==len(DEFAULTS)>=10


def test_vocabulary_words_are_the_owners():
 words=set(V.STATE_WORDS.values())
 for w in ('Increasing','Decreasing','Increasing continuously','Decreasing continuously','Expanding rapidly',
  'Contracting rapidly','Stable','Reversing higher','Reversing lower','Unusually active','Quiet',
  'Broadening across strikes','Concentrating at fewer strikes','Shifting higher','Shifting lower'):
  assert w in words
 assert V.state_word('iv','up')=='Expanding' and V.state_word('maxpain','up')=='Shifting higher'


# --- natural language ------------------------------------------------------------------------------------
def test_the_owners_example_sentence():
 r=nl.parse('Show me F&O stocks where call OI has been increasing for the last 45 minutes and call IV is expanding.',UNDERLYINGS)
 d=r['definition']
 assert d['universe']=={'kind':'stocks'}
 c1,c2=d['conditions']
 assert (c1['metric'],c1['side'],c1['state'],c1['window'])==('oi','CE','up_cont',{'kind':'minutes','value':45})
 assert (c2['metric'],c2['side'],c2['state'],c2['join'])==('iv','CE','up','and')
 assert not r['unmapped']


def test_premium_percentage_reads_as_rapid_and_says_so():
 r=nl.parse('call premium has grown 100% in the last two 15 min',UNDERLYINGS)
 c=r['definition']['conditions'][0]
 assert (c['metric'],c['side'],c['state'],c['window'])==('premium','CE','up_rapid',{'kind':'readings','value':2})
 assert any('not a threshold' in a for a in r['assumptions'])


@pytest.mark.parametrize('text,metric,state,window',[
 ('max pain shifting higher since open','maxpain','up',{'kind':'open'}),
 ('PCR increasing 45 min','pcr','up',{'kind':'minutes','value':45}),
 ('put IV expanding rapidly 30 min','iv','up_rapid',{'kind':'minutes','value':30}),
 ('OI spreading across multiple strikes last 30 minutes','spread','broadening',{'kind':'minutes','value':30}),
 ('unusual volume near atm','volume','unusual',{'kind':'minutes','value':15}),
 ('call writing last hour','flow','writing',{'kind':'minutes','value':60}),
 ('underlying reversing higher last 30 min','underlying','rev_up',{'kind':'minutes','value':30}),
 ('premium rising continuously 3 readings','premium','up_cont',{'kind':'readings','value':3}),
])
def test_phrases(text,metric,state,window):
 c=nl.parse(text,UNDERLYINGS)['definition']['conditions'][0]
 assert (c['metric'],c['state'],c['window'])==(metric,state,window)


def test_symbols_indices_scope_and_carry_over():
 r=nl.parse('NIFTY call OI building 30 min or puts decreasing, atm ±3, next expiry',UNDERLYINGS)
 d=r['definition']
 assert d['universe']=={'kind':'symbols','symbols':['NIFTY']}
 assert d['strikes']=={'kind':'atm','below':3,'above':3} and d['expiry']=='next'
 assert d['conditions'][1]['join']=='or' and d['conditions'][1]['metric']=='oi' and d['conditions'][1]['side']=='PE'
 assert d['conditions'][1]['window']=={'kind':'minutes','value':30}


def test_lowercase_words_are_not_stock_symbols():
 r=nl.parse('good idea: call oi building',UNDERLYINGS)
 assert r['definition']['universe']=={'kind':'all'}


def test_unmapped_words_are_returned_not_dropped():
 r=nl.parse('call oi building and moon phase aggressive',UNDERLYINGS)
 assert r['unmapped'] and 'moon' in r['unmapped'][0]


def test_nothing_understood_is_an_error():
 with pytest.raises(D.DefinitionError):nl.parse('hello there',UNDERLYINGS)


def test_minutes_are_rounded_to_readings_and_said():
 r=nl.parse('call oi increasing last 40 minutes',UNDERLYINGS)
 assert r['definition']['conditions'][0]['window']=={'kind':'minutes','value':45}
 assert any('rounded' in a for a in r['assumptions'])


# --- lifecycle -------------------------------------------------------------------------------------------
def test_lifecycle_walk():
 matched=[None,False,True,True,True,None,True,False,False,True]
 pace=[None,None,None,None,'strengthening',None,'weakening',None,None,None]
 timeline,episodes,events=L.walk(matched,pace)
 assert timeline==['unseen','none','new','still','strengthening','gap','weakening','ended','none','new']
 assert [(e['start'],e['end'],e['readings']) for e in episodes]==[(2,7,4),(9,None,1)]
 assert events==[(2,'new'),(4,'changed'),(6,'changed'),(7,'ended'),(9,'new')]
 assert L.current(timeline)==('new',9)


def test_same_pace_twice_is_not_a_second_change():
 _t,_e,events=L.walk([True,True,True,True],[None,'strengthening',None,'strengthening'])
 assert [k for _i,k in events]==['new','changed']
