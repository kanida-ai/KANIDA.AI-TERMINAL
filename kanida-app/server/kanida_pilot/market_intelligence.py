"""Deterministic session replay. Provider-neutral observations in, structured events out.

No network, clock, database, language model or user identity. Thresholds are versioned
research heuristics, not calibrated probabilities or proof of buyer/seller intent.
"""
from __future__ import annotations
from collections import Counter
from datetime import datetime
import hashlib
import math

VERSION = 'session-engine-v1'
POLICY = dict(oi_fraction=.01, price_fraction=.01, iv_points=.25,
              opening_multiplier=2, opening_end='09:45', max_gap_multiple=1.5)
BEHAVIORS = {'buying', 'writing', 'covering', 'unwinding'}


def number(value):
    try:
        out = float(value)
        return out if math.isfinite(out) else None
    except (ValueError, TypeError):
        return None


def delta(current, previous):
    a, b = number(current), number(previous)
    return a-b if a is not None and b is not None else None


def stamp(value):
    return datetime.fromisoformat(value)


def minutes(a, b):
    return int((stamp(a)-stamp(b)).total_seconds()/60)


def classify(current, previous, regime):
    facts = {key: current.get(key) for key in ('strike','side','token','oi','price','iv','volume',
        'bid','ask','source','iv_source','iv_reason','last_trade_time','quality')}
    facts.update(oi_change=None, price_change=None, iv_change=None, interval_volume=None)
    if previous is None:
        return {**facts, 'behavior':'unconfirmed', 'reason':'No adjacent reading for this contract.'}
    for field in ('oi','price','iv'):
        facts[field+'_change'] = delta(current.get(field), previous.get(field))
    facts['interval_volume'] = delta(current.get('volume'), previous.get('volume'))
    oi, price, iv, volume = [facts[k] for k in ('oi_change','price_change','iv_change','interval_volume')]
    quality = current.get('quality') or previous.get('quality')
    if quality:
        return {**facts,'behavior':'unconfirmed','reason':quality}
    if any(x is None for x in (oi,price,volume)) or volume <= 0:
        return {**facts,'behavior':'unconfirmed','reason':'OI, premium or interval traded volume does not confirm activity.'}
    lot = max(1, number(current.get('lot_size')) or 1)
    multiplier = POLICY['opening_multiplier'] if regime == 'opening' else 1
    oi_floor = max(lot, abs(number(previous.get('oi')) or 0)*POLICY['oi_fraction'])*multiplier
    price_floor = max(number(current.get('tick_size')) or .05,
                      abs(number(previous.get('price')) or 0)*POLICY['price_fraction'])*multiplier
    if abs(oi) < oi_floor or abs(price) < price_floor:
        return {**facts,'behavior':'unconfirmed','reason':'Position or premium change is small or conflicting.'}
    if regime == 'expiry':
        return {**facts,'behavior':'unconfirmed','reason':'Expiry-day changes require a separate calibrated interpretation.'}
    if iv is None:
        return {**facts,'behavior':'unconfirmed','reason':'Positions and premiums changed, but IV confirmation is unavailable.'}
    if oi > 0:
        behavior = 'buying' if price > 0 and iv >= POLICY['iv_points'] else (
            'writing' if price < 0 and iv <= -POLICY['iv_points'] else 'unconfirmed')
    else:
        behavior = 'covering' if price > 0 and iv >= POLICY['iv_points'] else (
            'unwinding' if price < 0 and iv <= -POLICY['iv_points'] else 'unconfirmed')
    return {**facts,'behavior':behavior,'reason':('OI, premium and IV changes align; participant intent is inferred.'
            if behavior != 'unconfirmed' else 'OI and premium changes lack confirming IV movement.')}


def explain(event):
    """Language is a rendering of the event, never a second market classifier."""
    side = 'Call' if event['side']=='CE' else 'Put'
    state, behavior = event['current_state'], event['behavior']
    lead = event['leading_strike']
    location = f"near {lead:g} {event['side']}" if lead is not None else 'across the observed strikes'
    if state == 'initial':
        return dict(headline=f'{side} activity baseline recorded',
            explanation='This is the first available reading. A comparable next reading is needed to identify new activity.',
            context=f"Observed at {event['timestamp'][11:16]} IST; an earlier opening state is not assumed.")
    if behavior == 'unconfirmed':
        title = 'earlier activity is unconfirmed' if event['previous_state'] else 'activity remains unconfirmed'
        detail = event['uncertainty'][0] if event['uncertainty'] else 'The available evidence does not support a clear participant interpretation.'
        return dict(headline=f'{side} {title}', explanation=detail,
            context=('The earlier pattern remains in session memory; this reading does not extend its confirmed duration.'
                     if event['previous_state'] else f"Reading at {event['timestamp'][11:16]} IST."))
    labels = {'appearing':'appears','continuing':'continues','building':'is building','slowing':'is slowing',
              'broadening':'is broadening','concentrating':'is concentrating','shifting':'is shifting',
              'reversing':'changes the earlier pattern','resuming':'is confirmed again'}
    wording = labels.get(state,state)
    evidence = next(e for e in event['evidence'] if e['strike']==lead and e['behavior']==behavior)
    oiword = 'risen' if evidence['oi_change']>0 else 'fallen'
    priceword = 'risen' if evidence['price_change']>0 else 'fallen'
    explanation = (f"Positions have {oiword}, while premiums and IV have {priceword} at the leading strike, "
                   f"consistent with {side.lower()} {behavior}.")
    count = event['breadth']['participating']
    context = f"{count} strike{'s' if count!=1 else ''} participate; first detected at {event['first_detected_time'][11:16]} IST."
    if event['consecutive_scans']>1:
        context += f" Confirmed in {event['consecutive_scans']} consecutive readings, spanning {event['duration_minutes']} minutes."
    if event['uncertainty']:
        context += ' '+event['uncertainty'][0]
    return dict(headline=f'{side} {behavior} {wording} {location}',explanation=explanation,context=context)


def replay(instrument, expiry, scans, cadence_minutes=15):
    """Replay one session in event time, preserving absolute strike identity as ATM moves.

    Every observed strike is evaluated; display proximity never trims session memory.
    A missing interval or missing contract is not a zero or a continuation.
    """
    scans = sorted(scans,key=lambda x:x['at'])
    if len({s['at'] for s in scans}) != len(scans):
        raise ValueError('Duplicate scan times must be normalized before replay')
    if len({s['at'][:10] for s in scans}) > 1:
        raise ValueError('Replay accepts exactly one trading session')
    events, memory, previous_rows, opening = [], {}, {}, {}
    previous_at = None
    for scan in scans:
        at = scan['at']
        if len({r['token'] for r in scan['contracts']}) != len(scan['contracts']):
            raise ValueError('Duplicate contracts within a scan')
        gap = previous_at is not None and minutes(at,previous_at)>cadence_minutes*POLICY['max_gap_multiple']
        regime = 'expiry' if at[:10]==expiry else ('opening' if at[11:16]<POLICY['opening_end'] else 'regular')
        ladder = sorted({r['strike'] for r in scan['contracts'] if number(r.get('strike')) is not None})
        spot = number(scan.get('spot'))
        atm = min(ladder,key=lambda k:(abs(k-spot),k)) if ladder and spot is not None else None
        for side in ('CE','PE'):
            rows = [r for r in scan['contracts'] if r['side']==side]
            evidence = [classify(r, None if gap else previous_rows.get(r['token']),regime) for r in rows]
            for e in evidence:
                base=opening.get(e['token'])
                e['since_first_observed_oi']=delta(e['oi'],base.get('oi')) if base else None
                e['since_first_observed_price']=delta(e['price'],base.get('price')) if base else None
            counts = Counter(e['behavior'] for e in evidence if e['behavior'] in BEHAVIORS)
            # No winner is forced when different behaviors tie in breadth.
            ranking = counts.most_common()
            behavior = ranking[0][0] if ranking and (len(ranking)==1 or ranking[0][1]>ranking[1][1]) else 'unconfirmed'
            active = [e for e in evidence if e['behavior']==behavior] if behavior in BEHAVIORS else []
            # Leadership combines position change, interval participation and relative price/IV movement.
            # Components are normalized within this behavior, not compared across incompatible units.
            scales={k:max([abs(e[k]) for e in active] or [1]) or 1
                    for k in ('oi_change','interval_volume','price_change','iv_change')}
            leader=max(active,key=lambda e:(sum(abs(e[k])/scales[k] for k in scales),-e['strike'])) if active else None
            strikes=sorted({e['strike'] for e in active})
            old=memory.get(side)
            uncertainty=[]
            if gap: uncertainty.append('A capture gap breaks continuity; this reading establishes a new comparison baseline.')
            if regime=='opening': uncertainty.append('Opening conditions use wider noise thresholds.')
            if regime=='expiry': uncertainty.append('Expiry-day evidence is withheld from ordinary-day buying and writing classifications.')
            if len(counts)>1: uncertainty.append('Other strikes show conflicting behavior; this is not a uniform market move.')
            if not active and evidence: uncertainty.append(Counter(e['reason'] for e in evidence).most_common(1)[0][0])
            if not rows: uncertainty.append(f'No {side} contracts were captured at this reading.')
            if any(e.get('iv_source')=='computed' for e in active):
                uncertainty.append('IV is model-derived from premium and spot, not an independent exchange measurement.')
            if active:
                uncertainty.append('OI and prices cannot identify individual buyer or seller intent; this interpretation is provisional.')
            strength=sum(abs(e['oi_change']) for e in active)
            count, first, duration=1,at,0
            state='initial' if previous_at is None else 'unconfirmed'
            if active:
                same=bool(old and old['behavior']==behavior and not gap)
                if same:
                    first=old['first']; count=old['count']+1 if old['last']==previous_at else 1
                    duration=minutes(at,old['streak_start']) if count>1 else 0
                    if old['last']!=previous_at: state='resuming'
                    elif set(strikes)>set(old['strikes']): state='broadening'
                    elif set(strikes)<set(old['strikes']): state='concentrating'
                    elif leader['strike']!=old['lead']: state='shifting'
                    elif strength>old['strength']*1.25: state='building'
                    elif strength<old['strength']*.75: state='slowing'
                    else: state='continuing'
                else: state='reversing' if old and not gap else 'appearing'
                memory[side]=dict(behavior=behavior,first=first,count=count,last=at,
                    streak_start=old['streak_start'] if same and count>1 else at,
                    strikes=strikes,lead=leader['strike'],strength=strength,state=state)
            if gap: memory.pop(side,None)
            breadth='isolated' if len(strikes)==1 else 'clustered' if len(strikes)>1 else 'unconfirmed'
            if len(strikes)>1 and any(ladder.index(b)-ladder.index(a)>1 for a,b in zip(strikes,strikes[1:])):
                breadth='dispersed'
            event=dict(id=hashlib.sha256(f'{VERSION}|{instrument}|{expiry}|{at}|{side}'.encode()).hexdigest()[:24],
                engine_version=VERSION,timestamp=at,session=at[:10],instrument=instrument,expiry=expiry,
                side=side,current_atm=atm,event_type=state,current_state=state,
                previous_state=old['state'] if old else None,previous_behavior=old['behavior'] if old else None,
                behavior=behavior,first_detected_time=first if active else (old['first'] if old else None),
                duration_minutes=duration,consecutive_scans=count if active else 0,
                leading_strike=leader['strike'] if leader else None,strike_cluster=strikes,
                breadth=dict(kind=breadth,participating=len(strikes),observed=len(rows),previous=len(old['strikes']) if old else 0),
                regime=regime,evidence=evidence,uncertainty=uncertainty,
                historical_significance=None,baselines=dict(previous_scan=previous_at,
                    first_observed=scans[0]['at'],market_open=scans[0]['at'] if scans[0]['at'][11:16]=='09:15' else None,
                    previous_close=None,historical_normal=None),
                cross_market=dict(spot=spot,futures_price=scan.get('futures_price'),futures_oi=scan.get('futures_oi'),
                    interpretation=None),policy=POLICY)
            event['narrative']=explain(event)
            events.append(event)
        previous_rows={r['token']:r for r in scan['contracts']}
        for token,row in previous_rows.items(): opening.setdefault(token,row)
        previous_at=at
    return events
