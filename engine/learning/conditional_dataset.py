from __future__ import annotations

from typing import Any

from engine.readers.sqlite_reader import ReadOnlySQLite


def load_learning_rows(db: ReadOnlySQLite, config: dict[str, Any]) -> list[dict[str, Any]]:
    markets = config["markets"]
    placeholders = ",".join("?" for _ in markets)
    timeframes = config.get("timeframes") or ["1D", "1W"]
    tf_placeholders = ",".join("?" for _ in timeframes)
    params: list[Any] = [*markets, *timeframes]
    roster_join = """
        left join signal_roster r
          on r.market=e.market
         and r.ticker=e.ticker
         and r.timeframe=e.timeframe
         and r.strategy_name=e.strategy_name
         and r.bias=e.bias
    """
    roster_filter = ""
    if config.get("roster_passed_only", True):
        roster_filter = "and coalesce(r.tier, '') not in ('', 'retired')"

    limit = int(config.get("max_learning_groups", 250000))
    # Limit by groups, not raw events, to keep validation runs bounded and stable.
    sql = f"""
    with eligible_groups as (
      select e.market, e.ticker, e.timeframe, e.signal_date, e.bias
      from signal_events e
      join signal_outcomes o on o.signal_event_id=e.id
      {roster_join}
      where e.market in ({placeholders})
        and e.timeframe in ({tf_placeholders})
        and o.is_complete=1
        and o.ret_15d is not null
        and e.bias in ('bullish', 'bearish')
        {roster_filter}
      group by e.market, e.ticker, e.timeframe, e.signal_date, e.bias
      order by e.signal_date desc
      limit {limit}
    )
    select
      e.id as signal_event_id,
      e.market,
      e.ticker,
      e.timeframe,
      e.signal_date,
      e.strategy_name,
      e.bias,
      r.tier,
      r.fitness_score,
      r.quality_grade,
      r.frequency_grade,
      case
        when e.bias='bearish' then -o.ret_15d
        else o.ret_15d
      end as directional_return_15d
    from eligible_groups g
    join signal_events e
      on e.market=g.market and e.ticker=g.ticker and e.timeframe=g.timeframe
     and e.signal_date=g.signal_date and e.bias=g.bias
    join signal_outcomes o on o.signal_event_id=e.id
    {roster_join}
    where o.is_complete=1
      and o.ret_15d is not null
      {roster_filter}
    order by e.market, e.ticker, e.timeframe, e.bias, e.signal_date, e.strategy_name
    """
    return db.query(sql, params)


def load_recent_signal_rows(db: ReadOnlySQLite, config: dict[str, Any]) -> list[dict[str, Any]]:
    markets = config["markets"]
    placeholders = ",".join("?" for _ in markets)
    timeframes = config.get("timeframes") or ["1D", "1W"]
    tf_placeholders = ",".join("?" for _ in timeframes)
    params: list[Any] = [*markets, *timeframes]
    limit = int(config.get("max_live_groups", 50000))
    roster_filter = ""
    if config.get("roster_passed_only", True):
        roster_filter = "and coalesce(r.tier, '') not in ('', 'retired')"
    sql = f"""
    with latest as (
      select market, bias, max(signal_date) as signal_date
      from signal_events
      where market in ({placeholders})
        and timeframe in ({tf_placeholders})
        and bias in ('bullish', 'bearish')
      group by market, bias
    ),
    groups as (
      select e.market, e.ticker, e.timeframe, e.signal_date, e.bias
      from signal_events e
      join latest l on l.market=e.market and l.bias=e.bias
      left join signal_roster r
        on r.market=e.market and r.ticker=e.ticker and r.timeframe=e.timeframe
       and r.strategy_name=e.strategy_name and r.bias=e.bias
      where e.timeframe in ({tf_placeholders})
        and e.signal_date >= date(l.signal_date, '-5 day')
        {roster_filter}
      group by e.market, e.ticker, e.timeframe, e.signal_date, e.bias
      order by e.signal_date desc
      limit {limit}
    )
    select
      e.id as signal_event_id,
      e.market,
      e.ticker,
      e.timeframe,
      e.signal_date,
      e.strategy_name,
      e.bias,
      r.tier,
      r.fitness_score,
      r.quality_grade,
      r.frequency_grade,
      0.0 as directional_return_15d
    from groups g
    join signal_events e
      on e.market=g.market and e.ticker=g.ticker and e.timeframe=g.timeframe
     and e.signal_date=g.signal_date and e.bias=g.bias
    left join signal_roster r
      on r.market=e.market and r.ticker=e.ticker and r.timeframe=e.timeframe
     and r.strategy_name=e.strategy_name and r.bias=e.bias
    where e.timeframe in ({tf_placeholders})
      {roster_filter}
    order by e.market, e.ticker, e.timeframe, e.bias, e.signal_date, e.strategy_name
    """
    return db.query(sql, [*markets, *timeframes, *timeframes, *timeframes])
