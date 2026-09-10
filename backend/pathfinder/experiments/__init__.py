"""
Pathfinder Session S2 — THE EXPERIMENT LOOP.

    Observe -> Ask -> Research -> Backtest -> Decide if worth testing -> Deploy virtual capital
    -> Track forward -> Compare expected vs actual -> Learn -> Change the experiment -> Next

Built on top of S1's research engine (`pathfinder.research`): an S1 finding is the OBSERVATION
and its founder-authored follow-up question is the ASK; `hypotheses.py` is the closed set of
conditioned variants the engine may RESEARCH; `gate.py` is the DECISION (the Constitution's
gauntlet + the NDP promote-only-if-it-holds check); `book.py` DEPLOYS virtual capital and
TRACKS it deterministically; `grading.py` COMPARES expected vs actual under a rule frozen
before the period opened; `learning.py` LEARNS from a failure with a counted number of trials
and proposes the next version or buries the idea; `store.py` is the append-only, hash-chained
registry; `narrate.py` tells the seven-line story from computed facts only.

Governing sentence (docs/sessions/PATHFINDER.md): the engine computes every number; the model
never produces one.
"""
