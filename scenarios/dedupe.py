"""Duplicate delivery: every event applied once however often it is sent."""
from chaos import Window, event_id_sample, seconds
from lib import Checks, Inconclusive


def _dups(c: Checks, rate, secs, min_share, lag_check):
    w = Window(c)
    w.apply({"duplicateRate": rate}, secs)
    dups, acc = w.delta("injectedDups"), w.delta("accepted")
    if dups < min_share * acc:
        raise Inconclusive(f"duplicates injected {dups} < {min_share:.0%} of accepted {acc}: chaos did not take effect")
    c.add("duplicates were injected", True, f"{dups} duplicates, {acc} accepted")
    w.finish(c, lag_check)
    event_id_sample(c, w.seed)


def run(c: Checks):
    _dups(c, 0.2, seconds(300), 0.15, "nfr12")


def run_all(c: Checks):
    _dups(c, 1.0, seconds(60), 0.9, "age")
