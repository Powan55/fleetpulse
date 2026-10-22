"""Duplicate delivery: every event applied once however often it is sent."""
import m3
from chaos import Window, event_id_sample, seconds
from lib import Checks, Inconclusive, sim


def _dups(c: Checks, rate, secs, min_share, lag_check, flap=False):
    w = Window(c)
    if flap:
        c.note(f"flap fault (3 bad, 5 good samples) on {len(m3.flap_100())} devices")
    w.apply({"duplicateRate": rate}, secs)
    dups, acc = w.delta("injectedDups"), w.delta("accepted")
    if dups < min_share * acc:
        raise Inconclusive(f"duplicates injected {dups} < {min_share:.0%} of accepted {acc}: chaos did not take effect")
    c.add("duplicates were injected", True, f"{dups} duplicates, {acc} accepted")
    sim("DELETE", "/faults")
    w.finish(c, lag_check)
    event_id_sample(c, w.seed)


def run(c: Checks):
    _dups(c, 0.2, seconds(300), 0.15, "nfr12", flap=True)


def run_all(c: Checks):
    _dups(c, 1.0, seconds(60), 0.9, "age")
