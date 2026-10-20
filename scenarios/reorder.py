"""Out-of-order delivery: late events never move a device backwards."""
from chaos import Window, seconds
from lib import Checks, Inconclusive, db_query, sim


def _late(epoch):
    return tuple(db_query("SELECT (SELECT coalesce(sum(late_count), 0) FROM v_reconciliation WHERE epoch = %s), (SELECT count(*) FROM late_event WHERE epoch = %s)", (epoch, epoch))[0])


def run(c: Checks):
    w = Window(c)
    late0, rows0 = _late(w.epoch)
    w.apply({"reorderRate": 0.05}, seconds(300))
    if w.delta("reordered") <= 0:
        raise Inconclusive("no event was reordered: chaos did not take effect")
    w.finish(c)
    late1, rows1 = _late(w.epoch)
    w.k1 = sim("GET", "/counters")
    late, rows, re = int(late1 - late0), rows1 - rows0, w.delta("reordered")
    c.add("late events do not exceed reordered events", late <= re, f"late={late} reordered={re}")
    c.add("late_event rows equal late_count", late == rows, f"late_count={late} rows={rows}")
    if re > 1000 and late == 0:
        c.note("WARN: more than 1000 reordered and no late event: batches may be hiding reordering")
