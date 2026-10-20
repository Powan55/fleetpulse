"""Device-side loss: dropped events are counted apart and never enter the ledger."""
from chaos import Window, seconds
from lib import Checks, Inconclusive


def run(c: Checks):
    w = Window(c)
    w.apply({"dropRate": 0.1}, seconds(300))
    dropped, gen = w.delta("dropped"), w.delta("generated")
    if dropped < 0.07 * gen:
        raise Inconclusive(f"dropped {dropped} < 7% of generated {gen}: chaos did not take effect")
    c.add("events were dropped", True, f"{dropped} dropped of {gen} generated")
    w.finish(c)
