"""Kafka reads from the runner: topic contents, dead-letter records, consumer-group lag."""
import json
import os
import time
import uuid

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from lib import Inconclusive

BOOT = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("FP_TOPIC", "fleetpulse.telemetry.v1")
DLQ = os.environ.get("FP_DLQ_TOPIC", TOPIC + ".dlq")
GROUP = os.environ.get("FP_GROUP_ID", "fleetpulse-processor")
STALL_S = 60
REQUIRED = {"schema_version", "event_id", "device_id", "epoch", "seq", "event_time", "type", "replay", "payload"}


def _consumer():
    return Consumer({"bootstrap.servers": BOOT, "group.id": "scenarios-" + uuid.uuid4().hex, "enable.auto.commit": False})


def _partitions(c, topic):
    md = c.list_topics(topic, timeout=10)
    if topic not in md.topics or md.topics[topic].error:
        raise Inconclusive(f"topic {topic} not found")
    return sorted(md.topics[topic].partitions)


def watermarks(topic=TOPIC):
    """{partition: (low, high)}"""
    c = _consumer()
    try:
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in _partitions(c, topic)}
    except KafkaException as e:
        raise Inconclusive(f"kafka unreachable: {e}")
    finally:
        c.close()


def read(topic=TOPIC, since_ms=0):
    """Yield messages from the first record with timestamp >= since_ms up to the end offsets taken now."""
    c = _consumer()
    try:
        ps = _partitions(c, topic)
        wm = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in ps}
        begin = {p: wm[p][0] for p in ps}
        if since_ms:
            for t in c.offsets_for_times([TopicPartition(topic, p, since_ms) for p in ps], timeout=10):
                begin[t.partition] = t.offset if t.offset >= 0 else wm[t.partition][1]
        todo = {p for p in ps if begin[p] < wm[p][1]}
        c.assign([TopicPartition(topic, p, begin[p]) for p in ps])
        last = time.monotonic()
        while todo:
            msgs = c.consume(num_messages=2000, timeout=1.0)
            if not msgs:
                if time.monotonic() - last > STALL_S:
                    raise Inconclusive(f"kafka read stalled on {topic}")
                continue
            last = time.monotonic()
            for m in msgs:
                if m.error():
                    raise Inconclusive(f"kafka read error: {m.error()}")
                p = m.partition()
                if m.offset() >= wm[p][1]:
                    continue
                yield m
                if m.offset() == wm[p][1] - 1:
                    todo.discard(p)
    except KafkaException as e:
        raise Inconclusive(f"kafka unreachable: {e}")
    finally:
        c.close()


def envelope(m):
    """The parsed record if it looks like a sent event (right keys and types, within the size cap), else None."""
    if m.value() is None or len(m.value()) > 1024:
        return None
    try:
        d = json.loads(m.value())
    except ValueError:
        return None
    if not isinstance(d, dict):
        return None
    ok = REQUIRED <= d.keys() and isinstance(d.get("event_id"), str) and isinstance(d.get("device_id"), str) and type(d.get("epoch")) is int and type(d.get("seq")) is int
    return d if ok else None


def events(since_ms=0, epoch=None, until_ms=None, min_seq=None):
    """(by_device, records, conflicts).

    by_device = {device: {seq: (event_id, payload, type)}} for parseable envelopes, unique by event_id.
    conflicts lists (device, seq) seen with two different event_ids. min_seq {device: n} skips seq <= n.
    """
    by, records, conflicts = {}, 0, []
    for m in read(TOPIC, since_ms):
        if until_ms is not None and m.timestamp()[1] >= until_ms:
            continue
        d = envelope(m)
        if d is None or (epoch is not None and d["epoch"] != epoch) or (min_seq and d["seq"] <= min_seq.get(d["device_id"], 0)):
            continue
        records += 1
        slot = by.setdefault(d["device_id"], {})
        prev = slot.get(d["seq"])
        if prev and prev[0] != d["event_id"]:
            conflicts.append((d["device_id"], d["seq"]))
        slot[d["seq"]] = (d["event_id"], d.get("payload"), d.get("type"))
    return by, records, conflicts


def dlq(since_ms=0):
    """[(reason, sqlstate)] for every dead-letter record since since_ms."""
    out = []
    for m in read(DLQ, since_ms):
        h = {k: (v.decode("utf-8", "replace") if v else "") for k, v in (m.headers() or [])}
        out.append((h.get("fp-dlq-reason", "?"), h.get("fp-dlq-sqlstate", "")))
    return out


def group_lag(topic=TOPIC, group=GROUP, with_end=False):
    """{partition: records behind} (and the summed high watermark if with_end).

    A partition with no committed offset counts from its low watermark.
    """
    try:
        admin = AdminClient({"bootstrap.servers": BOOT})
        res = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[group].result(timeout=15)
    except (KafkaException, TimeoutError) as e:
        raise Inconclusive(f"kafka admin unreachable: {e!r}")
    committed = {tp.partition: tp.offset for tp in res.topic_partitions if tp.topic == topic}
    wm = watermarks(topic)
    lag = {p: hi - (committed[p] if committed.get(p, -1) >= 0 else lo) for p, (lo, hi) in wm.items()}
    return (lag, sum(hi for _, hi in wm.values())) if with_end else lag


def produce(key, value, topic=TOPIC):
    """Produce one record and wait for the broker ack."""
    errors = []
    p = Producer({"bootstrap.servers": BOOT, "acks": "all", "enable.idempotence": True})
    p.produce(topic, key=key, value=value, on_delivery=lambda err, _m: errors.append(err) if err else None)
    if p.flush(15) > 0 or errors:
        raise Inconclusive(f"produce to {topic} failed: {errors or 'timeout'}")
