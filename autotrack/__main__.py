import asyncio

import os
import logging
import random
import time
import uuid

import google.auth
from google.cloud import redis as gredis
from google.cloud.bigtable.data import (
    BigtableDataClientAsync,
    row_filters,
    RowMutationEntry,
    SetCell,
    ReadRowsQuery,
    TableAsync,
)
import redis

class Counter:
    redis: any = None
    scripts = {}
    log:logging.Logger = None

    # A lua script for atomic updates

    def __init__(self, log: logging.Logger):
        self.redis = redis.Redis(host="localhost", port=6379, db=0)
        self.log = log
        self.scripts['update'] = self.redis.register_script("""
            local key = KEYS[1]
            local new_state = ARGV[1]
            local old_state = ARGV[2]
            local expiry_s = tonumber(ARGV[3])
            local now = tonumber(ARGV[4])

            -- increment the new state hash
            redis.call('HINCRBY', 'state_counts', new_state, 1)

            -- if old_state, decrement that.
            if old_state and old_state ~= '' then
                redis.call('HINCRBY', 'state_counts', old_state, -1)
            end

            -- now add the key to the timestamp cleaner hash
            redis.call('ZADD', 'items', now, key)
            redis.call('EXPIRE', key, expiry_s)
            return true
        """)

        self.scripts['gc'] = self.redis.register_script("""
            local now = tonumber(ARGV[1])
            -- Collect all the items that have a score less than `now`
            local expired_items = redis.call('ZRANGEBYSCORE', 'items', '-inf', now)

            for i, key in ipairs(expired_items) do
                local state = redis.call('HGET', key, 'state')
                if state then
                    redis.call('HINCRBY', 'state_counts', state, -1)
                end
                -- now clean things up.
                redis.call('ZREM', 'items', key)
                redis.call('DEL', key)
            end
            -- And return the count of items we removed.
            return #expired_items
        """)

    def update(self, messageId, new, old=None, expiry_s=None):
        self.log.debug(f"± update {messageId} {old} → {new}")
        now = time.time()
        if expiry_s is None:
            expiry_s = now
        # lua_script([keys], [args])
        with self.redis.pipeline() as pipeline:
            pipeline.hincrby('state_counts', new, 1)
            if old is not None:
                pipeline.hincrby('state_counts', old, -1)
            pipeline.zadd('items', {messageId:now})
            pipeline.expire(messageId, expiry_s)
            pipeline.execute()
        # self.scripts["update"]([messageId], [new, old, expiry_s, now])

    def gc(self):
        """Remove all the exipred junk, decrement the counters."""
        purged = self.scripts['gc']([],[time.time()])
        self.log.debug(f"± Cleaned {purged} item(s)")
        return purged

    def counts(self, keys=[]):
        return {keys: item for keys, item in zip(keys, map( lambda x: int(x), self.redis.hmget('state_counts',keys)))}

class Session:
    project: str
    instance: str
    table_name: str
    credentials: any = None
    table: any = None
    log: logging.Logger = None
    milestones: any = {}
    counter: Counter = None

    def __init__(self, log):
        self.instance = os.environ.get("INSTANCE", "autopush-dev")
        self.table_name = os.environ.get("TABLE", "tracking")
        self.credentials, project = google.auth.default()
        self.project = project or os.environ.get("PROJECT", "jrconlin-push-dev")
        self.log = log
        self.milestones ={
            "rcvd": {
                "success": 100,  # likelihood of failure
                "next": ["stor", "trns"],
                "delay": 0,  # ms to sit on this.
            },
            "stor": {"success": 90, "next": ["retr"], "delay": 100},
            "retr": {"success": 60, "next": ["trns"], "delay": 10},
            "trns": {"success": 90, "next": ["accp"], "delay": 100},
            "accp": {"success": 80, "next": ["delv"], "delay": 1},
            "delv": {
                "success": 100,
                "next": [],  # There are no next steps, so bail.
                "delay": 0,
            },
        }
        self.counter = Counter(self.log)


def init_logs():
    level = getattr(logging, os.environ.get("PYTHON_LOG", "INFO").upper(), None)
    logging.basicConfig(level=level)
    log = logging.getLogger("autotrack")
    return log


"""Note: Bigtable stores everything internally as a byte array. Oddly,
Bigquery can read the qualifiers as string for some queries.

for example (yeah, this is does a table scan, but illustrates how to find
a familyName[qualifier])

```
SELECT _key, state['exit']
FROM `tracking`(WITH_HISTORY=>FALSE)
WHERE state['exit'] is null
ORDER by _key
LIMIT 10
```

Bigquery does NOT allow aggregation, so you can't do things like max(_key),
or max(state). I suppose one could do a search for all rows that have
a given state, but setting the time range could be tricky, since the key
starts with a random set of bytes. (e.g. how do you specify you want
"0000#aaa#foo" to "8888#bbb#foo" and not include "4444#aaa#gorp"?)

"""

async def store_by_state_mid(
    messageId: str,
    expiry_ns: int,
    state: str,
    previous: str,
    session: Session,
    table:TableAsync,
):
    """This approach will at least allow us to gather up the messages that
    are in given milestone states. We can filter them by looking at ones
    that do not have a state['exit'].
    I'm debating if it's worth adding a ts to the key to allow for sliding
    window queries or if we could just add a second filter looking at the
    row's timestamp.
    """
    session.log.debug(f"Updating {messageId} -> {state}")
    async with table.mutations_batcher() as batcher:
        mut_list = [
                    SetCell(family="ttl", qualifier="ttl", new_value=int(expiry_ns)),
                    SetCell(family="state", qualifier=state, new_value=int(time.time_ns())),
                ]
        await batcher.append(
            RowMutationEntry (
                messageId,
                mut_list,
            )
        )
    # need to convert the expiry to seconds for redis.
    expiry_s = int(int(expiry_ns*1e-9) - time.time())
    session.log.debug(f"expire in {expiry_s}")
    session.counter.update(messageId, state, previous, expiry_s)
    return messageId

async def store_by_mid_date_state(
    messageId: str,
    expiry_ns: int,
    state: str,
    previous: str,
    session: Session,
    table:TableAsync,
):
    """Store by the messageId#time#state, adding entry and exit fields.

    This has promise, but is proving to be less good for things like
    "return all items at a given state" or "return items that are between
    these times.

    """
    session.log.debug(f"Updating {messageId} -> {state}")
    key = f"{messageId}#{time.time_ns()}#{state}"
    async with table.mutations_batcher() as batcher:
        if previous:
            await batcher.append(
                RowMutationEntry(
                    previous,
                    [SetCell(family="state", qualifier="exit", new_value=time.time_ns())]
                )
            )
        # Ideally, the "prev_state" would be the UAID+Timestamp of the prior state.
        mut_list = [
                    SetCell(family="ttl", qualifier="ttl", new_value=expiry_ns),
                    SetCell(family="state", qualifier="entry", new_value=time.time_ns()),
                ]
        await batcher.append(
            RowMutationEntry (
                key,
                mut_list,
            )
        )
    return key


async def query_milestones(
        session: Session,
):
    print(session.counter.counts(session.milestones.keys()))

async def process_message(
    messageId: str, expiry_ns: int, session: Session, table
):
    # get the first state out of the milestones dict.
    state_label = next(iter(session.milestones.keys()))
    previous = None
    # really dumb state machine:
    while True:
        state = session.milestones.get(state_label)
        if random.randint(0, 100) > state.get("success"):
            session.log.warning(f"🚫 Faking an error for {messageId} at {state_label}")
            return
        previous = await store_by_state_mid(
            messageId=messageId,
            expiry_ns=expiry_ns,
            state=state_label,
            previous=previous,
            session=session,
            table=table
        )
        time.sleep(0.001 * state.get("delay", 0))
        now = time.time_ns()
        if now > expiry_ns:
            session.log.warning(f"🪦 {messageId} has expired at {state_label}")
            return
        next_states = state.get("next")
        if next_states == []:
            session.log.info(f"🎉 {messageId} completed successfully")
            return
        state_label = random.choice(next_states)


# purge with `cbt deleteallrows $TABLE`
async def fill(session: Session, count: int):
    async with BigtableDataClientAsync(project=session.project) as client:
        async with client.get_table(session.instance, session.table_name) as table:
            session.log.info("Filling table...")
            for i in range(0, count):
                messageId = uuid.uuid4().hex
                # time is limited to seconds?
                expiry_ns = time.time_ns() + (random.randint(0, 10) * 1e+9)
                await process_message(messageId, expiry_ns, session, table)


async def amain (log: logging.Logger):
    session = Session(log)
    start = time.time()
    print(session.counter.gc())
    log.debug(f"GC time {time.time() - start}")
    await fill(session, 10)
    start = time.time()
    print(session.counter.gc())
    log.debug(f"GC time {time.time() - start}")
    await query_milestones(session)
    for i in range(1,100):
        time.sleep(5)
        print(session.counter.gc())
        await query_milestones(session)


def main():
    log = init_logs()
    log.info("Starting up...")
    asyncio.run(amain(log))

    # TODO: Add a timer to run the session.counter.gc() function to clean up the counter table.


if __name__ == "__main__":
    main()
