import asyncio

import os
import logging
import random
import time
import uuid

import google.auth
from google.cloud import bigtable
from google.cloud.bigtable.data import (
    BigtableDataClientAsync,
    row_filters,
    RowMutationEntry,
    SetCell,
    ReadRowsQuery,
    TableAsync,
)


class Session:
    project: str
    instance: str
    table_name: str
    credentials: any
    table: any
    log: any
    milestones: any

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
    message_id: str,
    expiry: int,
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
    session.log.debug(f"Updating {message_id} -> {state}")
    key = f"{state}#{message_id}"
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
                    SetCell(family="ttl", qualifier="ttl", new_value=expiry),
                    SetCell(family="state", qualifier="entry", new_value=time.time_ns()),
                ]
        await batcher.append(
            RowMutationEntry (
                key,
                mut_list,
            )
        )
    return key

async def store_by_mid_date_state(
    message_id: str,
    expiry: int,
    state: str,
    previous: str,
    session: Session,
    table:TableAsync,
):
    """Store by the message_id#time#state, adding entry and exit fields.

    This has promise, but is proving to be less good for things like
    "return all items at a given state" or "return items that are between
    these times.

    """
    session.log.debug(f"Updating {message_id} -> {state}")
    key = f"{message_id}#{time.time_ns()}#{state}"
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
                    SetCell(family="ttl", qualifier="ttl", new_value=expiry),
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
        table:TableAsync,
):
    """ Generate a query to return the count of items at various milestones
        within given times.
    """
    counts = {}
    for key in session.milestones.keys():
        if key == 'delv':
            continue
        filter = row_filters.RowFilterChain(
            filters=[
                row_filters.RowKeyRegexFilter(f"^{key}#.*"),
                row_filters.ConditionalRowFilter(
                    true_filter=row_filters.BlockAllFilter(True),
                    false_filter=row_filters.PassAllFilter(True),
                    predicate_filter=row_filters.ColumnQualifierRegexFilter('exit')
                )
            ]
        )

        count =0
        session.log.debug(f"➕ Counting {key}")
        async for row in await table.read_rows_stream(ReadRowsQuery(row_filter=filter)):
            # ttl = int.from_bytes(row.get_cells(family='ttl', qualifier='ttl').pop().value)
            count += 1
        counts[key] = count

    return counts


async def process_message(
    message_id: str, expiry: int, session: Session, table
):
    # get the first state out of the milestones dict.
    state_label = next(iter(session.milestones.keys()))
    previous = None
    # really dumb state machine:
    while True:
        state = session.milestones.get(state_label)
        if random.randint(0, 100) > state.get("success"):
            session.log.warning(f"🚫 Faking an error for {message_id} at {state_label}")
            return
        previous = await store_by_state_mid(
            message_id=message_id,
            expiry=expiry,
            state=state_label,
            previous=previous,
            session=session,
            table=table
        )
        time.sleep(0.001 * state.get("delay", 0))
        now = time.time_ns()
        if now > expiry:
            session.log.warning(f"🪦 {message_id} has expired at {state_label}")
            return
        next_states = state.get("next")
        if next_states == []:
            session.log.info(f"🎉 {message_id} completed successfully")
            return
        state_label = random.choice(next_states)


# purge with `cbt deleteallrows $TABLE`
async def fill(log: logging.Logger, count: int):
    session = Session(log)
    async with BigtableDataClientAsync(project=session.project) as client:
        async with client.get_table(session.instance, session.table_name) as table:
            """
            log.info("Filling table...")
            for i in range(0, count):
                message_id = uuid.uuid4().hex
                expiry = time.time_ns() + (random.randint(0, 10) * 100000000)
                await process_message(message_id, expiry, session, table)
            """
            print(await query_milestones(session, table))


def main():
    log = init_logs()
    log.info("Starting up...")
    asyncio.run(fill(log, 10))


if __name__ == "__main__":
    main()
