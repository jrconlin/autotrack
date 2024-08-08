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
    MutationsBatcherAsync,
    SetCell,
    ReadRowsQuery,
)


class Session:
    project: str
    instance: str
    table_name: str
    credentials: any
    table: any

    def __init__(self):
        self.instance = os.environ.get("INSTANCE", "autopush-dev")
        self.table_name = os.environ.get("TABLE", "tracking")
        self.credentials, project = google.auth.default()
        self.project = project or os.environ.get("PROJECT", "jrconlin-push-dev")


def init_logs():
    level = getattr(logging, os.environ.get("PYTHON_LOG", "ERROR").upper(), None)
    logging.basicConfig(level=level)
    log = logging.getLogger("autotrack")
    return log


async def update_database(
    base_key: str,
    expiry: int,
    state: str,
    prev_state: any,
    session: Session,
    client,
    table,
):
    async with table.mutations_batcher() as batcher:
        if prev_state:
            await batcher.append(
                RowMutationEntry(
                    f"{base_key}#{prev_state}",
                    [SetCell("state", "exit", time.time_ns())],
                )
            )
        await batcher.append(
            RowMutationEntry(
                f"{base_key}#{state}",
                [
                    SetCell("ttl", "expiry", expiry),
                    SetCell("state", "entry", time.time_ns()),
                ],
            )
        )
    return


async def process_message(
    key: str, expiry: int, session: Session, log: logging.Logger, client, table
):
    milestones = {
        "received": {
            "success": 100,  # likelihood of failure
            "next": ["stored", "transmitted"],
            "delay": 0,  # ms to sit on this.
        },
        "stored": {"success": 80, "next": ["retrieved"], "delay": 100},
        "retrieved": {"success": 100, "next": ["transmitted"], "delay": 10},
        "transmitted": {"success": 90, "next": ["accepted"], "delay": 100},
        "accepted": {"success": 80, "next": ["delivered"], "delay": 1},
        "delivered": {
            "success": 100,
            "next": [],  # There are no next steps, so bail.
            "delay": 0,
        },
    }
    state_label = "received"
    prev_state = None
    # really dumb state machine:
    while True:
        state = milestones.get(state_label)
        if random.randint(0, 100) > state.get("success"):
            log.warning(f"🚫 Faking an error for {key} at {state_label}")
            return
        await update_database(
            key, expiry, state_label, prev_state, session, client, table
        )
        time.sleep(0.001 * state.get("delay", 0))
        now = time.time_ns()
        if now > expiry:
            log.warning(f"🪦 {key} has expired at {state}")
            return
        next_states = state.get("next")
        if next_states == []:
            log.info(f"🎉 {key} completed successfully")
            return
        state_label = random.choice(next_states)


# purge with `cbt deleteallrows $TABLE`
async def fill(log: logging.Logger, count: int):
    session = Session()
    async with BigtableDataClientAsync(project=session.project) as client:
        async with client.get_table(session.instance, session.table_name) as table:
            log.info("Filling table...")
            for i in range(0, count):
                uaid = uuid.uuid4().hex
                key = f"{uaid}#{(time.time_ns())}"
                expiry = time.time_ns() + (random.randint(0, 10) * 100000000)
                await process_message(key, expiry, session, log, client, table)


def main():
    log = init_logs()
    log.info("Starting up...")
    asyncio.run(fill(log, 1000))


if __name__ == "__main__":
    main()
