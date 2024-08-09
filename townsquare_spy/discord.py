"""
This module implements an extension and cog which extend a bot with
functionality related to the townsquare spy.

It monitors specific channels for mentions of townsquare URLs, then
begins monitoring them and logging what happens to an sqlite database.
While this database could be examined manually, commands to query it
are also provided.

See __main__.py for a much simpler usage of the spy functionality.
"""

import asyncio
import nextcord
import os
import re
import sqlite3

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from io import BytesIO, TextIOWrapper
from nextcord.ext import commands
from typing import Optional

from .spy import random_player_id, interpret_url, connect_to_session, receive, Session

class DatabaseThread(object):
    """
    To ensure the main thread (and thus the bot) remain responsive even if the
    disk is busy/slow, we set aside a thread and do all database access there.
    One thread suffices, and this avoids the need to do additional synchronization
    if there were multiple.
    """
    conn: Optional[sqlite3.Connection]
    executor: ThreadPoolExecutor

    def __init__(self, path: str):
        self.conn = None
        self.executor = ThreadPoolExecutor(1, "Townsquare Spy Database", self.connect, (path,))

    def connect(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.executescript("""
            BEGIN;
            CREATE TABLE IF NOT EXISTS session_log(
                url VARCHAR(255),
                session_start TIMESTAMP,
                timestamp TIMESTAMP,
                message TEXT,
                state TEXT
            );
            CREATE INDEX IF NOT EXISTS session_log_index
            ON session_log(url, session_start, timestamp);
            COMMIT;
        """)

    def log(self, messages: list[dict]) -> asyncio.Future:
        def write_on_thread():
            self.conn.executemany(
                """
                    INSERT INTO session_log
                    (url, session_start, timestamp, message, state)
                    VALUES (:url, :session_start, :timestamp, :message, :state);
                """,
                messages)
            self.conn.commit()
        return asyncio.get_running_loop().run_in_executor(
            self.executor, write_on_thread)
    
    def latest(self, url: str) -> asyncio.Future:
        def read_on_thread():
            cur = self.conn.execute(
                """
                    SELECT timestamp, message
                    FROM session_log
                    WHERE
                        url = :url AND
                        session_start = (SELECT MAX(session_start) FROM session_log WHERE url = :url)
                """,
                dict(url=url))
            return cur.fetchall()
        return asyncio.get_running_loop().run_in_executor(
            self.executor, read_on_thread)

async def monitor_session(url, db_thread: DatabaseThread):
    """
    Monitors a session. Expected to be run as a task.
    Dispatches database access to a thread pool, but attempts to
    cancel it if the task is itself cancelled.
    """
    session_start = datetime.now(timezone.utc)
    player_id = random_player_id()
    socket_url, app_origin = interpret_url(url, player_id)
    session = Session()

    messages = []
    session.log = lambda message: messages.append(dict(
        url=url,
        session_start=session_start,
        timestamp=datetime.now(timezone.utc),
        message=message,
        state="dummy state"))

    socket = connect_to_session(socket_url, origin=app_origin, player_id=player_id)
    async for m in socket:
        receive(session, m)
        if not messages: continue

        # If there are now messages to log, do so. If this task is cancelled
        # while waiting for that to finish, attempt to cancel it.
        write_future = db_thread.log(messages)
        try:
            await write_future
        except asyncio.CancelledError:
            write_future.cancel()
            raise
        messages.clear()

class TownsquareSpyCog(commands.Cog):
    bot: commands.Bot
    db_thread: DatabaseThread
    session_tasks: dict[str, asyncio.Task]
    watched_channels: set[int]
    watch_re: re.Pattern

    def __init__(self, bot: commands.Bot, db_path: str):
        self.bot = bot
        self.db_thread = DatabaseThread(db_path)
        self.session_tasks = dict()
        self.watched_channels = set(int(c) for c in os.environ["TOWNSQUARE_SPY_CHANNELS"].split(","))
        self.watch_re = re.compile(r'\bhttps?://clocktower\.(?:online|live)/#[A-Za-z0-9-_]+\b')

    def cog_unload(self):
        """
        Called if this cog is being unloaded.
        This cancels all ongoing monitoring.
        """
        for task in self.session_tasks:
            task.cancel()

    @commands.Cog.listener()
    async def on_message(self, message: nextcord.Message):
        """
        Observes messages in text channels.
        If a message mentioning a townsquare link is found, it is monitored.
        """
        if message.channel.id not in self.watched_channels: return
        for url in self.watch_re.findall(message.content):
            session_task = asyncio.create_task(
                monitor_session(url, db_thread=self.db_thread))
            self.session_tasks[url] = session_task
            def discard(task):
                if self.session_tasks.get(url) == task:
                    del self.session_tasks[url]
            session_task.add_done_callback(discard)

    @nextcord.slash_command()
    async def spyshowlog(self, interaction: nextcord.Interaction, session_url: str):
        latest = await self.db_thread.latest(session_url)
        if not latest:
            await interaction.send("No session log found.")
        else:
            data = BytesIO()
            text = TextIOWrapper(data, encoding="utf-8", newline="\n")
            for timestamp, message in latest:
                print(f"[{timestamp}] {strip_ansi(message)}", file=text)
            text.flush()
            data.seek(0)
            with nextcord.File(data, filename="session.txt", description="Session Log") as f:
                await interaction.send(file=f)

def strip_ansi(s):
    return re.sub(r'\x1b\[[\x30-\x3f]*[\x20-\x2f]*[\x40-\x7e]', '', s)

def setup(bot, db_path=":memory:"):
    """
    Invoked as part of extension loading:
    https://docs.nextcord.dev/en/stable/ext/commands/extensions.html
    """
    bot.add_cog(TownsquareSpyCog(bot, db_path))