#!/usr/bin/env python
import asyncio
import logging
from typing import (
    Any,
    List,
    Callable,
    Optional,
)

import hummingbot
from hummingbot.logger import HummingbotLogger
from hummingbot.notifier.notifier_base import NotifierBase
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.utils.async_utils import safe_ensure_future

from sqlalchemy.orm import (
    Session,
    Query
)
from hummingbot.model.data_to_db import DataToDb
import time

class StatusToDbNotifier(NotifierBase):
    tn_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls.tn_logger is None:
            cls.tn_logger = logging.getLogger(__name__)
        return cls.tn_logger

    def __init__(self,
                 hb: "hummingbot.client.hummingbot_application.HummingbotApplication") -> None:
        super().__init__()
        self._hb = hb
        self._ev_loop = asyncio.get_event_loop()
        self._async_call_scheduler = AsyncCallScheduler.shared_instance()
        self._msg_queue: asyncio.Queue = asyncio.Queue()
        self._send_msg_task: Optional[asyncio.Task] = None

    def start(self):
        if not self._started:
            self._started = True
            self._send_msg_task = safe_ensure_future(self.send_msg_from_queue(), loop=self._ev_loop)
            self.logger().info("Custom API is listening...")
            safe_ensure_future(self.send_command(), loop=self._ev_loop)

    def stop(self) -> None:
        if self._send_msg_task:
            self._send_msg_task.cancel()

    async def send_command(self):
        async_scheduler: AsyncCallScheduler = AsyncCallScheduler.shared_instance()
        while True:
            # status = await self._hb.strategy_status()
            # print(status)
            await async_scheduler.call_async(self._hb._handle_command, 'status_to_db')
            await asyncio.sleep(10)

    @staticmethod
    def _divide_chunks(arr: List[Any], n: int = 5):
        """ Break a list into chunks of size N """
        for i in range(0, len(arr), n):
            yield arr[i:i + n]

    def add_msg_to_queue(self, msg: str):
        lines: List[str] = msg.split("\n")
        msg_chunks: List[List[str]] = self._divide_chunks(lines, 30)
        for chunk in msg_chunks:
            self._msg_queue.put_nowait("\n".join(chunk))

    def add_msg_to_db(self, msg: str):
        session: Session = self._hb.trade_fill_db.get_shared_session()
        timestamp = int(time.time() * 1e3)
        query: Query = (session
                        .query(DataToDb)
                        .filter(DataToDb.status != '',
                                ))
        data_to_db: Optional[DataToDb] = query.one_or_none()
        if data_to_db is not None:
            data_to_db.status = msg
            data_to_db.timestamp = timestamp
        else:
            data_to_db = DataToDb(status=msg,
                                  timestamp=timestamp, )
            session.add(data_to_db)
        session.commit()

    async def send_msg_from_queue(self):
        while True:
            try:
                new_msg: str = await self._msg_queue.get()
                if isinstance(new_msg, str) and len(new_msg) > 0:
                    await self.send_msg_async(new_msg)
            except Exception as e:
                self.logger().error(str(e))
            await asyncio.sleep(1)

    async def send_msg_async(self, msg: str) -> None:
        """
        Send given markdown message
        """