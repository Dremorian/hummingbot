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
from hummingbot.core.utils.async_utils import safe_ensure_future

class HistoryToDbNotifier(NotifierBase):
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
        self._send_command_task: Optional[asyncio.Task] = None

    def start(self):
        if not self._started:
            self._started = True
            self.logger().info("History to DB Started...")
            self._send_command_task = safe_ensure_future(self.send_command(), loop=self._ev_loop)

    def stop(self) -> None:
        if self._send_command_task:
            self._send_command_task.cancel()

    async def send_command(self):
        while True:
            self._hb.history_to_db(1)
            await asyncio.sleep(3600)

    def add_msg_to_queue(self, msg: str) -> None:
        """
        Nothing
        """

    def add_msg_to_db(self, msg: str) -> None:
        """
        Nothing
        """