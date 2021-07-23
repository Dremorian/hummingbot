#!/usr/bin/env python

from sqlalchemy import (
    Column,
    ForeignKey,
    Text,
    Integer,
    Index,
    BigInteger,
    Float,
    JSON
)

from . import HummingbotBase


class HistoryDb(HummingbotBase):
    __tablename__ = "HistoryDb"

    id = Column(Integer, primary_key=True, nullable=False)
    config_file_path = Column(Text, nullable=False)
    strategy = Column(Text, nullable=False)
    history = Column(Text, nullable=False)
    timestamp = Column(BigInteger, nullable=False)

    def __repr__(self) -> str:
        return f"HistoryDb(id='{self.id}', config_file_path='{self.config_file_path}', strategy='{self.strategy}', " \
            f"history='{self.history}', timestamp={self.timestamp})"