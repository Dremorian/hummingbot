#!/usr/bin/env python

from sqlalchemy import (
    Column,
    Text,
    JSON,
    Integer,
    BigInteger,
    Index
)

from . import HummingbotBase


class DataToDb(HummingbotBase):
    __tablename__ = "Data"
    __table_args = (Index("data_time_index",
                          "timestamp"))

    id = Column(Integer, primary_key=True, nullable=False)
    status = Column(Text, nullable=False)
    timestamp = Column(BigInteger, nullable=False)

    def __repr__(self) -> str:
        return f"Data(id='{self.id}', status='{self.status}', " \
            f"timestamp={self.timestamp}"
