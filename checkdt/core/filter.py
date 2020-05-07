import traceback

import sqlalchemy
from sqlalchemy import Column, Integer, BigInteger, String
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.declarative import declarative_base

from checkdt.core.connection import ConnectionBase, Connection
from checkdt.core.session_maker import session_init


Base = declarative_base()


class FilterBase(Base):
    __tablename__ = "cd_filter"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    filter_type = Column(ENUM("value", "feedback", name="filt_type"))
    value = Column(BigInteger)
    operation = Column(ENUM("max", "min", name="op_type"), nullable=True)
    field = Column(String(100))
    conn_id = Column(
        Integer, sqlalchemy.schema.ForeignKey(ConnectionBase.id), nullable=True
    )
    table = Column(String(100))

    def __repr__(self):
        return f"<Filter name={self.name}, filter_type={self.filter_type}>"


class Filter:
    def __init__(self, **kwargs):
        if "id" in kwargs:
            with session_init() as session:
                _filter = session.query(FilterBase).filter_by(id=kwargs["id"]).first()
                session.expunge(_filter)
            self.filter_base = _filter
        else:
            self.filter_base = FilterBase(**kwargs)

    def create_filter(self):
        if self.filter_base.filter_type == "feedback":
            res = all(
                hasattr(self.filter_base, x)
                for x in ["operation", "field", "conn_id", "table"]
            )
        elif hasattr(self.filter_base, "value"):
            res = True
        else:
            res = False

        if res:
            with session_init() as session:
                session.add(self.filter_base)
                session.flush()
                session.refresh(self.filter_base)
                session.expunge(self.filter_base)

    def update_filter(self):
        self.create_filter()

    def delete_filter(self):
        with session_init() as session:
            session.add(self.filter_base)
            session.delete(self.filter_base)
            session.flush()
            delete_id = self.filter_base.id
        self.filter_base = None
        return delete_id

    def get_filter_value(self):
        if self.filter_base.filter_type == "feedback":
            connection = Connection(id=self.filter_base.conn_id)
            query = select(
                [
                    getattr(func, self.filter_base.operation)(
                        text(self.filter_base.field)
                    )
                ]
            ).select_from(text(self.filter_base.table))
            with connection.get_conn() as conn:
                cur = conn.execute(query)
                return cur.fetchone()[0]
        else:
            return self.filter_base.value

    @classmethod
    def list_filter(cls):
        with session_init() as session:
            filter_array = session.query(FilterBase).all()
            for _filter in filter_array:
                session.expunge(_filter)
        return filter_array

    @classmethod
    def fetch_filter(cls, **filter_args):
        with session_init() as session:
            filter_array = session.query(FilterBase).filter_by(**filter_args)
            for _filter in filter_array:
                session.expunge(_filter)
        return filter_array
