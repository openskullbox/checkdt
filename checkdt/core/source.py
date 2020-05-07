import pandas as pd
from sqlalchemy import Column, Integer, String
from sqlalchemy import orm, schema
from sqlalchemy import select, text, literal_column
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.declarative import declarative_base

from checkdt.core.connection import ConnectionBase, Connection
from checkdt.core.filter import FilterBase, Filter
from checkdt.core.session_maker import session_init

Base = declarative_base()

OP_MAPPING = {
    "ge": ">=",
    "gt": ">",
    "lt": "<",
    "le": "<=",
    "eq": "==",
    "ne": "!=",
    "not": "!",
    "is": "is",
}


class SourceBase(Base):
    __tablename__ = "cd_source"

    id = Column(Integer, primary_key=True)
    conn_id = Column(Integer, schema.ForeignKey(ConnectionBase.id))
    name = Column(String(100))
    tablename = Column(String(100))

    field_list = orm.relationship(
        "SourceFieldBase", back_populates="source", lazy="joined", cascade="all, delete"
    )

    def __repr__(self):
        return f"<Source(name={self.name}, tablename={self.tablename})>"


class SourceFieldBase(Base):
    __tablename__ = "cd_source_field"

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, schema.ForeignKey(SourceBase.id))
    fieldname = Column(String(100))
    length = Column(Integer)
    type = Column(
        ENUM(
            "integer",
            "smallint",
            "bigint",
            "string",
            "bool",
            "array",
            "document",
            "float",
            "numeric",
            "datetime",
            "date",
            name="field_type",
        )
    )
    association = Column(Integer, schema.ForeignKey("cd_source_field.id"))
    filter_id = Column(Integer, schema.ForeignKey(FilterBase.id), nullable=True)
    op = Column(
        ENUM("ge", "gt", "lt", "le", "eq", "ne", "not", "is", name="operation_type")
    )

    source = orm.relationship("SourceBase", back_populates="field_list")

    def __repr__(self):
        return f"<SourceField(source_id={self.source_id}, fieldname={self.fieldname})>"


class Source:
    def __init__(self, **kwargs):
        if "id" in kwargs:
            with session_init() as session:
                _source = session.query(SourceBase).filter_by(id=kwargs["id"]).first()
                session.expunge(_source)
            self.source_base = _source
        else:
            source_args = {key: kwargs[key] for key in ["conn_id", "name", "tablename"]}
            self.source_base = SourceBase(**source_args)
            for field in kwargs["field_list"]:
                self.source_base.field_list.append(SourceFieldBase(**field))

    def create_source(self):
        with session_init() as session:
            session.add(self.source_base)
            session.flush()
            session.refresh(self.source_base)
            session.expunge(self.source_base)

    def add_field(self, field_dict):
        field = SourceFieldBase(**field_dict)
        self.source_base.field_list.append(field)

    def update_source(self):
        self.create_source()

    def delete_source(self):
        with session_init() as session:
            session.add(self.source_base)
            session.delete(self.source_base)
            session.flush()
            delete_id = self.source_base.id
        self.source_base = None
        return delete_id

    def get_source_data(self):
        filter_conditions = []
        fields = []
        for field in self.source_base.field_list:
            fields.append(literal_column(field.fieldname))
            if field.filter_id is not None:
                _filter = Filter(id=field.filter_id)
                val = _filter.get_filter_value()
                filter_conditions.append(
                    field.fieldname + OP_MAPPING[field.op] + str(val)
                )

        if len(filter_conditions) > 0:
            where_clause = text(" AND ".join(filter_conditions))
            query = select(
                fields, whereclause=where_clause
            ).select_from(text(self.source_base.tablename))
        else:
            query = select(fields).select_from(
                text(self.source_base.tablename)
            )

        _conn = Connection(id=self.source_base.conn_id)
        with _conn.get_conn() as conn:
            cur = conn.execute(query)
            df = pd.DataFrame(cur.fetchall())
            df.columns = cur.keys()
            return df
