from sqlalchemy import create_engine

from checkdt.config.config import CORE__SQLALCHEMY_CONN
from checkdt.core.connection import Base as BaseCdConnection
from checkdt.core.filter import Base as BaseCdFilter
from checkdt.core.source import Base as BaseCdSource

engine = create_engine(CORE__SQLALCHEMY_CONN)

# BaseCdConnection.metadata.create_all(engine)
# BaseCdFilter.metadata.create_all(engine)
BaseCdSource.metadata.create_all(engine)