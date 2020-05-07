from sqlalchemy import Column, Integer, String, Boolean, SmallInteger
from sqlalchemy.ext.declarative import declarative_base

from checkdt.core.session_maker import session_init

Base = declarative_base()


class TransformBase(Base):
    __tablename__ = 'cd_transform'

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    contrib = Column(Boolean)
    path = Column(String(500))
    num_streams = Column(SmallInteger)

    def __repr__(self):
        return f"<Transform(name={self.name}, tablename={self.tablename})>"


class Transform:
    def __init__(self, **kwargs):
        if 'id' in kwargs:
            with session_init() as session:
                _trans = session.query(TransformBase).filter_by(id=kwargs['id']).first()
                session.expunge(_trans)
            self.trans_base = _trans
        else:
            self.trans_base = TransformBase(**kwargs)

    def create_transform(self):
        with session_init() as session:
            session.add(self.trans_base)
            session.flush()
            session.refresh(self.trans_base)
            session.expunge(self.trans_base)
