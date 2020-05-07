from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from checkdt.config.config import CORE__SQLALCHEMY_CONN


@contextmanager
def session_init():
    engine = create_engine(CORE__SQLALCHEMY_CONN)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
