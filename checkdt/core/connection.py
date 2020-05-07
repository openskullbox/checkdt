from cryptography.fernet import Fernet
import sqlalchemy
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.declarative import declarative_base

from checkdt.config.config import CORE__ENCRYPTION_KEY
from checkdt.core.session_maker import session_init


Base = declarative_base()


class ConnectionBase(Base):
    __tablename__ = "cd_connection"

    id = Column(Integer, primary_key=True)
    type = Column(ENUM("redshift", "mysql", "mongo", "redis", name="database_type"))
    name = Column(String(100))
    host = Column(String(500))
    username = Column(String(100))
    password = Column(String(500))
    database = Column(String(100))
    port = Column(Integer)

    def __repr__(self):
        return f"<Connection(name={self.name}, host={self.host})>"


class Connection:
    def __init__(self, **kwargs):
        if "id" in kwargs:
            with session_init() as session:
                _connection = (
                    session.query(ConnectionBase).filter_by(id=kwargs["id"]).first()
                )
                session.expunge(_connection)
            self.conn_base = _connection
        elif all(x in kwargs for x in ("name", "host")):
            self.conn_base = ConnectionBase(**kwargs)
            self.conn_base.password = self.__encode_passwd(
                self.conn_base.password
            ).decode("ascii")

    @staticmethod
    def __encode_passwd(password):
        f = Fernet(CORE__ENCRYPTION_KEY)
        return f.encrypt(password.encode("ascii"))

    @staticmethod
    def __decode_passwd(password):
        f = Fernet(CORE__ENCRYPTION_KEY)
        return f.decrypt(password.encode("ascii"))

    def create_conn(self):
        with session_init() as session:
            session.add(self.conn_base)
            session.flush()
            session.refresh(self.conn_base)
            session.expunge(self.conn_base)

    def update_conn(self, password_update=False):
        if password_update:
            self.conn_base.password = self.__encode_passwd(
                self.conn_base.password
            ).decode("ascii")
        self.create_conn()

    def delete_conn(self):
        with session_init() as session:
            session.add(self.conn_base)
            session.delete(self.conn_base)
            session.flush()
            delete_id = self.conn_base.id
        self.conn_base = None
        return delete_id

    def get_conn(self):
        passwd = self.__decode_passwd(self.conn_base.password).decode("ascii")
        if self.conn_base.type == "redshift":
            conn_params = {
                "host": self.conn_base.host,
                "user": self.conn_base.username,
                "password": passwd,
                "database": self.conn_base.database,
                "port": self.conn_base.port,
            }
            return RedshiftConnection(**conn_params)

    @classmethod
    def list_conn(cls):
        with session_init() as session:
            connection_array = session.query(ConnectionBase).all()
            for conn in connection_array:
                session.expunge(conn)
        return connection_array

    @classmethod
    def fetch_conn(cls, **filter_args):
        with session_init() as session:
            connection_array = session.query(ConnectionBase).filter_by(**filter_args)
            for conn in connection_array:
                session.expunge(conn)
        return connection_array


class RedshiftConnection:
    def __init__(self, **conn_args):
        user = conn_args["user"]
        password = conn_args["password"]
        host = conn_args["host"]
        port = conn_args["port"]
        database = conn_args["database"]

        conn_string = (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )
        engine = sqlalchemy.create_engine(conn_string)
        self.session = sqlalchemy.orm.sessionmaker(bind=engine)()

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, value, traceback):
        self.session.close()
