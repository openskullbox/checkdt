import unittest

from sqlalchemy import func, select, text

from checkdt.core.connection import Connection
from tests.common import SortTest
from tests.const import CONNECT_ARGS

sort_test = SortTest(
    [
        "test_create_conn",
        "test_fetch_conn",
        "test_get_conn",
        "test_update_conn",
        "test_list_conn",
        "test_filter_conn",
        "test_delete_conn",
    ]
)
sort_test.sort_run()


class TestConnection(unittest.TestCase):
    conn_id = None

    def test_create_conn(self):
        connection = Connection(**CONNECT_ARGS)
        connection.create_conn()
        self.__class__.conn_id = connection.conn_base.id
        self.assertTrue(type(connection.conn_base.id) == int)

    def test_fetch_conn(self):
        connection = Connection(id=self.__class__.conn_id)
        self.assertEqual(connection.conn_base.name, "warehouse")

    def test_get_conn(self):
        connection = Connection(id=self.__class__.conn_id)
        with connection.get_conn() as conn:
            cur = conn.execute(
                select([func.count()]).select_from(text("public.x_pms_crs_ids"))
            )
            res = cur.fetchone()
            self.assertEqual(res[0], 7)

    def test_update_conn(self):
        connection = Connection(id=self.__class__.conn_id)
        connection.conn_base.password = "abc123"
        connection.conn_base.name = "xyz"
        connection.update_conn(password_update=True)
        self.assertEqual(connection.conn_base.name, "xyz")

    def test_list_conn(self):
        conn_array = Connection.list_conn()
        for conn in conn_array:
            self.assertEqual(conn.name, "xyz")

    def test_filter_conn(self):
        conn_array = Connection.fetch_conn(type="redshift")
        for conn in conn_array:
            self.assertEqual(conn.name, "xyz")

    def test_delete_conn(self):
        connection = Connection(id=self.__class__.conn_id)
        deleted_id = connection.delete_conn()
        self.assertEqual(deleted_id, self.__class__.conn_id)
