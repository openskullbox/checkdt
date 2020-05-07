import unittest

from checkdt.core.connection import Connection
from checkdt.core.filter import Filter
from tests.common import SortTest
from tests.const import CONNECT_ARGS, FILTER_ARGS, FILTER_VAL

sort_test = SortTest(
    [
        "test_create_filter",
        "test_fetch_filter",
        "test_get_filter_value",
        "test_update_filter",
        "test_list_filter",
        "test_filter_filter",
        "test_delete_filter",
    ]
)
sort_test.sort_run()


class TestFilter(unittest.TestCase):
    conn_id = None
    filter_id = None

    def test_create_filter(self):
        connection = Connection(**CONNECT_ARGS)
        connection.create_conn()
        self.__class__.conn_id = connection.conn_base.id
        FILTER_ARGS['conn_id'] = self.__class__.conn_id

        db_filter = Filter(**FILTER_ARGS)
        db_filter.create_filter()
        self.__class__.filter_id = db_filter.filter_base.id
        self.assertTrue(type(db_filter.filter_base.id) == int)

    def test_fetch_filter(self):
        db_filter = Filter(id=self.__class__.filter_id)
        self.assertEqual(db_filter.filter_base.name, FILTER_ARGS["name"])

    def test_get_filter_value(self):
        db_filter = Filter(id=self.__class__.filter_id)
        val = db_filter.get_filter_value()
        self.assertEqual(val, FILTER_VAL)

    def test_update_filter(self):
        db_filter = Filter(id=self.__class__.filter_id)
        db_filter.filter_base.name = "static_filter"
        db_filter.filter_base.filter_type = "value"
        db_filter.filter_base.value = 100
        db_filter.update_filter()
        self.assertEqual(db_filter.filter_base.name, "static_filter")
        self.assertEqual(db_filter.get_filter_value(), 100)

    def test_list_filter(self):
        filters = Filter.list_filter()
        for filt in filters:
            self.assertEqual(filt.name, "static_filter")

    def test_filter_filter(self):
        filters = Filter.fetch_filter(name="static_filter")
        for filt in filters:
            self.assertEqual(filt.name, "static_filter")

    def test_delete_filter(self):
        db_filter = Filter(id=self.__class__.filter_id)
        delete_id = db_filter.delete_filter()
        self.assertEqual(delete_id, self.__class__.filter_id)

        connection = Connection(id=self.__class__.conn_id)
        connection.delete_conn()
