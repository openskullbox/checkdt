import unittest

from checkdt.core.connection import Connection

from checkdt.core.filter import Filter
from checkdt.core.source import Source
from tests.common import SortTest
from tests.const import CONNECT_ARGS, SOURCE_ARGS, FILTER_ARGS, FILTER_VAL

sort_test = SortTest(
    [
        "test_create_source",
        "test_fetch_source",
        "test_update_source",
        "test_get_source_data",
        "test_delete_source",
    ]
)
sort_test.sort_run()


class TestSource(unittest.TestCase):
    conn_id = None
    source_id = None
    filter_id = None

    def test_create_source(self):
        connection = Connection(**CONNECT_ARGS)
        connection.create_conn()
        self.__class__.conn_id = connection.conn_base.id

        SOURCE_ARGS['conn_id'] = self.__class__.conn_id
        source = Source(**SOURCE_ARGS)
        source.create_source()
        self.__class__.source_id = source.source_base.id
        self.assertTrue(type(source.source_base.id) == int)

    def test_fetch_source(self):
        source = Source(id=self.__class__.source_id)
        self.assertEqual(source.source_base.name, "table")
        self.assertEqual(len(source.source_base.field_list), 3)

    def test_update_source(self):
        FILTER_ARGS["conn_id"] = self.__class__.conn_id
        _filter = Filter(**FILTER_ARGS)
        _filter.create_filter()
        self.__class__.filter_id = _filter.filter_base.id
        source = Source(id=self.__class__.source_id)
        source.source_base.name = "public_table"
        source.source_base.field_list[0].filter_id = self.__class__.filter_id
        source.source_base.field_list[0].op = "ge"
        source.add_field({"fieldname": "description", "type": "string", "length": 100})
        source.update_source()

        self.assertEqual(source.source_base.name, "public_x_pms_crs_ids")
        self.assertEqual(len(source.source_base.field_list), 4)

    def test_get_source_data(self):
        source = Source(id=self.__class__.source_id)
        df = source.get_source_data()
        self.assertEqual(df.loc[0, 'id'], FILTER_VAL)

    def test_delete_source(self):
        source = Source(id=self.__class__.source_id)
        delete_id = source.delete_source()
        _filter = Filter(id=self.__class__.filter_id)
        _filter.delete_filter()
        connection = Connection(id=self.__class__.conn_id)
        connection.delete_conn()
        self.assertEqual(delete_id, self.__class__.source_id)
