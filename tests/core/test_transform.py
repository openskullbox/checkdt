import unittest

from checkdt.core.transform import Transform
from tests.common import SortTest

sort_test = SortTest(
    [
        "test_create_transform",
        # "test_execute_transform"
    ]
)
sort_test.sort_run()


class TestTransform(unittest.TestCase):
    def test_create_transform(self):
        start_args = {
            'name': 'csv_to_s3',
            'kwargs': {'s3_bucket': 'path',},
        }
        _transform = Transform(**start_args)
        _transform.create_transform()
        self.assertTrue(type(_transform.id), int)
