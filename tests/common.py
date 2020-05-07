import unittest

class SortTest:
    def __init__(self, test_order):
        self.test_order = test_order

    def test_sorter(self, a, b):
        a_idx = self.test_order.index(a)
        b_idx = self.test_order.index(b)
        return 1 if a_idx > b_idx else -1

    def sort_run(self):
        unittest.TestLoader.sortTestMethodsUsing = self.test_sorter