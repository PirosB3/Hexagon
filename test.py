import unittest
import leveldb

import logging
import tempfile
from triangle import Triangle

import mock

logger = logging.getLogger('triangle')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


class TriangleTest(unittest.TestCase):

    def setUp(self):
        db = leveldb.LevelDB(tempfile.mktemp('.ldb'))
        self.triangle = Triangle(db)

    def test_it_exists(self):
        self.assertTrue(self.triangle)

    def test_insertion(self):
        self.triangle.db = mock.Mock()
        self.triangle.insert(s='Daniel', p='loves', o='Cheese')
        self.assertEqual(
            set(i[0] for i in self.triangle.db.Put.call_args_list),
            {('spo::Daniel::loves::Cheese', ''),
             ('sop::Daniel::Cheese::loves', ''),
             ('pso::loves::Daniel::Cheese', ''),
             ('pos::loves::Cheese::Daniel', ''),
             ('osp::Cheese::Daniel::loves', ''),
             ('ops::Cheese::loves::Daniel', '')}
        )

    def test_retreival(self):
        self.triangle.insert(s='Daniel', p='loves', o='Cheese')
        self.triangle.insert(s='Daniel', p='loves', o='Sushi')
        self.assertEqual(set(self.triangle.start(s='Daniel')), {
            ('Daniel', 'loves', 'Cheese'),
            ('Daniel', 'loves', 'Sushi')
        })
        self.assertEqual(set(self.triangle.start(o='Sushi')), {
            ('Daniel', 'loves', 'Sushi')
        })

    def test_retreival_2(self):
        self.triangle.insert(s='Daniel', p='loves', o='Cheese')
        self.triangle.insert(s='Daniel', p='loves', o='Sushi')
        self.assertEqual(set(self.triangle.start(s='Daniel').traverse(o='Cheese')), {
            ('Daniel', 'loves', 'Cheese'),
        })

    def test_batch_insert(self):
        with self.triangle.batch_insert() as f:
            f.insert(s='Daniel', p='loves', o='Cheese')
            f.insert(s='Daniel', p='loves', o='Sushi')
        self.assertEqual(set(self.triangle.start(s='Daniel')), {
            ('Daniel', 'loves', 'Cheese'),
            ('Daniel', 'loves', 'Sushi')
        })

    def test_exception_rolls_back(self):
        try:
            with self.triangle.batch_insert() as f:
                f.insert(s='Daniel', p='loves', o='Cheese')
                f.insert(s='Daniel', p='loves', o='Sushi')
                0/0
        except ZeroDivisionError:
            pass
        self.assertEqual(len(set(self.triangle.start(s='Daniel'))), 0)


if __name__ == '__main__':
    unittest.main()
