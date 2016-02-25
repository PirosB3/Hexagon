import unittest
import leveldb

import logging
import tempfile
from hexagon import Hexagon
from hexagon.utils import *

import mock
import os
import json

logger = logging.getLogger('hexagon')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


class HexagonTest(unittest.TestCase):

    def setUp(self):
        path = tempfile.mktemp('.tmp')
        self.triangle = Hexagon(path)

    def test_insertion(self):
        wb = mock.Mock()
        self.triangle.relations_db = mock.Mock()
        with mock.patch('hexagon.core.leveldb.WriteBatch', return_value=wb):
            self.triangle.insert(s='Daniel', p='loves', o='Cheese')
            self.assertEqual(
                sorted(i[0][0] for i in wb.Put.call_args_list),
                sorted(['spo::Daniel::loves::Cheese',
                        'sop::Daniel::Cheese::loves',
                        'pso::loves::Daniel::Cheese',
                        'pos::loves::Cheese::Daniel',
                        'osp::Cheese::Daniel::loves',
                        'ops::Cheese::loves::Daniel'])
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

        self.assertEqual(list(self.triangle.subjects()), ['Daniel'])
        self.assertEqual(list(self.triangle.predicates()), ['loves'])
        self.assertEqual(list(self.triangle.objects()), ['Cheese', 'Sushi'])

    def test_exception_rolls_back(self):
        try:
            with self.triangle.batch_insert() as f:
                f.insert(s='Daniel', p='loves', o='Cheese')
                f.insert(s='Daniel', p='loves', o='Sushi')
                0/0
        except ZeroDivisionError:
            pass
        self.assertEqual(len(set(self.triangle.start(s='Daniel'))), 0)

    def test_graph_of_the_gods(self):
        path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'data/graph_of_the_gods.json'
        )
        with open(path) as f:
            graph = json.loads(f.read())

            vertex_map = {}
            for v in graph['node']:
                vertex_map[v['-id']] = v['data'][1]['#text'] + ':' + v['data'][0]['#text']

            with self.triangle.batch_insert() as h:
                for e in graph['edge']:
                    s = vertex_map[e['-source']]
                    o = vertex_map[e['-target']]
                    p = e['-label']
                    h.insert(s=s, p=p, o=o)

        # Where do gods live?
        self.assertEqual(
            sorted(list(self.triangle.start(s='god').traverse(p='lives'))),
            sorted([('god:jupiter', 'lives', 'location:sky'),
                   ('god:neptune', 'lives', 'location:sea'),
                   ('god:pluto', 'lives', 'location:tartarus')])
        )

        # Usually gods live in the sky
        self.assertEqual(
            list(self.triangle.start(s='god').traverse(p='lives').traverse(o='location:sky')),
            [('god:jupiter', 'lives', 'location:sky')]
        )

        result = (
            vs(self.triangle.start(s='demigod')) &
            vs(self.triangle.start(p='mother').traverse(o='human:alcmene'))
        )
        self.assertEqual(result, {'demigod:hercules'})


if __name__ == '__main__':
    unittest.main()
