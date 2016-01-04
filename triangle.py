import leveldb
import itertools
import logging
import pickle

logger = logging.getLogger(__name__)

SEPARATOR = '::'
DEFAULT_PREFIX = 'spo'


def _computer_ordered_values_set(ordered_values, pos=0):
    if pos == 3:
        return []
    current = [ordered_values[pos:pos+1]]
    if ':' in ordered_values[pos]:
        items = ordered_values[pos].split(':')
        for i in xrange(1, len(items)):
            current.append([':'.join(items[:i])])

    other = _computer_ordered_values_set(ordered_values, pos+1)
    if len(other) == 0:
        return current

    result = []
    for c in current:
        for o in other:
            result.append(c+o)

    return result


def _insert_permutations(insertion_kv, writer):
    value = tuple(insertion_kv[k] for k in DEFAULT_PREFIX)
    serialized_value = pickle.dumps(value)

    for winding_order in itertools.permutations('spo'):
        ordered_values = [insertion_kv[k] for k in winding_order]
        for order in _computer_ordered_values_set(ordered_values):
            key = (''.join(winding_order) + SEPARATOR + SEPARATOR.join(order))
            writer.Put(key, serialized_value)


class EmptyQueryException(Exception):
    pass


class FixedPointTransaction(object):
    SINGLE_QUERY_KEYS = {
        'ps': 'pso',
        'sp': 'spo',
        'o': 'osp',
        'p': 'pso',
        's': 'spo',
        'so': 'sop',
        'os': 'osp',
        'po': 'pos',
        'op': 'ops'
    }

    def __init__(self, db, *args, **kwargs):
        self.db = db
        self.query = {}
        for pt, pt_value in kwargs.iteritems():
            if pt in ['s', 'p', 'o']:
                self.query[pt] = pt_value

    def traverse(self, *args, **kwargs):
        return FixedPointTransaction(self.db, **dict(self.query, **kwargs))

    def _generate_query_key(self):
        if len(self.query) == 0:
            raise EmptyQueryException(self.query)

        ks, vs = zip(*self.query.iteritems())
        prefix = ''.join(ks)
        if len(prefix) < 3:
            prefix = self.SINGLE_QUERY_KEYS[''.join(prefix)]
        return prefix, prefix + SEPARATOR + SEPARATOR.join(vs)

    def __iter__(self):
        seen = set()
        prefix, query = self._generate_query_key()
        for item_k, item_v in self.db.RangeIter(query):

            if not item_k.startswith(query):
                raise StopIteration()

            if item_v not in seen:
                seen.add(item_v)
                yield pickle.loads(item_v)


class BatchInsertStatement(object):

    def __init__(self, db):
        self.db = db
        self.batch = leveldb.WriteBatch()

    def insert(self, **kwargs):
        assert set(kwargs.keys()) == {'s', 'p', 'o'}
        return _insert_permutations(kwargs, self.batch)

    def __exit__(self, type, value, traceback):
        if traceback is not None:
            logger.error("An exception occured while performing a Triangle "
                         "transaction and therefore will roll-back")
        else:
            return self.db.Write(self.batch, sync=True)

    def __enter__(self):
        return self


class Triangle(object):

    def __init__(self, db):
        self.db = db

    def insert(self, **kwargs):
        assert set(kwargs.keys()) == {'s', 'p', 'o'}
        return _insert_permutations(kwargs, self.db)

    def start(self, **kwargs):
        return FixedPointTransaction(self.db, **kwargs)

    def batch_insert(self):
        return BatchInsertStatement(self.db)
