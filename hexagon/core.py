import leveldb
import itertools
import logging
import pickle

logger = logging.getLogger(__name__)

SEPARATOR = '::'
DEFAULT_PREFIX = 'spo'


def _computer_ordered_values_set(ordered_values, pos=0):
    """
    Computes triples with all levels of precision possible.
    Returns a list of triples in DEFAULT_PREFIX order
    """
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


def _insert_permutations(insertion_kv, writer, single_writers):
    value = tuple(insertion_kv[k] for k in DEFAULT_PREFIX)
    serialized_value = pickle.dumps(value)

    # Generate all possible triple permutations
    for winding_order in itertools.permutations(DEFAULT_PREFIX):

        # Order values by triple order
        ordered_values = [insertion_kv[k] for k in winding_order]

        # Compute triples to persist
        for order in _computer_ordered_values_set(ordered_values):
            key = (''.join(winding_order) + SEPARATOR + SEPARATOR.join(order))
            writer.Put(key, serialized_value)

    for item, writer in single_writers.iteritems():
        writer.Put(insertion_kv[item], '')


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
            if pt in DEFAULT_PREFIX:
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

    def __init__(self, db, single_writers):
        self.db = db
        self.batch = leveldb.WriteBatch()
        self.single_writers = single_writers

    def insert(self, **kwargs):
        assert set(kwargs.keys()) == set(DEFAULT_PREFIX)
        return _insert_permutations(kwargs, self.batch,
                                    self.single_writers)

    def __exit__(self, type, value, traceback):
        if traceback is not None:
            logger.error("An exception occured while performing a Triangle "
                         "transaction and therefore will roll-back")
        else:
            return self.db.Write(self.batch, sync=True)

    def __enter__(self):
        return self


class Hexagon(object):

    def __init__(self, path):
        self.relations_db = leveldb.LevelDB(path + '_relations.ldb')
        self._single_writers = {
            's': leveldb.LevelDB(path + '_subject.ldb'),
            'o': leveldb.LevelDB(path + '_object.ldb'),
            'p': leveldb.LevelDB(path + '_predicate.ldb'),
        }

    def insert(self, **kwargs):
        assert set(kwargs.keys()) == {'s', 'p', 'o'}
        batch = leveldb.WriteBatch()
        _insert_permutations(kwargs, batch, self._single_writers)
        self.relations_db.Write(batch, sync=True)

    def start(self, **kwargs):
        return FixedPointTransaction(self.relations_db, **kwargs)

    def batch_insert(self):
        return BatchInsertStatement(self.relations_db, self._single_writers)

    def subjects(self, prefix=''):
        for subject, _ in self._single_writers['s'].RangeIter(prefix):
            yield subject

    def objects(self, prefix=''):
        for obj, _ in self._single_writers['o'].RangeIter(prefix):
            yield obj

    def predicates(self, prefix=''):
        for predicate, _ in self._single_writers['p'].RangeIter(prefix):
            yield predicate
