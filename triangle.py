import itertools

SEPARATOR = '::'
DEFAULT_PREFIX = 'spo'


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
        if len(ks) < 3:
            prefix = self.SINGLE_QUERY_KEYS[''.join(ks)]
            return prefix, prefix + SEPARATOR + SEPARATOR.join(vs)

    def __iter__(self):
        prefix, query = self._generate_query_key()
        for item_k, item_v in self.db.RangeIter(query):
            pair = item_k.split(SEPARATOR)[1:]

            if not item_k.startswith(query):
                raise StopIteration()

            prefixed_pair = zip(pair, prefix)
            prefixed_pair.sort(key=lambda x: DEFAULT_PREFIX.index(x[1]))
            yield tuple(p[0] for p in prefixed_pair)


class Triangle(object):

    def __init__(self, db):
        self.db = db

    def insert(self, **kwargs):
        for winding_order in itertools.permutations('spo'):
            ordered_values = [kwargs[k] for k in winding_order]
            key = (''.join(winding_order) + SEPARATOR +
                   SEPARATOR.join(ordered_values))
            self.db.Put(key, '')

    def start(self, **kwargs):
        return FixedPointTransaction(self.db, **kwargs)
