import pandas as pd
from collections import defaultdict
from roaringbitmap import RoaringBitmap
from sortedcontainers import SortedDict


class LCM(object):
    def __init__(self,
                 min_supp=.005,
                 n_jobs=1,
                 verbose=1,
                 return_tids=False):
        self.min_support = min_supp
        self.item_to_tids = SortedDict()
        self.verbose = verbose
        self.format = self._format_with_tids if return_tids else self._format
        self.extra_col = 'tids' if return_tids else 'support'
        self.n_transactions = 0

    def add(self, transaction):
        transaction = frozenset(transaction)
        for item in transaction:
            if item in self.item_to_tids:
                self.item_to_tids[item].add(self.n_transactions)
            else:
                self.item_to_tids[item] = RoaringBitmap([self.n_transactions])
        self.n_transactions += 1

    def discover_yield(self):
        items = [e for e in self.item_to_tids.items() if len(e[1]) >= self.min_support]
        for key, key_idxs in items:
            if len(key_idxs) >= self.min_support:
                yield from self._inner(frozenset(), key_idxs, key, items)

    def discover(self):
        data = self.discover_yield()
        return pd.DataFrame.from_records(data=data, columns=['itemset', self.extra_col])

    def _format_with_tids(self, p_prime, p_idxs):
        return p_prime, p_idxs

    def _format(self, p_prime, p_idxs):
        return p_prime, len(p_idxs)

    def get_new_scope_keys(self, new_items, p_prime, p_idxs, limit):
        for new_limit, limit_idxs in new_items:
            if new_limit not in p_prime:
                inter_len = p_idxs.intersection_len(limit_idxs)
                if inter_len >= self.min_support:
                    new_pidxs = p_idxs.intersection(limit_idxs)
                    yield limit, new_pidxs

    def _inner(self, p, p_idxs, limit, scope_items):
        cp = (k for k, idxs in reversed(scope_items) if p_idxs.issubset(idxs))

        max_k = next(cp, None)
        if max_k and max_k == limit:
            cp = set(cp).union({max_k})
            p_prime = p.union(cp)
            yield self.format(p_prime, p_idxs)

            new_items = self.item_to_tids.items()[:self.item_to_tids.bisect(limit)]
            for new_limit, new_pidxs in self.get_new_scope_keys(new_items, p_prime, p_idxs, limit):
                yield from self._inner(p_prime, new_pidxs, new_limit, scope_items)

            cp.clear()
