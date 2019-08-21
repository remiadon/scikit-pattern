import itertools
import pandas as pd
from collections import defaultdict
from roaringbitmap import RoaringBitmap
from collections import defaultdict


class LCM(object):
    def __init__(self,
                 min_supp=.005,
                 n_jobs=1,
                 verbose=1,
                 return_tids=False):
        # TODO = add filter_fn
        self.min_support = min_supp
        self.item_to_tids = defaultdict(RoaringBitmap)
        self.verbose = verbose
        self.format = self._format_with_tids if return_tids else self._format
        self.extra_col = 'tids' if return_tids else 'support'
        self.n_transactions = 0

    def add(self, transaction):
        transaction = frozenset(transaction)
        for item in transaction:
            self.item_to_tids[item].add(self.n_transactions)
        self.n_transactions += 1

    def discover_yield(self):
        sorted_keys = sorted(self.item_to_tids.keys())
        for idx, key in enumerate(sorted_keys):
            key_idxs = self.item_to_tids[key]
            if len(key_idxs) >= self.min_support:
                scope_keys = self.item_to_tids.keys()
                yield from self._inner(frozenset(), key_idxs, key, scope_keys)

    def discover(self):
        data = self.discover_yield()
        return pd.DataFrame.from_records(data=data, columns=['itemset', self.extra_col])

    def get_new_lens(self, p_idxs, keys, p_prime):
        new_keys = keys - p_prime
        new_lens = set()
        for item in new_keys:
            inter_len = p_idxs.intersection_len(self.item_to_tids[item])
            if inter_len >= self.min_support:
                new_lens.add(item)
        return new_lens

    def _format_with_tids(self, p_prime, p_idxs):
        return p_prime, p_idxs

    def _format(self, p_prime, p_idxs):
        return p_prime, len(p_idxs)

    def get_new_scope_keys(self, new_keys, p_idxs):
        new_scope_keys = set()
        for key in new_keys:
            inter_len = p_idxs.intersection_len(self.item_to_tids[key])
            if inter_len >= self.min_support:
                new_scope_keys.add(key)
        return new_scope_keys

    def _inner(self, p, p_idxs, limit, scope_keys):
        # CDB = project and reduceDB w.r.t.P and limit
        keys = scope_keys - p
        cp = {k for k in keys if p_idxs.issubset(self.item_to_tids[k])}

        max_k = max(cp, default=0)
        if max_k and max_k <= limit:
            p_prime = p.union(cp)
            new_scope_keys = self.get_new_scope_keys(keys - p_prime, p_idxs)

            yield self.format(p_prime, p_idxs)

            new_tids = dict()

            for new_limit in new_scope_keys:
                if new_limit < limit:
                    if not new_tids:
                        new_tids = {item: p_idxs.intersection(self.item_to_tids[item]) for item in new_scope_keys}
                    yield from self._inner(p_prime, new_tids[new_limit], new_limit, new_scope_keys)


            new_tids.clear()
            #new_scope_keys.clear()
        cp.clear()


class LCM_max(LCM):
    pass