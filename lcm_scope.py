from transformers import TIDLister
from sklearn.externals.joblib import Parallel, delayed  # TODO from skpattern.externals
import itertools
import pandas as pd


def default_filter(args):
    return len(args[0]) > 1


class LCM(object):
    def __init__(self,
                 min_supp=.005,
                 filter_fn=default_filter,
                 n_jobs=1,
                 verbose=1,
                 return_tids=False):
        self.min_support = min_supp
        self.item_to_tids = TIDLister(n_jobs=n_jobs)
        self.filter_fn = filter_fn
        self.n_jobs = n_jobs
        self.verbose = verbose
        self.format = self._format_with_tids if return_tids else self._format
        self.extra_col = 'tids' if return_tids else 'support'

    def add(self, df):
        if self.min_support < 1:
            self.min_support *= df.shape[0]

        self.item_to_tids.add(df)

    def discover_yield(self):
        self.item_to_tids.trim(self.min_support)
        criteria = lambda k: (len(self.item_to_tids[k]), k)
        sorted_keys = sorted(self.item_to_tids.keys(), key=criteria)
        for idx, key in enumerate(sorted_keys):
            key_idxs = self.item_to_tids[key]
            scope_keys = set(sorted_keys[idx:])
            yield from self._inner(frozenset(), key_idxs, key, scope_keys, self.item_to_tids.keys())

    def discover(self):
        data = filter(self.filter_fn, self.discover_yield())
        return pd.DataFrame.from_records(data=data, columns=['itemset', self.extra_col])

    def get_new_lens(self, p_idxs, keys, p_prime):
        new_keys = keys - p_prime
        new_lens = dict()
        for item in new_keys:
            inter_len = p_idxs.intersection_len(self.item_to_tids[item])
            if inter_len >= self.min_support:
                new_lens[item] = inter_len
        return new_lens

    def _format_with_tids(self, p_prime, p_idxs):
        return p_prime, p_idxs

    def _format(self, p_prime, p_idxs):
        return p_prime, len(p_idxs)

    def _inner(self, p, p_idxs, limit, scope_keys, keys):
        # CDB = project and reduceDB w.r.t.P and limit
        keys = keys - p
        cp = {k for k in scope_keys if p_idxs.issubset(self.item_to_tids[k])}

        max_k = max(cp, default=0)
        if max_k and max_k <= limit:
            p_prime = p.union(cp)
            new_lens = self.get_new_lens(p_idxs, keys, p_prime)

            yield self.format(p_prime, p_idxs)

            new_tids = dict()

            for new_limit in new_lens.keys():
                if new_limit < limit:
                    if not new_tids:
                        new_tids = {item: p_idxs.intersection(self.item_to_tids[item]) for item in new_lens.keys()}
                    yield from self._inner(p_prime, new_tids[new_limit], new_limit, new_lens.keys(), new_lens.keys())


            new_tids.clear()
            new_lens.clear()
        cp.clear()


class LCM_max(LCM):
    pass