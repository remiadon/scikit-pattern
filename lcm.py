from roaringbitmap import RoaringBitmap
from transformers import TIDLister
from sklearn.externals.joblib import Parallel, delayed  # TODO from skpattern.externals
import itertools
import pandas as pd


def default_filter(args):
    return len(args[0]) > 1

class LCM(object):
    def __init__(self, min_supp=.005, verbose=1):
        self.min_support = min_supp
        self.item_to_tids = TIDLister()

    def discover_yield(self, df):
        df = df.copy()
        if self.min_support < 1:
            self.min_support *= df.shape[0]

        self.item_to_tids.add(df)

        item_to_bitmaps = {k: v for k, v in self.item_to_tids.items() if len(v) >= self.min_support}

        key = lambda e: len(e[1])
        item_to_bitmaps = sorted(item_to_bitmaps.items(), key=key, reverse=True)

        #import ipdb; ipdb.set_trace()

        for item, item_idxs in item_to_bitmaps:
            yield from self._inner(frozenset(), item_idxs, df, item, self.min_support)

        item_to_bitmaps.clear()

    def discover(self, df, k=None):
        data = itertools.islice(self.discover_yield(df), k)
        return pd.DataFrame.from_records(data=data, columns=['itemset', 'tid_list'])

    def _inner(self, p, p_idxs, df, limit, min_supp):
        # CDB = project and reduceDB w.r.t.P and limit
        conditional_df = df.loc[p_idxs]

        # TODO : pass min_supp to tid_lister
        new_tids = TIDLister().add(conditional_df)
        cp_bitmaps = {
            k: v for k, v in new_tids.items() if len(v) == conditional_df.shape[0]
        }

        max_k = max(cp_bitmaps.keys() - p, default=None)
        if max_k and max_k <= limit:
            p_prime = frozenset(cp_bitmaps.keys())
            p_prime_idxs = RoaringBitmap.intersection(*cp_bitmaps.values())

            yield p_prime, p_prime_idxs

            for new_limit, new_indexes in new_tids.items():
                if new_limit < limit and len(new_indexes) >= min_supp:
                    yield from self._inner(p_prime, new_indexes, conditional_df, new_limit, min_supp)

        new_tids.clear()
        cp_bitmaps.clear()


class BMLCM(object):
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
        self.lcm_inner = self._inner_with_tids if return_tids else self._inner
        self.extra_col = 'tids' if return_tids else 'support'

    def add(self, df):
        if self.min_support < 1:
            self.min_support *= df.shape[0]

        self.item_to_tids.add(df)

        #self.item_to_tids.trim(self.min_support)

    def discover_yield(self):
        # key = lambda e: e[0]
        # item_to_bitmaps = sorted(item_to_bitmaps.items(), key=key, reverse=True)

        for item, item_idxs in self.item_to_tids.items():
            yield from self.lcm_inner(frozenset(), item_idxs, item, self.item_to_tids.keys())

        #self.item_to_tids.clear()

    def discover(self):
        self.item_to_tids.trim(self.min_support)
        data = filter(self.filter_fn, self.discover_yield())
        return pd.DataFrame.from_records(data=data, columns=['itemset', self.extra_col])

    #@profile
    def _inner(self, p, p_idxs, limit, keys):
        # CDB = project and reduceDB w.r.t.P and limit
        keys = keys - p
        cp = {k for k in keys if p_idxs.issubset(self.item_to_tids[k])}

        max_k = max(cp, default=0)
        if max_k and max_k <= limit:
            new_lens = {item: p_idxs.intersection_len(self.item_to_tids[item]) for item in keys}
            p_prime = p.union(cp)
            p_prime_supp = len(p_idxs)

            yield p_prime, p_prime_supp

            new_tids = None

            for new_limit, new_len in new_lens.items():
                if new_len >= self.min_support and new_limit < limit:
                    if not new_tids:
                        new_tids = {item: p_idxs.intersection(self.item_to_tids[item]) for item in keys}
                    yield from self.lcm_inner(p_prime, new_tids[new_limit], new_limit, keys)

            if new_tids:
                new_tids.clear()
            new_lens.clear()
        cp.clear()

    def _inner_with_tids(self, p, p_idxs, limit, keys):
        # CDB = project and reduceDB w.r.t.P and limit
        keys = keys - p
        cp = {k for k in keys if p_idxs.issubset(self.item_to_tids[k])}

        max_k = max(cp, default=0)
        if max_k and max_k <= limit:
            new_tids = {item: p_idxs.intersection(self.item_to_tids[item]) for item in keys}
            p_prime = p.union(cp)
            p_prime_idxs = RoaringBitmap.union(*new_tids.values())

            yield p_prime, p_prime_idxs

            for new_limit, new_p_idxs in new_tids.items():
                if len(new_p_idxs) >= self.min_support and new_limit < limit:
                    yield from self.lcm_inner(p_prime, new_p_idxs, new_limit, keys)

            new_tids.clear()
        cp.clear()
