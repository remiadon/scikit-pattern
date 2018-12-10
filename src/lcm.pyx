# distutils: language = c++

from transformers import TIDLister
from sklearn.externals.joblib import Parallel, delayed  # TODO from skpattern.externals
import pandas as pd

from libcpp.unordered_set cimport unordered_set
from cython.operator cimport dereference

ctypedef bint (*filter_type)(tuple)

cdef bint default_filter(tuple args):
    return len(args[0]) > 1

cdef tuple format_with_tids(set p_prime, object p_idxs):
    return p_prime, p_idxs

def format_no_tids(set p_prime, object p_idxs):
    return p_prime, len(p_idxs)


cdef set get_new_scope_keys(self, unordered_set[int]& new_keys, p_idxs):
    # TODO  parallel iteration over new_keys
    cdef:
        int key
        unsigned int inter_len
        unsigned int min_supp = self.min_support
        unordered_set[int]* new_scope_keys = new unordered_set[int]()

    for key in new_keys:
        with nogil:
            inter_len = p_idxs.intersection_len(self.item_to_tids[key])
        if inter_len >= min_supp:
            dereference(new_scope_keys).insert(key)

    return new_scope_keys


class LCM():
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
        self.extra_col = 'tids' if return_tids else 'support'
        self.format = format_with_tids if return_tids else format_no_tids

    def add(self, df):
        if self.min_support < 1:
            self.min_support *= df.shape[0]

        self.item_to_tids.add(df)

    def discover_yield(self):
        self.item_to_tids.trim(self.min_support)
        for key, key_idxs in self.item_to_tids.items():
            yield from self._inner(set(), key_idxs, key, self.item_to_tids.keys())

    def discover(self):
        data = filter(self.filter_fn, self.discover_yield())
        return pd.DataFrame.from_records(data=data, columns=['itemset', self.extra_col])

    def _inner(self, p, p_idxs, limit, keys):
        # CDB = project and reduceDB w.r.t.P and limit
        keys = keys - p
        with nogil:
            cp = {k for k in keys if p_idxs.issubset(self.item_to_tids[k])}

        max_k = max(cp, default=0)
        if max_k and max_k <= limit:
            p_prime = p.union(cp)
            new_scope_keys = get_new_scope_keys(self, keys - p_prime, p_idxs)

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
    pass # TODO