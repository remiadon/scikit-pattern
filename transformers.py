from collections import defaultdict
from roaringbitmap import RoaringBitmap
from sklearn.externals import joblib
from sortedcontainers import SortedDict
from collections import Callable

import pandas as pd


def check_df_or_series(fn, *args, **kwargs):
    def wrapper(obj):
        if not isinstance(obj, (pd.DataFrame, pd.Series)):
            raise ValueError(f'{obj} should be either an instance of pd.DataFrame or pd.Series')
        return fn(obj, *args, **kwargs)
    return wrapper


class SortedDefaultDict(SortedDict):
    def __init__(self, default_factory=None, *a, **kw):
        if (default_factory is not None and not isinstance(default_factory, Callable)):
            raise TypeError('first argument must be callable')
        super(SortedDefaultDict, self).__init__(self, *a, **kw)
        self.default_factory = default_factory
        
    def __getitem__(self, key):
        try:
            return SortedDict.__getitem__(self, key)
        except KeyError:
            return self.__missing__(key)
 
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def trim(self, min_supp):
        # TODO every call to discard is O(log(n)), we can surely do better
        keys_to_discard = {k for k, v in self.items() if len(v) < min_supp}
        for key in keys_to_discard:
            del self[key]
        keys_to_discard.clear()


class TIDLister(defaultdict):
    def __init__(self, n_jobs=1, verbose=0):
        self.n_jobs = n_jobs
        self.verbose = verbose
        super(TIDLister, self).__init__(RoaringBitmap)

    def add(self, obj):
        return self.add_df_par(obj) if isinstance(obj, pd.DataFrame) else self.add_series(obj)

    def add_series(self, s):
        for row in s.iteritems():
            index, items = row[0], row[1:]
            for i in items[0]:
                self[i].add(index)
        return self

    def add_df_col(self, col):
        for idx, value in col.iteritems():
            item = '{}_{}'.format(col.name, value).encode('utf-8')
            self[item].add(idx)

        return self

    def add_df_par(self, df):
        df = df.reset_index()
        n_jobs = self.n_jobs if df.shape[0] > 200 else 1
        p = joblib.Parallel(n_jobs=n_jobs, verbose=self.verbose, backend='threading')
        p(joblib.delayed(self.add_df_col)(df[col]) for col in df.columns)
        return self

    def trim(self, min_supp):
        keys = {k for k, v in self.items() if len(v) < min_supp}
        for k in keys:
            del self[k]
        keys.clear()
