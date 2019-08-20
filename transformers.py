from collections import defaultdict
from roaringbitmap import RoaringBitmap
from sklearn.externals import joblib

import pandas as pd


def check_df_or_series(fn, *args, **kwargs):
    def wrapper(obj):
        if not isinstance(obj, (pd.DataFrame, pd.Series)):
            raise ValueError(f'{obj} should be either an instance of pd.DataFrame or pd.Series')
        return fn(obj, *args, **kwargs)
    return wrapper


class TIDLister(defaultdict):
    def __init__(self, n_jobs=1, verbose=0):
        self.n_jobs = n_jobs
        self.verbose = verbose
        self.low = 0
        self.high = 0
        super(TIDLister, self).__init__(RoaringBitmap)

    def add(self, transaction):
        for item in transaction:
            self[item].add(self.high)
        self.high += 1

    def remove(self, transaction):
        for item in transaction:
            self[item].remove(self.low)
        self.low += 1

    def trim(self, min_supp):
        keys = {k for k,v in self.items() if len(v) < min_supp}
        for k in keys:
            del self[k]
        keys.clear()
