from collections import defaultdict
from roaringbitmap import RoaringBitmap
from sklearn.externals import joblib

import pandas as pd


class TIDLister(defaultdict):
    def __init__(self, n_jobs=1, verbose=0, key_encoding=id):
        self.n_jobs = n_jobs
        self.verbose = verbose
        self.n_transactions = 0
        self.key_encoding = key_encoding
        super(TIDLister, self).__init__(RoaringBitmap)

    def add(self, obj):
        return self.add_df_par(obj) if isinstance(obj, pd.DataFrame) else self.add_series(obj)

    def add_series(self, s):
        for row in s.iteritems():
            index, items = row[0], row[1:]
            for i in items[0]:
                self[self.key_encoding(i)].add(index)
        return self

    def add_df_col(self, col):
        for idx, value in col.iteritems():
            item = (col.name, value)
            item = self.key_encoding(item)
            self[item].add(idx)

        return self

    def add_transaction(self, transaction):
        self.n_transactions += 1
        for item in transaction:
            self[item].add(self.n_transactions)

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
