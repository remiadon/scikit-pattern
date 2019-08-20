"""
Implementation for CloStream Algorithm
"""
from collections import defaultdict
import pandas as pd
from roaringbitmap import RoaringBitmap
from copy import deepcopy


def default_fiter_fn(itemset):
    return len(itemset) > 1


class CloStream():
    def __init__(self, filter_fn=default_fiter_fn):
        self.closed_df = pd.DataFrame(dict(itemset=[frozenset()], support=[0]))
        self.cid_list_map = defaultdict(RoaringBitmap)
        self.n_transactions = 0
        self.filter_fn = filter_fn

    def get_closed_ids(self, transaction):
        # TODO : union_fn = np.frompyfunc(BitMap.union) --> union_fn.reduce(sub_cid_list)
        sub_cid_list = [self.cid_list_map[item] for item in transaction]
        return RoaringBitmap.union(*sub_cid_list)

    def _phase_1(self, transaction):
        temp_table = {transaction: 0}

        closed_ids = self.get_closed_ids(transaction)

        sub_closed_df = self.closed_df.loc[closed_ids]

        #sub_closed_df.loc[:, 'S'] = sub_closed_df.itemset.map(transaction.intersection)

        for closed_id, itemset, cti_supp in sub_closed_df.itertuples(index=True, name=None):
            intersection = transaction.intersection(itemset)
            closure_id = temp_table.get(intersection)

            if closure_id is not None:
                ctt_supp = self.closed_df.loc[closure_id, 'support']

                if cti_supp > ctt_supp:
                    temp_table[intersection] = closed_id

            else:
                temp_table[intersection] = closed_id

        return temp_table

    def _phase_2(self, table_temp):
        for entry, cid in table_temp.items():
            ctc, ctc_supp = self.closed_df.loc[cid]

            # filter_fn at first ?
            if entry == ctc:
                self.closed_df.loc[cid, 'support'] += 1
            elif not self.filter_fn(entry):
                continue
            else:
                self.closed_df.loc[self.closed_df.shape[0]] = [entry, ctc_supp + 1]
                for item in entry:
                    cid_set = self.cid_list_map[item]
                    if not cid_set:
                        self.cid_list_map[item] = cid_set

                    cid_set.add(len(self.closed_df) - 1)

    def add(self, transaction):
        transaction = frozenset(transaction)

        table_temp = self._phase_1(transaction)

        self._phase_2(table_temp)

        self.n_transactions += 1

        return self

    def discover(self):
        return self.closed_df[:1]


class TIDCloStream(CloStream):
    def __init__(self, filter_fn):
        self.closed_df = pd.DataFrame(dict(itemset=[frozenset()], tid_bitmap=RoaringBitmap()))
        self.cid_list_map = defaultdict(RoaringBitmap)
        self.n_transactions = 0
        self.filter_fn = filter_fn

    def add_with_tid(self, tid, transaction):
        # TOD0 : automatically deduce tid
        transaction = frozenset(transaction)
        table_temp = self._phase_1(transaction)
        self._phase_2_with_tid(tid, table_temp)

    def _phase_2_with_tid(self, tid, table_temp):
        for entry, cid in table_temp.items():
            ctc = self.closed_df[cid]

            if entry == ctc:
                ctc.tid_bitmap.add(tid)
            elif not self.filter_fn(entry):
                continue
            else:
                entry_bm = deepcopy(ctc.tid_bitmap)
                #entry_bm = ctc.tid_bitmap.copy()
                self.closed_df.loc[self.closed_df.shape[0] + 1] = [entry, entry_bm]
                for item in entry:
                    cid_set = self.cid_list_map[item]
                    if not cid_set:
                        self.cid_list_map[item] = cid_set

                    cid_set.add(len(self.closed_df) - 1)

