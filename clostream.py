"""
Implementation for CloStream Algorithm
inspired from
`https://github.com/matfax/.../spmf/algorithms/frequentpatterns/clostream/AlgoCloSteam.java`
"""
from pyroaring import BitMap


class ItemSet(frozenset):
    def __new__(cls, iterable, support):
        return frozenset.__new__(cls, iterable)

    def __init__(self, iterable, support):
        self.support = support

    def __repr__(self):
        return str(set(self)) + ' : ' + repr(self.support)


def default_fiter_fn(itemset):
    return len(itemset) > 1


class CloStream():
    def __init__(self, filter_fn=default_fiter_fn):
        self.closed_table = [ItemSet([], 0)]
        self.cid_list_map = dict()
        self.n_transactions = 0
        self.filter_fn = filter_fn

    def _phase_1(self, transaction):
        temp_table = {transaction: 0}

        closed_ids = BitMap.union(*(self.cid_list_map.get(item, BitMap()) for item in transaction))

        for closed_id in closed_ids:
            cti = self.closed_table[closed_id]
            intersections = transaction.intersection(cti)

            closure_id = temp_table.get(intersections)
            if closure_id is not None:
                ctt = self.closed_table[closure_id]

                if cti.support > ctt.support:
                    temp_table[intersections] = closed_id

            else:
                temp_table[intersections] = closed_id

        return temp_table

    def _phase_2(self, table_temp):
        for entry, cid in table_temp.items():
            ctc = self.closed_table[cid]

            if entry == ctc:
                ctc.support += 1
            elif not self.filter_fn(entry):
                continue
            else:
                self.closed_table.append(ItemSet(entry, ctc.support + 1))
                # yield entry
                for item in entry:
                    cid_set = self.cid_list_map.get(item, BitMap())
                    if not cid_set:
                        self.cid_list_map[item] = cid_set

                    cid_set.add(len(self.closed_table) - 1)

    def add(self, transaction):
        transaction = frozenset(transaction)

        table_temp = self._phase_1(transaction)

        self._phase_2(table_temp)

        self.n_transactions += 1

    def get_closed_itemsets(self):
        return self.closed_table[:1]

    def get_closed_frequent_itemsets(self, min_supp):
        assert min_supp >= 0
        if isinstance(min_supp, float):
            assert min_supp < 0.5
            min_supp *= self.n_transactions

        return [x for x in self.closed_table[1:] if x.support > min_supp]




