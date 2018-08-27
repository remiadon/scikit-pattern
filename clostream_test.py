from clostream import CloStream, ItemSet
from pyroaring import BitMap
from random import shuffle

TABLE_1 = ['CD', 'AB', 'ABC', 'ABC', 'ACD']
TABLE_1 = [frozenset(t) for t in TABLE_1]

TABLE_2 = [
    ItemSet('', 0),
    ItemSet('CD', 2),
    ItemSet('AB', 3),
    ItemSet('ABC', 2),
    ItemSet('C', 4),
    ItemSet('ACD', 1),
    ItemSet('A', 4),
    ItemSet('AC', 3),
]

TABLE_3 = {
    'A': BitMap({2, 3, 5, 6, 7}),
    'B': BitMap({2, 3}),
    'C': BitMap({1, 3, 4, 5, 7}),
    'D': BitMap({1, 5}),
}

TABLE_4 = {
    frozenset('C'): 4,
    frozenset('B'): 2,
    frozenset('BC'): 3,
}

TABLE_5 = [
    ItemSet('', 0),
    ItemSet('CD', 2),
    ItemSet('AB', 3),
    ItemSet('ABC', 2),
    ItemSet('C', 5),
    ItemSet('ACD', 1),
    ItemSet('A', 4),
    ItemSet('AC', 3),
    ItemSet('B', 4),
    ItemSet('BC', 3),
]

TABLE_6 = {
    'A': BitMap({2, 3, 5, 6, 7}),
    'B': BitMap({2, 3, 8, 9}),
    'C': BitMap({1, 3, 4, 5, 7, 9}),
    'D': BitMap({1, 5}),
}

t_6 = frozenset('BC')

NO_FILTER_FN = lambda itemset: True

NULL_ITEMSET = ItemSet('', 0)


def test_phase_1():
    cs = CloStream(filter_fn=NO_FILTER_FN)
    cs.closed_table = TABLE_2
    cs.cid_list_map = TABLE_3

    assert cs._phase_1(t_6) == TABLE_4


def test_phase_1_no_modif():
    cs = CloStream(filter_fn=NO_FILTER_FN)
    for transaction in TABLE_1:
        transaction = frozenset(transaction)
        assert cs._phase_1(transaction) == {transaction: 0}
        assert len(cs.closed_table) == 1


def test_phase_2_no_temp_table_from_phase_1():
    cs = CloStream(filter_fn=NO_FILTER_FN)

    transaction = frozenset('CD')
    temp_table = {transaction: 0}

    assert cs.closed_table[0] == NULL_ITEMSET
    cs._phase_2(temp_table)

    assert cs.closed_table == [ItemSet([], 0), ItemSet('CD', 1)]
    assert cs.cid_list_map == dict(C=BitMap({1}), D=BitMap({1}))


def test_phase_2_temp_table_from_phase_1():
    cs = CloStream(filter_fn=NO_FILTER_FN)
    cs.closed_table = TABLE_2
    cs.cid_list_map = TABLE_3
    cs._phase_2(TABLE_4)

    assert set(cs.closed_table) == set(TABLE_5)
    assert cs.cid_list_map == TABLE_6


def test_add():
    cs = CloStream(filter_fn=NO_FILTER_FN)
    for transaction in TABLE_1:
        cs.add(transaction)

    cs.add(t_6)

    assert set(cs.cid_list_map) == set(TABLE_6)


def test_add_same_transactions_in_different_orders():
    transactions = TABLE_1.copy()

    baseline = CloStream()
    for t in transactions:
        baseline.add(t)

    for i in range(20):
        new_cs = CloStream()
        shuffle(transactions)
        for t in transactions:
            new_cs.add(t)

        assert set(new_cs.closed_table) == set(baseline.closed_table)


def test_filter_as_arg_equivalent_to_post_process_filter():
    filter_fns = [
        lambda itemset: len(itemset) > 2,
        lambda itemset: 'A' not in itemset,
        lambda itemset: {'B', 'C'} in itemset,
    ]

    for filter_fn in filter_fns:
        cs_no_filter = CloStream(filter_fn=NO_FILTER_FN)
        cs_filter = CloStream(filter_fn=filter_fn)

        for transaction in TABLE_1:
            cs_filter.add(transaction)
            cs_no_filter.add(transaction)

        post_filtered_closed_table = set(filter(filter_fn, cs_no_filter.closed_table))
        post_filtered_closed_table.add(NULL_ITEMSET)
        assert set(cs_filter.closed_table) == post_filtered_closed_table
