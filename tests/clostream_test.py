from src.clostream import CloStream
import pandas as pd
from pyroaring import BitMap
from random import shuffle

TABLE_1 = ['CD', 'AB', 'ABC', 'ABC', 'ACD']
TABLE_1 = [frozenset(t) for t in TABLE_1]

TABLE_2 = [
    [frozenset(''), 0],
    [frozenset('CD'), 2],
    [frozenset('AB'), 3],
    [frozenset('ABC'), 2],
    [frozenset('C'), 4],
    [frozenset('ACD'), 1],
    [frozenset('A'), 4],
    [frozenset('AC'), 3],
]

TABLE_2 = pd.DataFrame(TABLE_2, columns=['itemset', 'support'])

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
    [frozenset(''), 0],
    [frozenset('CD'), 2],
    [frozenset('AB'), 3],
    [frozenset('ABC'), 2],
    [frozenset('C'), 5],
    [frozenset('ACD'), 1],
    [frozenset('A'), 4],
    [frozenset('AC'), 3],
    [frozenset('B'), 4],
    [frozenset('BC'), 3],
]

TABLE_5 = pd.DataFrame(TABLE_5, columns=['itemset', 'support'])

TABLE_6 = {
    'A': BitMap({2, 3, 5, 6, 7}),
    'B': BitMap({2, 3, 8, 9}),
    'C': BitMap({1, 3, 4, 5, 7, 9}),
    'D': BitMap({1, 5}),
}

t_6 = frozenset('BC')

NO_FILTER_FN = lambda itemset: True

NULL_ITEMSET = [frozenset(''), 0]


def test_phase_1():
    cs = CloStream(filter_fn=NO_FILTER_FN)
    cs.closed_df = TABLE_2
    cs.cid_list_map = TABLE_3

    assert cs._phase_1(t_6) == TABLE_4


def test_phase_1_no_modif():
    cs = CloStream(filter_fn=NO_FILTER_FN)
    for transaction in TABLE_1:
        transaction = frozenset(transaction)
        assert cs._phase_1(transaction) == {transaction: 0}
        assert len(cs.closed_df) == 1


def test_phase_2_no_temp_table_from_phase_1():
    cs = CloStream(filter_fn=NO_FILTER_FN)

    transaction = frozenset('CD')
    temp_table = {transaction: 0}

    assert cs.closed_df.loc[0, 'itemset'] == frozenset()
    cs._phase_2(temp_table)

    assert cs.closed_df.values.tolist() == [[frozenset(), 0], [frozenset('CD'), 1]]
    assert cs.cid_list_map == dict(C=BitMap([1]), D=BitMap([1]))


def test_phase_2_temp_table_from_phase_1():
    cs = CloStream(filter_fn=NO_FILTER_FN)
    cs.closed_df = TABLE_2
    cs.cid_list_map = TABLE_3
    cs._phase_2(TABLE_4)

    assert set(cs.closed_df) == set(TABLE_5)
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

        assert set(new_cs.closed_df) == set(baseline.closed_df)


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

        post_filterd_indexes = cs_no_filter.closed_df.itemset.apply(filter_fn)
        post_filtered_closed_df = cs_no_filter.closed_df[post_filterd_indexes]

        cs_filter_closed_df = cs_filter.closed_df[cs_filter.closed_df.itemset != frozenset()]
        post_filtered_closed_df = post_filtered_closed_df[post_filtered_closed_df.itemset != frozenset()]
        assert pd.np.array_equal(post_filtered_closed_df.values, cs_filter_closed_df.values)
