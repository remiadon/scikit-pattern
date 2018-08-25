from clostream import CloStream, ItemSet
from random import shuffle

table_1 = ['CD', 'AB', 'ABC', 'ABC', 'ACD']
table_1 = [frozenset(t) for t in table_1]

table_2 = [
    ItemSet('', 0),
    ItemSet('CD', 2),
    ItemSet('AB', 3),
    ItemSet('ABC', 2),
    ItemSet('C', 4),
    ItemSet('ACD', 1),
    ItemSet('A', 4),
    ItemSet('AC', 3),
]

table_3 = {
    'A': {2, 3, 5, 6, 7},
    'B': {2, 3},
    'C': {1, 3, 4, 5, 7},
    'D': {1, 5},
}

table_4 = {
    frozenset('C'): 4,
    frozenset('B'): 2,
    frozenset('BC'): 3,
}

table_5 = [
    ItemSet('', 0),
    ItemSet('CD', 2),
    ItemSet('AB', 3),
    ItemSet('ABC', 2),
    ItemSet('C', 5),
    ItemSet('ACD', 1),
    ItemSet('A', 4),
    ItemSet('AC', 3),
    ItemSet('BC', 3),
    ItemSet('B', 4),
]

table_6 = {
    'A': {2, 3, 5, 6, 7},
    'B': {2, 3, 8, 9},
    'C': {1, 3, 4, 5, 7, 9},
    'D': {1, 5},
}

t_6 = frozenset('BC')


def test_phase_1():
    cs = CloStream()
    cs.closed_table = table_2
    cs.cid_list_map = table_3

    assert cs._phase_1(t_6) == table_4


def test_phase_1_no_modif():
    cs = CloStream()
    for transaction in table_1:
        transaction = frozenset(transaction)
        assert cs._phase_1(transaction) == {transaction: 0}
        assert len(cs.closed_table) == 1


def test_phase_2_no_temp_table_from_phase_1():
    cs = CloStream()

    transaction = frozenset('CD')
    temp_table = {transaction: 0}

    assert cs.closed_table[0] == ItemSet([], 0)
    cs._phase_2(temp_table)

    assert cs.closed_table == [ItemSet([], 0), ItemSet('CD', 1)]
    assert cs.cid_list_map == dict(C={1}, D={1})


def test_phase_2_temp_table_from_phase_1():
    cs = CloStream()
    cs.closed_table = table_2
    cs.cid_list_map = table_3
    cs._phase_2(table_4)

    assert set(cs.closed_table) == set(table_5)
    assert cs.cid_list_map == table_6


def test_add():
    cs = CloStream()
    for transaction in table_1:
        cs.add(transaction)

    cs.add(t_6)

    assert set(cs.cid_list_map) == set(table_6)


def test_add_same_transactions_in_different_orders():
    transactions = table_1.copy()

    baseline = CloStream()
    for t in transactions:
        baseline.add(t)

    for i in range(20):
        new_cs = CloStream()
        shuffle(transactions)
        for t in transactions:
            new_cs.add(t)

        assert set(new_cs.closed_table) == set(baseline.closed_table)
