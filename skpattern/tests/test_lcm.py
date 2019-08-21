from skpattern import LCM
from roaringbitmap import RoaringBitmap

import pandas as pd

def test_lcm():
    transactions = [
        [1, 2, 3, 4, 5, 6],
        [2, 3, 5],
        [2, 5],
        [1, 2, 4, 5, 6],
        [2, 4],
        [1, 4, 6],
        [3, 4, 6],
    ]

    lcm = LCM(min_supp=3)
    for t in transactions:
        lcm.add(t)

    true_item_to_tids = {
        1 : RoaringBitmap([0, 3, 5]),
        2: RoaringBitmap([0, 1, 2, 3, 4]),
        3 : RoaringBitmap([0, 1, 6]),
        4 : RoaringBitmap([0, 3, 4, 5, 6]),
        5 : RoaringBitmap([0, 1, 2, 3]),
        6 : RoaringBitmap([0, 3, 5, 6]),
    }

    for item in lcm.item_to_tids.keys():
        assert lcm.item_to_tids[item] == true_item_to_tids[item]

    results = lcm.discover()
    true_results_df = pd.DataFrame([
            [{2}, 5],
            [{3}, 3],
            [{4}, 5],
            [{2, 4}, 3],
            [{2, 5}, 4],
            [{4, 6}, 4],
            [{1, 4, 6}, 3],
    ], columns=['itemset', 'support'])
    true_results_df.loc[:, 'itemset'] = true_results_df.itemset.map(frozenset)
    assert results.to_dict('records') == true_results_df.to_dict('records')
