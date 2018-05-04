import pytest
import pandas as pd
import numpy as np

from d6tjoin.utils import df_str_summary, BaseJoin, PreJoin

def test_df_str_summary():
    df = pd.DataFrame({'a': ['a', 'aa'] * 2})
    df['b'] = ['aa', 'aaa'] * 2

    dft = df_str_summary(df)
    assert np.all(dft.values == np.array([[ 1.5,  1.5,  1. ,  2. ,  4. ],
       [ 2.5,  2.5,  2. ,  3. ,  4. ]]))
    dft = df_str_summary(df,['a'])
    assert np.all(dft.values == np.array([1.5,  1.5,  1. ,  2. ,  4.]))

    dft = df_str_summary(df,unique_count=True)
    assert np.all(dft.values == np.array([[ 1.5,  1.5,  1. ,  2. ,  4. ,  2. ],
       [ 2.5,  2.5,  2. ,  3. ,  4. ,  2. ]]))


def test_basejoin():
    df1 = pd.DataFrame({'a': range(3), 'b': range(3)})
    df2 = pd.DataFrame({'a': range(3), 'b': range(3)})

    with pytest.raises(ValueError) as e:
        j = PreJoin([df1], ['a'])
    with pytest.raises(NotImplementedError) as e:
        j = PreJoin([df1,df2,df1], ['a'])

    j1 = PreJoin([df1,df2], ['a','b'])
    j2 = PreJoin([df1,df2], [['a','b'],['a','b']], keys_bydf=True)
    j3 = PreJoin([df1,df2], [['a','a'],['b','b']])
    assert j1.keys == [['a', 'a'], ['b', 'b']]
    assert j1.keys == j2.keys
    assert j2.keys == j3.keys
    assert j1.keysdf == [['a', 'b'], ['a', 'b']]
    assert j1.keysdf == j2.keysdf
    assert j3.keysdf == j2.keysdf

    df2 = pd.DataFrame({'a': range(3), 'c': range(3)})

    with pytest.raises(KeyError) as e:
        j1 = PreJoin([df1,df2], ['a','c'])

    j2 = PreJoin([df1,df2], [['a','b'],['a','c']], keys_bydf=True)
    j3 = PreJoin([df1,df2], [['a','a'],['b','c']])
    assert j2.keys == [['a', 'a'], ['b', 'c']]
    assert j3.keys == j2.keys
    assert j2.keysdf == [['a', 'b'], ['a', 'c']]
    assert j3.keysdf == j2.keysdf


def test_prejoin():
    df1 = pd.DataFrame({'a': range(3), 'b': range(3)})
    df2 = pd.DataFrame({'a': range(3), 'c': range(3)})

    j = PreJoin([df1,df2],['a'])
    dfr = j.stats_prejoin(return_results=True)
    results = dfr.to_dict()
    check = {'all matched': {0: True, 1: True},
         'inner': {0: 3, 1: 3},
         'key left': {0: 'a', 1: '__all__'},
         'key right': {0: 'a', 1: '__all__'},
         'left': {0: 3, 1: 3},
         'outer': {0: 3, 1: 3},
         'right': {0: 3, 1: 3},
         'unmatched left': {0: 0, 1: 0},
         'unmatched right': {0: 0, 1: 0},
         'unmatched total': {0: 0, 1: 0}}
    assert results == check

    df2 = pd.DataFrame({'a': range(3,6), 'c': range(3)})

    j = PreJoin([df1,df2],['a'])
    dfr = j.stats_prejoin(return_results=True)
    assert (~dfr['all matched']).all()

    df2 = pd.DataFrame({'b': range(3,6), 'a': range(3), 'v':range(3)})
    cfg_keys = ['a', 'b']
    j = PreJoin([df1,df2],cfg_keys)
    dfr = j.stats_prejoin(return_results=True)
    assert dfr['all matched'].tolist()==[True, False, False]

    # test show_input
    dfr = j.show_input(1,keys_only=False)
    assert dfr[0].equals(df1.head(1))
    assert dfr[1].equals(df2.head(1))
    dfr = j.show_input(-1,keys_only=True)
    assert dfr[0][cfg_keys].equals(df1[cfg_keys])
    assert dfr[1][cfg_keys].equals(df2[cfg_keys])

    # test show_unmatched
    j.show_unmatched('b',print_only=True) # just make sure print_only runs without errors
    dfr = j.show_unmatched('b',nrecords=-1)
    assert dfr['left'].equals(df1['b'])
    assert dfr['right'].equals(df2['b'])
    dfr = j.show_matched('a',nrecords=-1)
    assert dfr['left'].equals(df1['a'])
    assert dfr['right'].equals(df2['a'])
    dfr = j.show_unmatched('__all__',nrecords=-1)
    assert dfr['left'].equals(df1[cfg_keys])
    assert dfr['right'].equals(df2[cfg_keys])
    dfr = j.show_matched('__all__')
    assert dfr['left'].empty
    assert dfr['right'].empty

    dfr = j.show_unmatched('b',nrecords=1)
    assert dfr['left'].equals(df1['b'].head(1))
    assert dfr['right'].equals(df2['b'].head(1))

    dfr = j.show_unmatched('b',keys_only=False,nrecords=-1)
    assert dfr['left'].equals(df1)
    assert dfr['right'].equals(df2)

    dfr = j.show_unmatched('a')
    assert dfr['left'].empty
    assert dfr['right'].empty
    dfr = j.show_matched('b')
    assert dfr['left'].empty
    assert dfr['right'].empty

    # test show_unmatched
    j = PreJoin([df1,df2],['a'])
    with pytest.raises(RuntimeError) as e:
        j.show_unmatched('a', print_only=True)
    j.stats_prejoin()
    dfr = j.show_matched('__all__',nrecords=-1)
    assert dfr['left'].equals(df1[['a']])
    assert dfr['right'].equals(df2[['a']])
    dfr = j.show_unmatched('__all__',nrecords=-1)
    assert dfr['left'].empty
    assert dfr['right'].empty

test_prejoin()
