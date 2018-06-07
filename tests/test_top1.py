import numpy as np
import pandas as pd
pd.set_option('display.expand_frame_repr', False)
import importlib
import d6tjoin.top1
import jellyfish

import tests.test_smartjoin

def gen_df2_str():
    l1 = ['a', 'b']
    l2 = [l1[0], 'ba', 'cd']
    df1 = pd.DataFrame({'id':l1*4})
    df2 = pd.DataFrame({'id':l2*4})
    df1['v1']=range(df1.shape[0])
    df2['v2']=range(df2.shape[0])
    return df1, df2

def gen_df2_num():
    l1 = [1,2]
    l2 = [l1[0],1.1,1.2]
    df1 = pd.DataFrame({'id': l1 * 4})
    df2 = pd.DataFrame({'id': l2 * 4})
    return df1, df2


def test_top1_gen_candidates():

    def helper(df1, df2):

        dfr = d6tjoin.top1.MergeTop1Diff(df1, df2,'id','id')._allpairs_candidates()
        assert dfr.shape==(4, 3)
        assert (dfr['__top1left__']==df1.values[0]).sum()==1
        assert (dfr['__top1left__']==df1.values[1]).sum()==3
        assert (dfr['__top1right__']==df1.values[0]).sum()==2
        assert (dfr['__top1right__']==df2.values[1]).sum()==1
        assert (dfr['__top1right__']==df2.values[2]).sum()==1
        assert (dfr['__matchtype__']=='exact').sum()==1
        assert (dfr['__matchtype__']=='top1 left').sum()==3

    df1, df2 = gen_df2_str()
    helper(df1, df2)

    df1, df2 = gen_df2_num()
    helper(df1, df2)


def test_top1_str():

    df1, df2 = gen_df2_str()

    r = d6tjoin.top1.MergeTop1Diff(df1, df2,'id','id',jellyfish.levenshtein_distance).merge()
    dfr = r['top1']
    assert dfr['__top1diff__'].min()==0
    assert dfr['__top1diff__'].max()==1
    assert dfr.shape==(3, 4)
    dfr = r['merged']
    assert dfr.shape==(48, 4)
    assert np.all(dfr.groupby('id').size().values==np.array([16, 32]))

    df1, df2 = tests.test_smartjoin.gen_multikey_complex(unmatched_date=False)
    r = d6tjoin.top1.MergeTop1Diff(df1, df2,'key','key',jellyfish.levenshtein_distance,['date'],['date']).merge()
    dfr = r['merged']
    assert dfr.shape==(18, 5)
    assert np.all(dfr.groupby(['date','key']).size().values==np.array([1, 1, 2, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]))

    df1.head()
    df1.merge(df2, on=['date','key']).head()
    dfr.head()

def test_top1_num():

    df1, df2 = tests.test_smartjoin.gen_multikey_complex(unmatched_date=True)
    r = d6tjoin.top1.MergeTop1Number(df1, df2,'date','date').merge()
    dfr = r['top1']
    assert dfr.shape==(4, 4)
    assert np.all(dfr.groupby('__match type__').size().values==np.array([2, 2]))
    assert dfr['__top1diff__'].dt.days.max()==2
    assert dfr['__top1diff__'].dt.days.min()==0

    df1, df2 = tests.test_smartjoin.gen_multikey_complex(unmatched_date=True)
    r = d6tjoin.top1.MergeTop1Number(df1, df2,'date','date',['key'],['key']).merge()
    dfr = r['merged']
    dfr.sort_values(['date','key'])
    r['top1'].sort_values(['__top1left__','key'])
    df1.sort_values(['key','date'])
    df2.sort_values(['key','date'])
    r['top1']

def test_top1_multi():

    df1, df2 = tests.test_smartjoin.gen_multikey_complex(unmatched_date=True)
    df2['key'] = 'Mr. '+df1['key']

    r = d6tjoin.top1.MergeTop1(df1, df2,['date','key'],['date','key']).merge()


    assert True

test_top1_multi()