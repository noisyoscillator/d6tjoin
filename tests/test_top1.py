import pandas as pd
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

        dfr = d6tjoin.top1.allpairs_candidates(df1, df2,'id','id')
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
    dfr = d6tjoin.top1.allpairs_diff(df1, df2,'id','id',jellyfish.levenshtein_distance)
    assert dfr['__top1diff__'].min()==0
    assert dfr.loc[dfr['__matchtype__']=='top1 left','__top1diff__'].min()==1
    assert dfr.loc[dfr['__matchtype__']=='top1 left','__top1diff__'].max()==2

    r = d6tjoin.top1.merge_top1_diff(df1, df2,'id','id',jellyfish.levenshtein_distance)
    dfr = r['merged']

    df1, df2 = tests.test_smartjoin.gen_multikey_complex(unmatched_date=False)
    r = d6tjoin.top1.merge_top1_diff_withblock(df1, df2,'key','key',jellyfish.levenshtein_distance,['date'],['date'])
    r['merged']

    assert True

test_top1_str()