import pandas as pd
import numpy as np
from collections import OrderedDict
import itertools
import warnings

# ******************************************
# helpers
# ******************************************
def set_values(dfg, key):
    v = dfg[key].unique()
    v = v[~pd.isnull(v)]
    return set(v)


def filter_group_min(dfg, col):
    """

    Returns all rows equal to min in col

    """
    return dfg[dfg[col] == dfg[col].min()]


def allpairs_candidates(dfg1, dfg2, fuzzy_left_on, fuzzy_right_on):
    values_left = set_values(dfg1, fuzzy_left_on)
    values_right = set_values(dfg2, fuzzy_right_on)

    values_left_exact = values_left.intersection(values_right)
    values_left_fuzzy = values_left.difference(values_right)

    df_candidates_fuzzy = list(itertools.product(values_left_fuzzy, values_right))
    df_candidates_fuzzy = pd.DataFrame(df_candidates_fuzzy,columns=['__top1left__','__top1right__'])
    df_candidates_fuzzy['__matchtype__'] = 'top1 left'

    df_candidates_exact = pd.DataFrame({'__top1left__': list(values_left_exact)})
    df_candidates_exact['__top1right__'] = df_candidates_exact['__top1left__']
    df_candidates_exact['__matchtype__'] = 'exact'

    df_candidates = df_candidates_exact.append(df_candidates_fuzzy, ignore_index=True)

    return df_candidates


def allpairs_diff(dfg1, dfg2, fuzzy_left_on, fuzzy_right_on, fun_diff):
    df_candidates = allpairs_candidates(dfg1, dfg2, fuzzy_left_on, fuzzy_right_on)

    idxSel = df_candidates['__matchtype__'] != 'exact'
    df_candidates.loc[idxSel,'__top1diff__'] = df_candidates[idxSel].apply(lambda x: fun_diff(x['__top1left__'], x['__top1right__']), axis=1)
    df_candidates.loc[~idxSel, '__top1diff__'] = 0

    return df_candidates


def merge_top1_diff(dfg1, dfg2, fuzzy_left_on, fuzzy_right_on, fun_diff, exact_left_on=[], exact_right_on=[], is_keep_debug=False):
    """

    Merges two dataframes with fuzzy top1 similarity

    Args:
        fuzzy* (str): single top1 similarity key

    """
    df_diff = allpairs_diff(dfg1, dfg2, fuzzy_left_on, fuzzy_right_on, fun_diff)
    has_duplicates = False

    df_diff = df_diff.groupby('__top1left__',group_keys=False).apply(lambda x: filter_group_min(x,'__top1diff__'))
    if df_diff.groupby('__top1left__').size().max()>1:
        warnings.warn('Top1 join for %s has duplicates' %fuzzy_left_on)
        has_duplicates = True
    dfjoin = dfg1.merge(df_diff, left_on=fuzzy_left_on, right_on='__top1left__')
    dfjoin = dfjoin.merge(dfg2, left_on='__top1right__', right_on=fuzzy_right_on)
    # todo: pass suffixes param?

    if not is_keep_debug:
        dfjoin = dfjoin[dfjoin.columns[~dfjoin.columns.str.startswith('__')]]

    return {'merged':dfjoin, 'differences':df_diff, 'duplicates':has_duplicates}


def merge_top1_diff_withblock(dfg1, dfg2, fuzzy_left_on, fuzzy_right_on, fun_diff, exact_left_on=[], exact_right_on=[], is_keep_debug=False):
    """

    Merges two dataframes with fuzzy top1 similarity. Includes exact keys blocking index

    Args:
        fuzzy* (str): single top1 similarity key
        exact* (list): list of exact join keys, acting as blocking index

    """
    has_duplicates = False

    def apply_gen_candidates_group(dfg):
        return pd.DataFrame(list(itertools.product(dfg['__top1left__'].values[0],dfg['__top1right__'].values[0])),columns=['__top1left__','__top1right__'])

    df_join_exact = dfg1.merge(dfg2, left_on=exact_left_on+[fuzzy_left_on], right_on=exact_right_on+[fuzzy_right_on], how='left')
    keysleft = dfg1[exact_left_on+[fuzzy_left_on]].drop_duplicates()
    keysright = dfg2[exact_right_on+[fuzzy_right_on]].drop_duplicates()
    keysleft = {tuple(x) for x in keysleft.values}
    keysright = {tuple(x) for x in keysright.values}
    values_left_exact = keysleft.intersection(keysright)
    values_left_fuzzy = keysleft.difference(keysright)

    df_keys_left_exact = pd.DataFrame(list(values_left_exact))
    df_keys_left_exact.columns = exact_left_on+['__top1left__']
    df_keys_left_exact['__top1right__']=df_keys_left_exact['__top1left__']
    df_keys_left_exact['__matchtype__'] = 'exact'

    df_keys_left_fuzzy = pd.DataFrame(list(values_left_fuzzy))
    df_keys_left_fuzzy.columns = exact_left_on+[fuzzy_left_on]

    df_keys_left = pd.DataFrame(df_keys_left_fuzzy.groupby(exact_left_on)[fuzzy_left_on].unique())
    df_keys_right = pd.DataFrame(dfg2.groupby(exact_right_on)[fuzzy_right_on].unique())
    df_keysets_groups = df_keys_left.merge(df_keys_right, left_index=True, right_index=True)
    df_keysets_groups.columns = ['__top1left__', '__top1right__']
    df_keysets_groups = df_keysets_groups.reset_index().groupby(exact_left_on).apply(apply_gen_candidates_group)
    df_keysets_groups = df_keysets_groups.reset_index(-1, drop=True).reset_index()
    df_keysets_groups = df_keysets_groups.dropna()

    df_candidates = df_keysets_groups[['__top1left__', '__top1right__']].drop_duplicates()
    df_candidates['__top1diff__'] = df_candidates.apply(lambda x: fun_diff(x['__top1left__'], x['__top1right__']), axis=1)
    df_candidates['__matchtype__'] = 'top1 left'

    df_diff = df_keysets_groups.merge(df_candidates, on=['__top1left__', '__top1right__'])

    has_duplicates = df_diff.groupby(exact_left_on+['__top1left__']).size().max()>1

    df_diff = df_diff.append(df_keys_left_exact, sort=False)
    df_diff['__top1diff__']=df_diff['__top1diff__'].fillna(0) # exact keys
    df_diff = df_diff.groupby(exact_left_on+['__top1left__'],group_keys=False).apply(lambda x: filter_group_min(x,'__top1diff__'))

    dfjoin = dfg1.merge(df_diff, left_on=exact_left_on+[fuzzy_left_on], right_on=exact_left_on+['__top1left__'])
    # todo: add exact join keys
    dfjoin = dfjoin.merge(dfg2, left_on=exact_left_on+['__top1right__'], right_on=exact_right_on+[fuzzy_right_on], suffixes=['','__right__'])

    dfjoin.columns
    dfjoin.groupby(exact_left_on+[fuzzy_left_on]).size()

    if not is_keep_debug:
        dfjoin = dfjoin[dfjoin.columns[~dfjoin.columns.str.startswith('__')]]

    return {'merged':dfjoin, 'differences':df_diff, 'duplicates':has_duplicates}

'''
multikey: want to merge left match onto right df
dont to numbers (non key) join until the very end
'''