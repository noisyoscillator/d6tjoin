# Databolt Smart Join

Easily join different datasets without writing custom code. Does fuzzy and time-series aware joins. For example you can quickly join similar but not identical stock tickers, addresses, names without manual processing.

## Sample Use

```python

import d6tjoin.smart_join

>>> sj = d6tjoin.utils.PreJoin([df1, df2], [['BARRA_PIT_CUSIP','cusip'],['date','Date']])

# check join quality
>>> sj.stats_prejoin()

          key left key right  all matched  inner  left  right  outer  unmatched total  unmatched left  unmatched right
0  BARRA_PIT_CUSIP     cusip        False      0   628  12692  13320            13320             628            12692
1             date      Date        False      1     2      2      3                2               1                1
2          __all__   __all__        False      0  1252  22975  24227            24227            1252            22975

>>> sj = d6tjoin.utils.FuzzyJoinTop1([df1, df2],fuzzy_keys= [['BARRA_PIT_CUSIP','cusip'],['date','Date']])

>>> df_merge_top1 = sj.run_match_top1('BARRA_PIT_CUSIP')
>>> df_merge_top1['table'].head()
      __top1left__ __top1right__  __top1diff__ __match type__
60731  b'19416210'     194162103             3      top1 left
36934  b'20588710'     205887102             3      top1 left
20183  b'27864210'     278642103             3      top1 left
38268  b'54042410'     540424108             3      top1 left
4732   b'H1467J10'     H1467J104             3      top1 left

```

## Features include
Enhances `pd.merge()` function with:
* Pre join diagnostics
* Fuzzy top1 similarity joins for strings, dates and numbers
	* Quickly join stock identifiers, addresses, names without manual processing

[SmartJoin Examples notebook](https://github.com/d6t/d6tjoin/blob/master/examples-smartjoin.ipynb)

## Installation

Install `pip install git+https://github.com/d6t/d6tjoin.git`

Update `pip install --upgrade git+https://github.com/d6t/d6tjoin.git`

## Documentation

*  [SmartJoin Examples notebook](https://github.com/d6t/d6tjoin/blob/master/examples-smartjoin.ipynb) - Demonstrates SmartJoin usage
*  [Official docs](http://d6tjoin.readthedocs.io/en/latest/index.html) - Detailed documentation for modules, classes, functions
*  [www.databolt.tech](https://www.databolt.tech/index-combine.html) - Web app if you don't want to code
