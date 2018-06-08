# Databolt Smart Join

Easily join different datasets without writing custom code. Does fuzzy and time-series aware joins. For example you can quickly join similar but not identical stock tickers, addresses, names without manual processing.

## Sample Use

```python

import d6tjoin.top1
import d6tjoin.utils

# check join quality
>>> d6tjoin.utils.PreJoin([df1,df2],['id','date']).stats_prejoin()
  key left key right  all matched  inner  left  right  outer  unmatched total  unmatched left  unmatched right
0       id        id        False      0    10     10     20               20              10               10
1     date      date         True    366   366    366    366                0               0                0
2  __all__   __all__        False      0  3660   3660   7320             7320            3660             3660

# check find closest match for id
>>> result = d6tjoin.top1.MergeTop1(df1.head(),df2,fuzzy_left_on=['id'],fuzzy_right_on=['id'],exact_left_on=['date'],exact_right_on=['date']).merge()
>>>
>>> print(result['top1']['id'].head(2))
         date __top1left__ __top1right__  __top1diff__ __matchtype__
10 2010-01-01     e3e70682        3e7068             2     top1 left
34 2010-01-01     e443df78        443df7             2     top1 left
>>>
>>> print(result['merged'].head(2))
        date        id   val1 id_right  val1_right   val2
0 2010-01-01  e3e70682  0.020   3e7068       0.020  0.034
1 2010-01-01  f728b4fa  0.806   728b4f       0.806  0.849

```

## Features include
Enhances `pd.merge()` function with:
* Pre join diagnostics
* Fuzzy top1 similarity joins for strings, dates and numbers
	* Quickly join stock identifiers, addresses, names without manual processing

## Installation

Install `pip install git+https://github.com/d6t/d6tjoin.git`

Update `pip install --upgrade git+https://github.com/d6t/d6tjoin.git`

## Documentation

*  [Diagnosing join problems notebook](https://github.com/d6t/d6tjoin/blob/master/examples-prejoin.ipynb)
*  [Top1 similarity join examples notebook](https://github.com/d6t/d6tjoin/blob/master/examples-top1.ipynb)
*  [Official docs](http://d6tjoin.readthedocs.io/en/latest/index.html) - Detailed documentation for modules, classes, functions
