import re
from string import ascii_letters, punctuation
from collections import defaultdict
from functools import wraps
import pandas as pd
import numpy as np
from tabulate import tabulate

import generic
from generic import mergedicts, strip, to_single_space, remove_non_ascii, fuzzyprep, integer, floating_point
from timeutils import Date, is_dayfirst

pd.set_option('display.max_colwidth', -1)

def testdf():
    return pd.DataFrame({
        'vals':[1,2,3,4,5, '$5.00'],
        '10':["Johnston Lemon Asset Management/ now New Potomac Partners LLC",2,3,4,5, '$15.00'],
        'col1':['a','','c','','',''],
        'col1.1':['','','','d','d',''],
        'col1.2':['','b','','','','d'],
        'col2.1':['1','%1.0',2,1.0,'$1,000,000,000.00','%1.00'],
        'col2.2':['hey','hey','hi','hi','hey','hi'],
        'name':['***john doe','hello','---','------------------','messy \t\t\t\t!!!!','messy \t\t\t\t!!!!'],
        'date':['12-01-2004','April 15, 2015','aug, 27 2017','9/15/15','',''],
        'date.1':['01-12-2004','April 15, 2015','aug, 27 2017','12/10/15','16/9/15','10/12/15']}).\
            rename(columns = {
                'col1.1' : 'col1',
                'col1.2' : 'col1',
                'date.1' : 'date'})

PUNCT_BLANK_ONLY = re.compile("(?:^[%s]+$|^$)" % punctuation)

class IsNotNumeric(Exception):
    pass

def dtypeobject(func):
    """Ensure series dtype is 'O' (object) before function execution."""
    @wraps(func)
    def inner(self, *args, **kwds):
        return func(self, *args, **kwds) if self.dtype == 'O' else self
    return inner

def maskhelper(func):
    @wraps(func)
    def inner(self, *args, **kwds):
        return func(self, *args, **kwds).fillna(False)
    return inner

def quickmapper(func):
    @wraps(func)
    def inner(self, *args, **kwds):
        return self.quickmap(lambda x: func(x, *args, **kwds))
    return inner

def checknumeric(func):
    @wraps(func)
    def inner(self, *args, **kwds):
        if self.dtype == 'O':
            raise IsNotNumeric("Values must be converted to a numeric data type (float, int) to ensure accurate comparisons.")
        return func(self, *args, **kwds)
    return inner

def series_functions():
    global dtypeobject, quickmapper, checknumeric, IsNotNumeric, _punctuation_or_blank_only

    #fruitful functions
    def unique(self):
        return np.unique(self.loc[self.notnull()])

    def quickdict(self, arg, *args, **kwds):
        """Create a dictionary containing the result of a function
        or dictionary mapped against the unique values in a series.

        If arg is a dictionary/other dict-like container, non matches
        will be left as is to ensure data fidelity.

        Parameters:
        ----------

        self : SubclassedSeries
        arg : callable or dict to parse series values. (dict, idict, function)
        [kwds] : keyword arguments for arg if arg is a function or callable.
        """
        return {s : ( arg(s, *args, **kwds) if callable(arg)
            else arg.get(s, s) ) for s in self.unique()}

    def quickmap(self, arg, *args, **kwds):
        return self.map(self.quickdict(arg, *args, **kwds))

    #validators
    def contains(self, pattern, **kwds):
        """Check self (astype(str)) for a given pattern.
        Parameters:
        ----------

        self : SubclassedSeries.
        pattern : String or compiled regex. (str, _sre.SRE_Pattern)
        [kwds] : Keyword arguments to be passed to self.str.contains.
        """
        return self.fillna('')\
            .to_ascii()\
            .astype(str)\
            .str.contains(pattern, **kwds)\
            .fillna(False)

    @checknumeric
    def gtzero(self):
        return self.to_numeric() > 0

    @checknumeric
    def ltzero(self):
        return self.to_numeric() < 0

    #modifiers
    _int = quickmapper(integer)
    _float = quickmapper(floating_point)
    to_text = quickmapper(to_single_space)
    to_ascii = quickmapper(remove_non_ascii)
    fuzzyprep = quickmapper(generic.fuzzyprep)

    @dtypeobject
    def clean(self, *args):
        """Strip whitespace and given punctuation from self.
        In addition, attempt to locate values that consist of punctuation
        ONLY and replace with np.nan.

        Parameters:
        ----------
        self : pd.Series.
        [args] : Additional strings to strip. str
        """
        args = list(args) + ['\t', ' ']
        BEGINNING_TRAILING = re.compile('|'.join(
            r'^(?=(?:{0}))|$(?<=(?:{0}))'.format((
                re.escape(i) if i in ['(', ')', '$', '\\'] else i
                    )) for i in args
                        ), re.I)

        mask = self.contains(BEGINNING_TRAILING)
        self = pd.Series(np.where(mask,
            self.str.strip(''.join(args)),
                self), index = self.index)
        self.loc[self.contains(PUNCT_BLANK_ONLY)] = np.nan
        return self

    def to_numeric(self, integer =  False, **kwds):
        """Convert values in self to a numeric data type.

        Parameters:
        ----------
        self : SubclassedSeries.
        [integer] : Flag specifying to convert as type int. bool
        """
        if integer:
            return self.astype(str)._int(**kwds).fillna(0)
        return self._float(**kwds)

    def to_datetime(self, fmt = False, disect = False, *args, **kwds):
        _ = self.quickmap(is_dayfirst).any()
        return self.quickmap(Date.parse,
            fmt = fmt,
            disect = disect,
            *args,
            **mergedicts(kwds, {'dayfirst' : _}))

    def disect_date(self, fields = [], **kwds):
        return pd.DataFrame(self.to_datetime(disect = True, **kwds).tolist())

    def modify(self, mask, ifvalue, elsevalue = None):
        """
        Modify values in a series using np.where.
        Values that meet the condition (mask) will be
        replaced with ifvalue.  All non-matching criteria
        will be replaced with elsevalue.

        Parameters
        ----------
        self : pd.Series
        mask : Boolean array. pd.Series
        ifvalue : Value used to modify current value. pd.Series, scalars

        [elsevalue] : self if used for non-matching criteria if not specified ("as is"). pd.Series
        """
        if elsevalue is None:
            elsevalue = self

        return pd.Series(
            np.where(mask, ifvalue, elsevalue),
                index = mask.index)

    for k,v in locals().items():
        setattr(pd.Series, k, v)

def dataframe_functions():

    def joinfields(df, fields = [], char = ' ', **kwds):
        if any(fields):
            kwds['items'] = fields
        joined = df.filter(**kwds)\
            .fillna('').astype(str)\
            .apply(lambda x: char.join(x), axis = 1)\
            .clean()

        return pd.Series([
            np.nan if not i else i for i in joined.values
                ], index = df.index).to_text()

    def dup_cols(self):
        _ = self.columns.str.replace(r'\.[\d]+(?:[\.\d]+)?$', '')
        __ = self.groupby(_, axis = 1).size()
        return __[__ > 1].index

    def mangle_cols(self):
        duplicates = self.dup_cols()
        if not any(duplicates):
            return self

        dmap = {}
        defmap = defaultdict(list)
        for i, f in enumerate(self.columns):
            if f in self.columns:
                defmap[f].append(i)
            else:
                dmap.update({i : f})

        for k, v in defmap.items():
            [dmap.update({
                i2 : "%s.%s.%s" % (k, i1, i2) if i1 > 0 else k
                    }) for i1,i2 in enumerate(v)]

        self.columns =  pd.Index([
            str(dmap[i]) for i,c in enumerate(self.columns)
                ])
        return self

    def combine_dup_cols(self, colname):
        """
        Fill gaps (populate nul values) in self[colname] with all over-lapping columns.
        """
        try:
            series = self[colname]
        except KeyError:
            series = pd.Series(None, index = self.index)

        if any(series.isnull()):
            for name in (col for col in self.columns if (colname in str(col) and col != colname)):
                series = series.combine_first(self[name])
        return series

    def filter_fields(self, **kwds): #filterfields
        return self.filter(**kwds).columns

    def drop_blank_fields(self):
        return self.dropna(how = 'all', axis = 1)

    def clean_fields(self):
        """Lower case and strip whitespace in column names.

        Parameters:
        ----------
        self : pd.DataFrame.
        """
        self.columns = pd.Index(map(strip, self.columns.str.lower()))
        return self

    def clean(self, *args):
        return self.mangle_cols()\
            .apply(pd.Series.clean, args = args)\
            .drop_blank_fields()\
            .dropna(how = 'all')

    def get_mapper(self, key_field, value_field):

        """Create a dictionary with the values from key_field
        as keys / value_field as values.

        Parameters:
        -----------
        self : SubclassedDataFrame
        key_field : Series name for dict keys.
        value_field : Series name for dict values.
        """
        if not value_field in self.columns or self.empty:
            return {}

        __ = self[value_field].unique()
        if any(__):
            return self.loc[self[value_field].isin(__)].dropna(
                subset = [value_field]).set_index(key_field)[value_field].to_dict()

    def prettify(self, headers = 'keys', **kwds):
        """
        Pretty print tabular data with tabulate.

        Parameters:
        ----------
        table : Python data structure; list, dict, pd.DataFrame, etc.

        headers : str
        kwds : tabluate keyword args.
        See https://pypi.python.org/pypi/tabulate for details.
        """
        if not kwds:
            kwds = {'tablefmt' : 'fancy_grid'}
        return tabulate(self, headers = headers, **kwds)

    def easy_agg(self, fields, flatten = True, sentinel = 'N/A', **kwds):
        """
        self.easy_aggregate('price', {'median_price':'mean'})

        Parameters:
        ----------
        self : pd.DataFrame
        fields : str, list
            String or list of fields to groupby
        flatten : bool
            Return re-indexed.
        kwds : dict, keyword arguments
            Field name to function key, value pairs (strings).
            If no kwds are provided, the default aggregation is value counts/columns/group.
        """
        if not isinstance(fields, list):
            fields = [fields]
        funcs = kwds
        if not funcs:
            funcs = 'count'
        result = self.groupby(fields).agg(funcs)
        if flatten:
            result.reset_index(inplace = True)
            result.columns = [
                '_'.join(field[::-1]).strip('_') if isinstance(field,tuple)
                else field for field in result.columns
                    ]

        return pd.DataFrame(result.fillna(sentinel))

    for k,v in locals().items():
        setattr(pd.DataFrame, k, v)

series_functions()
dataframe_functions()
