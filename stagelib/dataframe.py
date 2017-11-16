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
        '10':["HELLO my Name is ____",2,3,4,5, '$15.00'],
        'col1':['a','','c','','','!!!!!!!!!!!!!!!!!!!!!!'],
        'col1.1':['','','','d','d',''],
        'col1.2':['^^^^^^^^','b','','','','d'],
        'col2.1':['1','1.0',2,1.0,'1,000,000,000.00','1.00'],
        'col2.2':['hey','hey','hi','hi','hey','hi'],
        'name':['***john doe','hello','---','------------------','messy \t\t\t\t??','messy \t\t\t\t$$'],
        'date':['12-01-2004','April 15, 2015','aug, 27 2017','9/15/15','???????????','?'],
        'date.1':['01-12-2004','April 15, 2015','aug, 27 2017','12/10/15','16/9/15','10/12/15']}).\
            rename(columns = {
                'col1.1' : 'col1',
                'col1.2' : 'col1',
                'date.1' : 'date'})

BAD_CHARACTERS = ['$', '\\', '=', '"', "'", ' ', '\t', '?', '*']
START_END = r'^(?=(?:{0}))|$(?<=(?:{0}))'.format #look ahead for a character/ look behind for a character
re_PUNCTBLANK_ONLY = re.compile("(?:^[%s]+$|^$)" % punctuation)
re_START_END_TRASH = re.compile('|'.join(START_END((
        re.escape(i) if i in ['(', ')', '$', '\\', '?', '^', '*'] else i
            )) for i in BAD_CHARACTERS), re.I)

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
        self = self.to_numeric(
            force = kwds.pop('force', True),
            integer = kwds.pop('integer', False))
        if self.dtype == 'O': #if for some reason values did not get converted properly
            raise IsNotNumeric("Values must be converted to a numeric data type (float, int) to ensure accurate comparisons.")
        return func(self, *args, **kwds)
    return inner

def series_functions():
    global dtypeobject, quickmapper, checknumeric, IsNotNumeric, re_PUNCTBLANK_ONLY, re_START_OR_END_TRASH, BAD_CHARACTERS

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

        self : pd.Series.
        pattern : String or compiled regex. (str, _sre.SRE_Pattern)
        [kwds] : Keyword arguments to be passed to self.str.contains.
        """
        return self.fillna('')\
            .to_ascii()\
            .astype(str)\
            .str.contains(pattern,
                na = False, **kwds)

    @checknumeric
    def gtzero(self):
        return self > 0

    @checknumeric
    def ltzero(self):
        return self < 0

    #modifiers
    _int = quickmapper(integer)
    _float = quickmapper(floating_point)
    _strip = quickmapper(strip)
    to_text = quickmapper(to_single_space)
    to_ascii = quickmapper(remove_non_ascii)
    to_fuzzy = quickmapper(generic.fuzzyprep)

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
        mask = (self.contains(re_START_END_TRASH)) |\
               (self.astype(str).str.endswith(args))|\
               (self.astype(str).str.startswith(args))

        args = BAD_CHARACTERS + list(args)
        self = self.modify(mask, self._strip(*args))
        self.loc[self.contains(re_PUNCTBLANK_ONLY)] = np.nan
        return self

    def to_numeric(self, integer =  False, force = False, **kwds):
        """Convert values in self to a numeric data type.

        Parameters:
        ----------
        self : SubclassedSeries.
        [integer] : Flag specifying to convert as type int. bool
        """
        kwds = mergedicts(force = force, **kwds)
        if integer:
            return self.fillna('').astype(str)._int(**kwds)
        return self._float(**kwds)

    def isnull(self):
        return (super(pd.Series, self).isnull()) | (self.astype(str) == '')

    def notnull(self):
        return (super(pd.Series, self).notnull()) & (self.astype(str) != '')

    def unique(self):
        return super(pd.Series, self.loc[self.notnull()]).unique()

    def to_datetime(self, fmt = False, disect = False, force = False, *args, **kwds):
        _ = self.quickmap(is_dayfirst).any()
        return self.quickmap(Date.parse,
            fmt = fmt,
            force = force,
            disect = disect,
            *args,
            **mergedicts(kwds, {'dayfirst' : _}))

    def disectdate(self, fields = [], **kwds):
        return pd.DataFrame(self.to_datetime(disect = True, fields = fields, **kwds).tolist())

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
    def rows_containing(self, pattern, fields = [], **kwds):
        if not fields:
            fields = self.columns

        return np.column_stack([
            self[field].contains(pattern, **kwds) for field in fields
                ]).any(axis = 1)

    def joinfields(self, fields = [], char = ' ', **kwds):
        if not kwds:
            kwds['items'] = fields
            if not fields:
                raise Exception, "If no filters are specfied in **kwds, please provide a list of fields."

        joined = self.filter(**kwds)\
            .fillna('').astype(str)\
            .apply(lambda x: char.join(x), axis = 1)._strip()

        return pd.Series([
            np.nan if not i else i for i in joined.values
                ], index = self.index).to_text()

    def dupcols(self):
        _ = pd.Index([
            re.sub(r'\.[\d]+(?:[\.\d]+)?$', '', str(field)) for field in self.columns
                ])
        __ = self.groupby(_, axis = 1).size()
        return __[__ > 1].index

    def manglecols(self):
        duplicates = self.dupcols()
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

    def combine_dupcols(self, field):
        """
        Fill gaps (populate nul values) in self[field] with all over-lapping columns.
        """
        try:
            series = self[field]
        except KeyError:
            series = pd.Series(None, index = self.index)

        if any(series.isnull()):
            for name in (col for col in self.columns if (field in str(col) and col != field)):
                series = series.combine_first(self[name])
        return series

    def patchmissing(self, exclude = []):
        fields = [field for field in self.dupcols() if field not in exclude]
        for field in fields:
            self[field] = self.combine_dupcols(field)
        return self

    def filterfields(self, **kwds):
        return self.filter(**kwds).columns

    def drop_blankfields(self):
        return self.dropna(how = 'all', axis = 1)

    def cleanfields(self):
        """Lower case and strip whitespace in column names.

        Parameters:
        ----------
        self : pd.DataFrame.
        """
        self.columns = pd.Index(map(strip, self.columns.str.lower()))
        return self

    def clean(self, *args): ##ONLY for use on entire dataframe
        return self.manglecols()\
            .apply(pd.Series.clean, args = args)\
            .drop_blankfields()
            
    def lackingdata(df, thresh = None):
        idx = df.dropna(how = 'all', thresh = thresh).index
        return ~(df.index.isin(idx))

    def get_mapper(self, keys, values, where = None):

        """Create a dictionary with the values from field 'keys'
        as dict keys / values from field 'values' as values.

        Parameters:
        -----------
        self : pd.DataFrame
        keys : Field name to use for dict keys.
        values : Field name to use for dict values.
        """
        try:
            mask = self[values].notnull()
            if where is not None:
                mask = mask & where

            if mask.any():
                __ = self.loc[mask].set_index(keys)
                return __[values].to_dict()
            return {}
        except KeyError:
            return {}

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

    def easyagg(self, fields, flatten = True, sentinel = 'N/A', **kwds):
        """
        self.easyagg('price', {'median_price':'mean'})

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
