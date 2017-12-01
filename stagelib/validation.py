from __future__ import division
import re
from collections import defaultdict, OrderedDict
from functools import wraps
import pandas as pd
from generic import GenericBase, mergedicts
from files import df2excel
import dataframe

#validations
#Anything beinginning with a digit, decimal point or hyphen, can contain a decimal ponit, and must end with a digit, e.g. '.012', '1.12', '-12', '-5.00', '5.00'
re_INVALID_NUMERIC = re.compile(r'\s+|[a-z!\@\']+|^(?!\d+|-|\.)|\.$')

#Anything that does not start with a number greater than 0, followed by [3 digits][hyphen][2 digits][hyphen][2 digits] ('2017-11-12') OR '1970-01-01'.
re_INVALID_DATE  = re.compile(r'^(?:(?![1-9]\d{3}-\d{2}-\d{2}$).*)')
re_EPOCH = re.compile(r'^1970-01-01$')
re_INVALID_NAME = re.compile(r'^(?:[^a-z]+|^[^\s]+)$', re.I)

def notnull(field):
    def decorator(func):
        @wraps(func)
        def inner(df, *args, **kwds):
            return (df[field].notnull()) & (func(df, *args, **kwds))
        return inner
    return decorator

def checkstacked(**filters):
    def decorator(func):
        @wraps(func)
        def inner(df, **kwds):
            return func(df.filter(**filters).stack())\
                .unstack()\
                .fillna(False)\
                .reindex(df.index)
        return inner
    return decorator

def invalid_name(series):
    return series.contains(re_INVALID_NAME)
    
def name_too_many_chars(series, thresh = 70):
    return series.quickmap(len) >= thresh

def is_valid_name(series, **kwds):
    return ~(invalid_name(series)) & ~(name_too_many_chars(series, **kwds))

class Errorcatch(GenericBase):
    ADDITIONS = {}
    def __init__(self, *args, **kwds):
        schema_name = kwds.pop('schema_name', '')
        super(Errorcatch, self).__init__(schema_name, *args, **kwds)

    def __radd__(self, other):
        if other == 0:
            return self
        return self.__add__(other)

    def __add__(self, other):
        if not hasattr(self, 'table'):
            self.table = other.table
        else:
            self.table['count'] += other.table['count']

        self.length = getattr(self, 'length', 0) + other.length
        for k, v in other._errors.items():
            self._errors[k].append(v)
        return self

    @property
    def errors(self):
        return (self.table.level == 'ERROR') & (self.table['count'] > 0)

    @property
    def warnings(self):
        return self.table.level == 'WARNING'

    @property
    def danger(self):
        return self.errors.any()

    @property
    def checklist(self):
        __ = {"WARNING" : [], "ERROR" : []}
        for level, items in self.ADDITIONS.items():
            __[level].extend(items)
        return __

    def runchecks(self, df):
        __ = []
        for level, items in self.checklist.items():
            __.extend([
                mergedicts(item, level = level, mask = item['func'](df))
                for item in items
                    ])
        return __

    def parse(self, item, df):
        desc = item['desc']
        level = item['level']
        name = item['name']
        mask = item['mask']
        data = df.loc[mask].drop_blankfields()
        count = mask.sum()
        if count > 0:
            getattr(self, level.lower())("%s rows found where '%s'" % (count, desc))

        if not data.empty:
            self._errors[name] = data

        return {
            'shortname' : name,
            'description' : desc,
            'count' : count,
            'level' : ("Ok!" if count == 0 else level)
                }

    def _reconcile(self):
        __ = (self.warnings) &\
             (self.table['count'] / self.length >= 0.75)
        self.table.loc[__, 'level'] = 'ERROR'

    def evaluate(self, df):
        self.length = len(df)
        self._errors = defaultdict(pd.DataFrame)
        self.info("Checking for errors ....")
        self.table = pd.DataFrame([
            self.parse(item, df) for item in self.runchecks(df)
                ])
        
        self._reconcile()
        self.table.sort_values(by = ['count', 'level'],
                               ascending = False,
                               inplace = True)
        return self

    def save(self, outfile, **kwds):
        self.info("One moment please.  Saving errors to '%s'." % outfile)
        sheets = OrderedDict(mergedicts(self._errors, **kwds))
        df2excel(outfile, keepindex = True, **sheets)
        
    def showresults(self, **kwds):
        print; print self.table.prettify(**kwds)
