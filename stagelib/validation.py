from __future__ import division
import re
from collections import defaultdict
from functools import wraps
import pandas as pd
from generic import GenericBase, mergedicts
from files import df2excel
import dataframe

#validations
#Anything beinginning with a digit, decimal point or hyphen, can contain a decimal ponit, and must end with a digit, e.g. '.012', '1.12', '-12', '-5.00', '5.00'
re_INVALID_NUMERIC = re.compile(r'\s+|[a-z]+|(?:^(?!(?:-|\.)?\d+(?:$|\.\d+$)))')

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
    additions = {}
    def __init__(self, schema_name = '', logger = None, **kwds):
        self.schema_name = schema_name
        self._logger = logger
        super(Errorcatch, self).__init__(schema_name, **kwds)
        self.erroneous = defaultdict(pd.DataFrame)
        self.table = pd.DataFrame({'count' : [0], 'description': [''], 'level': ['']})
        self.length = 0

    def __radd__(self, other):
        if other == 0:
            return self
        return self.__add__(other)

    def __add__(self, other):
        self.table['count'] += other.table['count']
        self.length += other.length
        for k, v in other.erroneous.items():
            self.erroneous[k].append(v)
        return self

    @property
    def errors(self):
        return self.table.level == 'ERROR'

    @property
    def warnings(self):
        return self.table.level == 'WARNING'

    @property
    def danger(self):
        warnings_now_errors = (self.warnings) & (self.table['count'] / self.length >= 0.75)
        errors = (self.errors) & (self.table['count']) > 0
        self.table.loc[warnings_now_errors, 'level'] = 'ERROR'
        return errors.any()

    @property
    def checklist(self):
        __ = {"WARNING" : [], "ERROR" : []}
        for level, items in self.additions.items():
            __[level].extend(items)
        return __

    def runchecks(self, df):
        __ = []
        self.length += len(df)
        for level, items in self.checklist.items():
            __.extend([
                mergedicts(item, level = level, mask = item['func'](df))
                for item in items
                    ])
        return __

    def locate(self, mask, name, df):
        return df.loc[mask].drop_blankfields()

    def parse(self, item, df):
            desc = item['desc']
            level = item['level']
            name = item['name']
            data = self.locate(item['mask'], name, df)
            count = len(data)
            if count > 0:
                getattr(self, level.lower())("%s rows found where '%s'" % (count, desc))

            if not data.empty:
                self.erroneous[name] = data.assign(index = data.index)

            return {
                'description' : desc,
                'count' : count,
                'level' : ("Ok!" if count == 0 else level)
                    }

    def evaluate(self, df):
        self.info("Checking for errors ....")
        self.table = pd.DataFrame([self.parse(item, df) for item in self.runchecks(df)])
        return self

    def save(self, outfile, **kwds):
        self.info("Saving erroneous data to disk, this may take a moment ....")
        error_report = mergedicts(self.erroneous, totals = self.table, **kwds)
        df2excel(outfile, **error_report)
        for k, v in self.erroneous.items():
            self.erroneous[k] = v.index
        
    def showresults(self, sortkeys = ['count', 'level'], **kwds):
        print; print self.table.sort_values(by = sortkeys, ascending = False).prettify()
