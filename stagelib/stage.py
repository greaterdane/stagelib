from __future__ import division
import os, gc
from collections import defaultdict, OrderedDict
from functools import partial
import pandas as pd

from generic import GenericBase, filterdict, requiresattr, getkwds
from files import (ospath, File, Tabular, Csv, Folder,
                   IncompleteExcelFile, joinpath, newfolder,
                   createcsv, readjson, writejson)
import dataframe
from fieldlearner import learnfields
import record
from validation import Errorcatch

getfiles = partial(Folder.listdir, files_only = True)

def getfields(schema):
    return Stage.get_schemaconfig(schema).get('fields')

class Summary(object):
    AGGREGATIONS = {}
    def __init__(self, gbfields, unqiuefields = []):
        self.gbfields = [gbfields]
        self.uniquefields = uniquefields
        self.uniq = defaultdict(set)

    def agg(self, df, where = lambda x: x):
        return df.loc[where].easyagg(self.gbfields,
                                     **self.AGGREGATIONS)
    def add_uniques(self, df):
        for field in self.uniquefields:
            self.uniq["unique_%s" % field].update(df[field].unique())

    def get_totals(self, aggd):
        total = pd.DataFrame(columns = aggd.columns)
        for field in aggd.columns:
            if field not in self.gbfields:
                _ = field.split('_')[0]\
                   .replace('count', 'sum')
    
                total[field] = [getattr(aggd[field], _)()]

        total.assign(**{k: len(v) for k, v in self.uniq})
        return aggd.append(total)

class Stage(GenericBase):
    CONFIGDIR = newfolder(ospath.dirname(__file__), 'config')
    SCHEMADIR = newfolder(CONFIGDIR, 'schema')
    FIELDSDIR = newfolder(CONFIGDIR, 'fieldsmap')
    FIELDSPATH =  joinpath(FIELDSDIR, 'fields_map.json')

    def __init__(self, schema, *args, **kwds):
        self.schema = schema
        self.ready = False
        super(Stage, self).__init__(schema, *args)
        self.fieldspath = kwds.pop('fieldspath', self.FIELDSPATH)
        errorcatcher = kwds.pop('errorcatcher', None)
        if errorcatcher:
            self.errorcatch = errorcatcher(logger = self._logger,
                                           schema = schema, *args)
        self.load()
        self.rowsdropped = defaultdict(int)
        self.incomplete_excel = 0
        self.normalized = 0
        self.countsin = 0
        self.countsout = 0

    def __radd__(self, other):
        if other == 0:
            return self
        return self.__add__(other)

    def __add__(self, other):
        if hasattr(self, 'errorcatch'):
            self.errorcatch += other.errorcatch

        if not hasattr(self, 'issues'):
            self.issues = other.issues
        else:
            self.issues.extend(other.issues)

        self.counts = self.counts.append(other.counts)
        self.ready = other.ready
        
    @classmethod
    def findschema(cls, schema):
        return joinpath(cls.SCHEMADIR, '{}.json'.format(schema))

    @classmethod
    def getconfig(cls, path):
        if not ospath.exists(path):
            writejson(path, {})
        return readjson(path)

    @classmethod
    def registerschema(cls, name, fields = [], datetime_fields = [], text_fields = [], numeric_fields = [], converters = {}, **kwds):
        template = get_schemaconfig('template')
        map(lambda x: template.update({x[0] : x[1]}),
                                      kwds.items())
        template['fields'] = fields
        template['datetime_fields'] = datetime_fields
        template['text_fields'] = text_fields
        template['numeric_fields'] = numeric_fields
        writejson(cls.get_findschema(name), template)
        print "'{}' registered".format(name)

    @classmethod
    def get_schemaconfig(cls, schema):
        return cls.getconfig( cls.findschema(schema) )

    @classmethod
    def learnfields(cls, df, usedefault = False, **kwds):
        fieldsmap = kwds.pop('fieldsmap', {})
        if not fieldsmap:
            if not kwds.get('fieldspath'):
                fieldspath = cls.FIELDSPATH
            else:
                fieldspath = kwds.pop('fieldspath')

            fieldsmap.update(  cls.getconfig(fieldspath)  )

        fieldsmap = learnfields(df, fieldsmap,
                                table = kwds.pop('schema', ''), **kwds)
        writejson(fieldspath, fieldsmap)
        return fieldsmap

    @classmethod
    def conform(cls, df, schema = None, learn = False, fieldspath = '', fields = [], fieldsmap = {}, **kwds):       
        if not fields and schema:
            fields = getfields(schema) or []

        if learn:
            fieldsmap.update(
                cls.learnfields(df,
                    fieldspath = fieldspath,
                    schema = schema,
                    fields = fields,
                    **kwds))
            df.rename(columns = fieldsmap, inplace = True)
        return df.ix[:, fields]

    @classmethod
    def processfile(cls, path, *args, **kwds):
        return cls(*args)._processfile(path, **kwds)

    @property
    def fieldgroups(self):
        return {k : v for k, v in self.__dict__.items() if
                isinstance(v, list) and k.endswith('_fields')}

    @property
    def learnerkwds(self):
        return dict(fieldspath = self.fieldspath,
            schema = self.schema,
            fields = self.fields,
            path = getattr(self, 'filename', ''))

    @property
    def discarded_fields(self):
        return [k for k, v in self.fieldsmap.items()
                if v not in self.fields]

    @property
    def fieldskept(self):
        return list({
            v for k, v in self.fieldsmap.items()
            if v not in self.discarded_fields
                })

    @property
    def fieldsmap(self):
        return self.getconfig(self.fieldspath)

    @property
    def header(self):
        return self.conform(pd.DataFrame({f : [] for f in self.fields}),
                            fields = self.fields)
    def gatherissues(self):
        self.issues = []
        if hasattr(self, 'errorcatch'):
            _ = self.errorcatch.table\
                .filter(regex = 'desc|count')\
                .loc[self.errorcatch.errors]
    
            for d in _.to_dict(orient = 'records'):
                d.update(table = self.schema,
                         category = "PARSING_ERROR")
                self.issues.append(d)

        __ = dict(rowstruncated =  {'description' : "Data was been truncated during normalization.", 'category' : "DATA_TRUNCATED"},
                  incomplete_excel =  {'description' : "Excel sheet contains 65536 rows, data suspected incomplete.", 'category' : "INCOMPLETE_EXCELFILE"},
                  badlinescount =  {'description' : "Bad lines have been truncated in csv file.", 'category' : "BADCSV"})

        for k, v in __.items():
            attr = getattr(self, k, None)
            if attr:
                v.udpate(count = attr,
                         table = self.schema)
                self.issues.append(v)

    def load(self):
        for k, v in self.get_schemaconfig(self.schema).items():
            if k == 'converters':
                v = {k2 : eval(v2) for k2, v2 in v.items()}
            setattr(self, k, v)

    def countvalues(self, df):
        return df.fieldcounts(self.fieldskept)

    def _getfunc(self, name):
        return getattr(pd.Series,
                       'to_%s' % name.split('_')[0], None)

    def _conform(self, df, **kwds):
        return self.conform(df, fields = self.fields, **kwds)

    def droprows(self, df, thresh = None, conditions = {}):
        conditions["Not enough data"] = {'mask' : lambda df: df\
            .filter(items = self.fieldskept)\
            .lackingdata(thresh = thresh)}

        for reason, _dict in conditions.items():
            mask = _dict['mask'](df)
            count = mask.sum()
            if count > 0:
                msg = "Dropping %s rows where '%s'" % (count, reason)
                if reason == 'Not enough data':
                    msg =  "%s.  Data must have at least (%s) populated fields" % (msg, thresh)
                self.rowsdropped[reason] += count
                self.warning(msg)
                df = df.loc[~(mask)] #mask is NOT met.
        return df

    def patchmissing(self, df, exclude = []):
        return df.patchmissing(exclude = exclude)

    def parse(self, df, learnfields = True, **kwds):
        if learnfields:
            self.learnfields(df, self.fieldsmap,
                             endcheck = kwds.get('endcheck'),
                             **self.learnerkwds)

        df = self.patchmissing(
            df.rename(columns = self.fieldsmap)\
              .clean(nulls = kwds.get('nulls', []),
                     *kwds.get('omitchars', '')))

        self.countsin += self.countvalues(df)

        for name, fields in self.fieldgroups.items():
            func = self._getfunc(name)
            if func:
                if (name == 'datetime_fields' and kwds.get('formatdates')):
                    func = partial(func, fmt = True)

                f = df.filterfields(items = fields).astype(str)
                if f.any():
                    df[f] = df[f].apply(func)

        return self.droprows(self._conform(df))

    def process(self, df, *args, **kwds):
        conditions = kwds.pop('conditions', {})
        df = self.parse(df, *args, **kwds)
        if hasattr(self, 'errorcatch'):
            self.errorcatch.evaluate(df)
            errors = [(k, v) for k, v in self.errorcatch._errors.items()
                      if k in self.errorcatch.table.loc[self.errorcatch.errors, 'shortname'].values]

            conditions = {name : {'mask' : lambda df: df.index.isin(data.index)}
                          for name, data in errors}
            self.ready = self.errorcatch.ready
        return self.droprows(df, conditions = conditions)

    def evaluate(self):
        self.total_rowsdropped = sum(self.rowsdropped.values())
        self.counts = pd.DataFrame({
            'countsin' : self.countsin,
            'countsout' : self.countsout,
            'diff' : self.countsin - self.countsout
                }).dropna()

        self.rowstruncated = (self.counts['diff'].sum() -
                              self.rowsdropped["Not enough data"])
        if self.rowstruncated:
            self.ready = False

        if getattr(self, '_file', None):
            self.rowsdropped['Rows skipped'] += self._file.rowsdropped
            self.fileproperties = self._file.properties
        
        if hasattr(self, 'filename'):
            self.counts['filename'] = self.filename

        self.gatherissues()
        if not self.issues:
            self.ready = True
        return self

    def _processfile(self, path_or_file, *args, **kwds):
        self.info("START")
        self._file = path_or_file
        if isinstance(path_or_file, (str, basestring)):
            self._file = File.guess(path_or_file,
                                    **getkwds(kwds, pd.read_csv))

        self.filename = self._file.basename()
        if kwds.get('testbadlines') and isinstance(self._file, Csv):
            self.info("Checking rows in '%s' for embedded delimiters." % self.filename)
            self.badlines = Csv.locate_badlines(self._file.path,
                                                delimiter = self._file.delimiter)

            self.badlinescount = len(self.badlines)
            if self.badlinescount >= 1:
                self.warning("%s bad lines have been found in '%s'." % (self.badlinescount, self.filename))

        _ = newfolder( kwds.get('outdir', 'processed') )
        outfile = kwds.get('outfile')
        if not outfile:
            outfile = self._file.get_outfile(self.filename,
                                            dirname = _)
        createcsv(outfile, self.fields)
        for df in self._file.dfreader:
            try:
                df = self.process(df, *args, **kwds)
                self.countsout += self.countvalues(df)
                self.normalized += len(df)
            except IncompleteExcelFile as e:
                self.incomplete_excel += 1
            
            File.append(outfile, df.to_csvstring(header = False))
            self.info("%s rows written to %s" % (len(df), outfile))
            gc.disable(); gc.collect()

        self.emptysheets = getattr(self._file, 'emptysheets', None)
        self.info("END"); print
        return self.evaluate()

    @requiresattr('badlines')
    def savebadlines(self, outfile = ''):
        Csv.savebadlines(self._file.path,
                         self.badlines,
                         outfile = outfile,
                         _fixed = self.header)

    @requiresattr('errorcatch')
    def save_errors(self, outfile = 'reported_errors.xlsx', **kwds):
        if self.errorcatch._errors:
            self.errorcatch.save(outfile, _fixed = self.header, **kwds)
