from __future__ import division
import os, gc
from collections import defaultdict, OrderedDict
from functools import partial
import pandas as pd

from generic import GenericBase, odict, mergedicts, filterdict
from files import (ospath, File, Tabular, Csv, Folder,
                   IncompleteExcelFile, joinpath,
                   newfolder, getcsvkwds,
                   createcsv, readjson, writejson)
import dataframe
from fieldlearner import learnfields
import record
from validation import Errorcatch

getfiles = partial(Folder.listdir, files_only = True)

class Summary(object):
    AGGREGATIONS = {}
    def __init__(self, gbfields):
        self.gbfields = gbfields
        self.uniq = set()

    def agg(self, df, where = lambda x: x):
        return df.loc[where].easyagg(self.gbfields, **self.AGGREGATIONS)

    def get_totals(self, aggd):
        self.total = pd.DataFrame(columns = aggd.columns)
        for field in aggd.columns:
            if field == self.gbfields:
                continue

            series = aggd[field]
            _ = field.split('_')[0]\
                .replace('count', 'sum')

            func = getattr(series, _)
            aggdict.update()
            self.total[field] = [func()]
        return aggd.append(totalsrow)

class SchemaNotRegistered(Exception):
    pass

class SchemaNotSpecified(Exception):
    pass

class Stage(GenericBase):
    CONFIGDIR = newfolder(ospath.dirname(__file__), 'config')
    SCHEMADIR = newfolder(CONFIGDIR, 'schema')
    FIELDSDIR = newfolder(CONFIGDIR, 'fieldsmap')
    FIELDSPATH = joinpath(FIELDSDIR, 'fieldsconfig.json')

    def __init__(self, schema_name, *args, **kwds):
        self.schema_name = schema_name
        super(Stage, self).__init__(schema_name, *args)
        self.fieldspath = kwds.pop('fieldspath', self.FIELDSPATH)
        errorcatcher = kwds.pop('errorcatcher', Errorcatch)
        self.load()
        self.rowsdropped = defaultdict(int)
        self.normalized = 0
        self.countsin = 0
        self.countsout = 0
        self.errors = []
        self.errorcatch = errorcatcher(logger = self._logger, schema_name = schema_name, *args)

    @classmethod
    def findschema(cls, schema_name):
        return joinpath(cls.SCHEMADIR, '{}.json'.format(schema_name))

    @classmethod
    def getconfig(cls, path):
        if not ospath.exists(path):
            writejson(path, {})
        return readjson(path)

    @classmethod
    def registerschema(cls, name, fields = [], datetime_fields = [], text_fields = [], numeric_fields = [], converters = {}):
        template = cls.getconfig(cls.getschema_namepath('template'))
        template['fields'] = fields
        template['datetime_fields'] = datetime_fields
        template['text_fields'] = text_fields
        template['numeric_fields'] = numeric_fields
        writejson(cls.getschema_namepath(name), template)
        print "'{}' registered".format(name)

    @classmethod
    def get_schemaconfig(cls, schema_name):
        path = cls.findschema(schema_name)
        if not ospath.exists(path):
            raise SchemaNotRegistered, "'{}' not found.  Please register desired configuration.".format(schema_name)
        return cls.getconfig(path)

    @classmethod
    def getfields(cls, schema_name):
        __ = cls.get_schemaconfig(schema_name)
        if 'fields' not in __:
            raise SchemaNotRegistered, "{} is missing 'fields' attribute, please re-register or edit config path '{}'.".format(schema_name, cls.findschema)
        return __['fields']

    @classmethod
    def learnfields(cls, df, usedefault = False, **kwds):
        fieldsmap = kwds.pop('fieldsmap', {})
        if not fieldsmap:
            if not kwds.get('fieldspath'):
                if usedefault:
                    fieldspath = cls.FIELDSPATH
                if not kwds.get('schema_name'):
                    raise SchemaNotSpecified, "Must specify a schema name."
                else:
                    fieldspath = joinpath(cls.FIELDSDIR, "{}_fieldsmap.json".format(kwds['schema_name']))
            else:
                fieldspath = kwds.pop('fieldspath')
            fieldsmap.update(cls.getconfig(fieldspath))
        return learnfields(df, fieldsmap,
                           table = kwds.pop('schema_name', ''), **kwds)

    @classmethod
    def conform(cls, df, schema_name = None, learn = False, fieldspath = '', fields = [], fieldsmap = {}, **kwds):       
        if not fields:
            if schema_name:
                fields = cls.getfields(schema_name)
            else:
                raise SchemaNotSpecified, "Must provide one of 'fields' or 'schema_name'."

        if learn:
            fieldsmap.update(
                cls.learnfields(df,
                    fieldspath = fieldspath,
                    schema_name = schema_name,
                    fields = fields,
                    **kwds))
            df.rename(columns = fieldsmap, inplace = True)
        return df.ix[:, fields]

    @classmethod
    def processfile(cls, path, *args, **kwds):
        return cls(*args)._processfile(path, **kwds)

    @staticmethod
    def to_csvstring(df, **kwds):
        while True:
            try:
                return df.to_csv(index = False, header = False, **kwds)
            except UnicodeEncodeError as e:
                kwds.update(encoding = 'utf-8')

    @property
    def fieldgroups(self):
        return {k : v for k, v in self.__dict__.items() if
                isinstance(v, list) and k.endswith('_fields')}

    @property
    def learnerkwds(self):
        return dict(fieldspath = self.fieldspath,
            schema_name = self.schema_name,
            fields = self.fields,
            path = getattr(self, 'filename', ''))

    @property
    def discarded_fields(self):
        return [k for k, v in self.fieldsmap.items()
                if v not in self.fields]

    @property
    def fieldskept(self):
        return filterdict(self.fieldsmap,
            self.discarded_fields, inverse = True).values()

    @property
    def fieldsmap(self):
        return self.getconfig(self.fieldspath)

    @property
    def header(self):
        return self.conform(
            pd.DataFrame({f : [] for f in self.fields}),
            fields = self.fields)

    def load(self):
        __ = self.get_schemaconfig(self.schema_name)
        for k, v in __.items():
            if k == 'converters':
                v = {k2 : eval(v2) for k2, v2 in v.items()}
            setattr(self, k, v)

    def countvalues(self, df):
        return df.fieldcounts(self.fieldskept)

    def _getfunc(self, name):
        name = "to_{}".format(name.split('_')[0])
        if not hasattr(pd.Series, name):
            return
        return getattr(pd.Series, name)

    def _conform(self, df, **kwds):
        return self.conform(df, fields = self.fields, **kwds)

    def droprows(self, df, thresh = None, conditions = {}):
        _ec = self.errorcatch.evaluate(df)
        _mapping = _ec.table.get_mapper('shortname', 'description')
        __ = [(k, v) for k, v in _ec._errors.items()
              if k in _ec.table.loc[_ec.errors, 'shortname'].values]

        _c = {
            name : {
            'mask' : lambda df: df.index.isin(data.index),
            'msg' : "Dropping %s rows where '%s'"
                } for name, data in __
                    }

        _c["Not enough data"] = {
            'mask' : lambda df: df.filter(items = self.fieldsmap.values()).lackingdata(thresh = thresh),
            'msg' : "%s rows will be dropped due to lack of data.  Valid data must have at least (%s) populated fields."
                }

        conditions.update(_c)
        for reason, _dict in conditions.items():
            mask, msg = _dict['mask'](df), _dict['msg']
            count = mask.sum()
            if count > 0:
                if reason == 'Not enough data':
                    msg = msg % (count, 'any' if not thresh else thresh)
                elif reason in self.errorcatch._errors.keys():
                    msg = msg % (count, _mapping[reason])
                else:
                    msg = msg % count

                self.warning(msg)
                self.rowsdropped[reason] += count
                df = df.loc[~(mask)]

        self.errors.append(_ec)
        return df

    def patchmissing(self, df, exclude = []):
        return df.patchmissing(exclude = exclude)

    def parse(self, df, formatdates = False, *args, **kwds):
        self.learnfields(df, self.fieldsmap,
                         **self.learnerkwds)

        self.info("Cleaning data (removing non-printable characters, unwanted punctuation, stripping whitespace)")
        df = self.patchmissing(  df.rename(columns = self.fieldsmap).clean(*args)  )
        self.info("Data has been cleaned.")
        self.countsin += self.countvalues(df)

        _kwds = {}
        for name, fields in self.fieldgroups.items():
            flds = df.filterfields(items = fields).astype(str)
            func = self._getfunc(name)

            if name == 'datetime_fields' and formatdates:
                _kwds['fmt'] = True

            if flds.any():
                df[flds] = df[flds].apply(func, **_kwds)
                self.info("Applying function '%s' to fields '%s'" % (  func.func_name,  ', '.join(flds)  ))
                _kwds = {}

        return self._conform(df)

    def process(self, df, *args, **kwds):
        df = self.parse(df, *args, **kwds)
        df = self.droprows(df)
        self.countsout += self.countvalues(df)
        return df

    def evaluate(self):
        self.total_rowsdropped = sum(self.rowsdropped.values())
        self.errorcatch = sum(self.errors)
        delattr(self, 'errors')
        self.unsafe = self.errorcatch.danger
        self.counts = pd.DataFrame({
            'countsin' : self.countsin,
            'countsout' : self.countsout,
            'diff' : self.countsin - self.countsout
                }).dropna()

        self.datalost = False
        _ = self.counts['diff'].sum() - self.rowsdropped["Not enough data"]
        if _ >= 1:
            self.datalost = True

        __ = getattr(self, 'fobj', None)
        properties = None
        if __:
            self.rowsdropped['Rows skipped'] += self.fobj.rowsdropped
            properties = self.fobj.properties

        self.report = odict(fileproperties = properties,
                            rowsdropped = self.rowsdropped,
                            valuecounts = self.counts)
        return self

    def _processfile(self, path_or_fobj, outfile = '', outdir = 'processed', learn = True, testbadlines = True, *args, **kwds):
        self.info("Start normalization - '%s'" % path_or_fobj)
        if isinstance(path_or_fobj, (str, basestring)):
            if 'converters' not in kwds:
                kwds.update(converters = self.converters)
            self.fobj = File.guess(path_or_fobj, **getcsvkwds(kwds))
        else:
            self.fobj = path_or_fobj

        self.filename = self.fobj.basename()
        if testbadlines and isinstance(self.fobj, Csv):
            self.info("Checking rows in '%s' for embedded delimiters." % self.filename)
            self.badlines = Csv.locate_badlines(self.fobj.path,
                                                delimiter = self.fobj.delimiter)

            self.badlinescount = len(self.badlines)
            if self.badlinescount >= 1:
                self.warning("%s lines containing embedded delimiters have been found in '%s'." % (self.badlinescount, self.filename))

        if not outfile:
            outfile = self.fobj.get_outfile(self.filename,
                                            dirname = newfolder(outdir))
        dfreader = self.fobj.dfreader()
        createcsv(outfile, self.fields)
        for df in dfreader:
            try:
                self.info("Currently processing '%s'" % self.filename)
                df = self.process(df, *args, **kwds)
                self.normalized += len(df)
                File.append(outfile, self.to_csvstring(df))
            except IncompleteExcelFile as e:
                self.warning(e.message)
                self.incomplete_excel = True
            gc.disable(); gc.collect()

        if hasattr(self.fobj, 'emptysheets'):
            self.emptysheets = self.fobj.emptysheets

        self.info("End normalization - '%s'" % path_or_fobj)
        self.info("%s rows normalized" % self.normalized); print
        return self.evaluate()

    def savebadlines(self, outfile = ''):
        Csv.savebadlines(self.fobj.path,
                         self.badlines,
                         outfile = outfile,
                         _fixed = self.header)

    def save_errors(self, outfile = 'reported_errors.xlsx', **kwds):
        if self.errorcatch._errors:
            self.errorcatch.save(outfile, _fixed = self.header, **kwds)
