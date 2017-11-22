from __future__ import division
import os, gc
from collections import defaultdict
from functools import partial
import pandas as pd
from pandas.parser import CParserError

from generic import GenericBase, mergedicts, filterdict
from files import (ospath, File, Tabular,
                   ImproperCsvError, Folder,
                   joinpath, newfolder, getcsvkwds,
                   createcsv, readjson, writejson)
import dataframe
import record
from validation import Errorcatch

arrowfmt = "{} --> '{}'".format
getfiles = partial(Folder.listdir, files_only = True)

class SchemaNotRegistered(Exception):
    pass

class SchemaNotSpecified(Exception):
    pass

class Stage(GenericBase):
    CONFIGDIR = newfolder(ospath.dirname(__file__), 'config')
    SCHEMADIR = newfolder(CONFIGDIR, 'schema')
    FIELDSDIR = newfolder(CONFIGDIR, 'fieldsmap')
    FIELDSPATH = joinpath(FIELDSDIR, 'fieldsconfig.json')

    def __init__(self, schema_name, fieldspath = '', errorcatch = Errorcatch, **kwds):
        self.schema_name = schema_name
        super(Stage, self).__init__(schema_name)
        self.fieldspath = self.FIELDSPATH
        if fieldspath:
            self.fieldspath = fieldspath

        self.load()
        self.rowsdropped = defaultdict(int)
        self.report = {}
        self.normalized = 0
        self.counts_in = 0
        self.counts_out = 0
        self.errorcatch = errorcatch(logger = self._logger, schema_name = schema_name)

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
                kwds['fieldspath'] = fieldspath
            fieldsmap = cls.getconfig(kwds['fieldspath'])
        return Tabular.learnfields(df, fieldsmap, **kwds)

    @classmethod
    def conform(cls, df, name, learn = False, fieldspath = '', fields = [], fieldsmap = {}, **kwds):       
        if not fields:
            fields = cls.getfields(name)
        if learn:
            fieldsmap.update(
                cls.learnfields(df,
                    fieldspath = fieldspath,
                    name = name,
                    fields = fields,
                    **kwds))

        df.rename(columns = fieldsmap, inplace = True)
        return df.ix[:, fields]

    @property
    def fieldgroups(self):
        return {k : v for k, v in self.__dict__.items() if
                isinstance(v, list) and k.endswith('_fields')}

    @property
    def learnerkwds(self):
        return dict(fieldspath = self.fieldspath,
            fieldsmap = self.fieldsmap,
            table = self.schema_name,
            fields = self.fields,
            path = getattr(self, 'filename', ''))

    @property
    def discarded_fields(self):
        return [k for k, v in self.fieldsmap.items()
                if v not in self.fields]

    @property
    def fieldsmap(self):
        return self.getconfig(self.fieldspath)

    def load(self):
        __ = self.get_schemaconfig(self.schema_name)
        for k, v in __.items():
            if k == 'converters':
                v = {k2 : eval(v2) for k2, v2 in v.items()}
            setattr(self, k, v)

    def _conform(self, df, **kwds):
        return self.conform(df, self.schema_name,
            fieldsmap = self.fieldsmap,
            fields = self.fields, **kwds)

    def _getfunc(self, name):
        name = "to_{}".format(name.split('_')[0])
        if not hasattr(pd.Series, name):
            return
        return getattr(pd.Series, name)

    def droprows(self, df, thresh = None, conditions = {}):
        conditions.update({
            "Not enough data": {
            'mask' : lambda df: df.filter(items = self.fieldsmap.values()).lackingdata(thresh = thresh),
            'msg' : "%s rows will be dropped due to lack of data.  Valid data must have at least (%s) populated fields."
                }
                    })

        for reason, cnfg in conditions.items():
            mask = cnfg['mask'](df)
            count = mask.sum()
            if count > 0:
                if reason == 'Not enough data':
                    msg = cnfg['msg'] % (count, 'any' if not thresh else thresh)
                else:
                    msg = cnfg['msg'] % count
                self.warning(msg)
                self.rowsdropped[reason] += count
                df = df.loc[~(mask)]
        return df

    def patchmissing(self, df, exclude = []):
        return df.patchmissing(exclude = exclude)

    def to_string(self, df, **kwds):
        while True:
            try:
                return df.to_csv(index = False, header = False, **kwds)
            except UnicodeEncodeError as e:
                kwds.update(encoding = 'utf-8')

    def parse(self, df, formatdates = False, *args, **kwds):
        if kwds.get('learnfields'):
            self.learnfields(df, **self.learnerkwds)
        
        df = df.rename(columns = self.fieldsmap).manglecols()
        df = self.patchmissing(df.clean(*args))
        self.counts_in += df.counts(self.fields)

        _ = {}
        for name, fields in self.fieldgroups.items():
            flds = df.filterfields(items = fields).astype(str)
            func = self._getfunc(name)

            if name == 'datetime_fields' and formatdates:
                _['fmt'] = True

            if flds.any():
                if name == 'address_fields':
                    if hasattr(df, 'zip'):
                        self.info("Data contains address information.  Attempting to extract street, city, state, and zip.")
                        _z = df.zip.notnull()
                        df.loc[_z] = df.loc[_z].to_address()
                if not func:
                    continue

                self.info("Applying function '%s' to fields '%s'" % (func.func_name, ', '.join(flds)))
                df[flds] = df[flds].apply(func, **_)
                _ = {}

        return self.droprows(self._conform(df))

    def process(self, df, *args, **kwds):
        df = self.parse(df, *args, **kwds); print
        self.errorcatch.evaluate(df)
        self.counts_out += df.counts(self.fields).dropna()
        return df

    def evaluate(self):
        _ = self.counts_in - self.counts_out
        self.counts = pd.DataFrame({
            'counts_in' : self.counts_in,
            'counts_out' : self.counts_out.dropna(),
            'diff' : _})

        self.rowsdropped['Rows skipped (before and including column header)'] += self.fobj.rowsdropped
        total_rd = sum(self.rowsdropped.values())
        _ = self.counts['diff'].sum() - total_rd
        self.report.update(filename = self.filename,
                           fileproperties = self.fobj.properties,
                           totaldropped = total_rd,
                           rowsdropped = self.rowsdropped,
                           errorcatch = self.errorcatch,
                           correct = self.errorcatch.danger,
                           counts_per_field = self.counts,
                           rowstruncated = True if _ >= 1 else False)

        #reportfile = "{}_{}".format(ospath.basename(self.fobj.stem), reportfile) ##this may go soon
        #self.errorcatch.save(reportfile, counts = self.counts)  ##save report

    def processfile(self, path_or_fobj, outfile = '', outdir = 'processed', learn = True, *args, **kwds): #fobj being an object of type files.File (Csv, Excel, etc.)
        if isinstance(path_or_fobj, (str, basestring)):
            if 'converters' not in kwds:
                kwds.update(converters = self.converters)
            self.fobj = File.guess(path_or_fobj, **getcsvkwds(kwds))
        else:
            self.fobj = path_or_fobj

        if self.fobj.preprocessed:
            learn = False

        self.filename = self.fobj.basename()

        newfolder(outdir)
        if not outfile:
            outfile = self.fobj.get_outfile(outdir, self.filename)

        dfreader = self.fobj.dfreader(learnfields = learn, **self.learnerkwds)
        createcsv(outfile, self.fields)

        for df in dfreader:
            try:
                df = self.process(df, *args, **kwds)
                self.normalized += len(df)
                File.append(outfile, self.to_string(df))
            except CParserError:
                self.warning("Found rows with embedded delimiters in '%s'. Attemping to locate culprits." % self.filename)
                self.report['badlines'] = Csv.locate_badlines(self.fobj.path, delimiter = self.fobj.delimiter)
            except IncompleteExcelFile as e:
                self.warning(e.message)
                self.report['incomplete_excel'] = self.filename
        self.evaluate()
