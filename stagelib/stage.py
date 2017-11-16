from __future__ import division

import os, gc
from collections import defaultdict
from functools import partial
import pandas as pd
from pandas.parser import CParserError

from generic import GenericBase, mergedicts, filterdict
from fileIO import (OSPath, File, Tabular,
                    ImproperCsvError, Folder,
                    writedata, appendData,
                    mkpath, mkdir, getcsvkwds,
                    from_json, to_json)
import dataframe
import record

arrowfmt = "{} --> '{}'".format
getfiles = partial(Folder.listdir, files_only = True)

class SchemaNotRegistered(Exception):
    pass

class SchemaNotSpecified(Exception):
    pass

class Itemcounter(object):
    pass

class Errorcheck(object):
    ##'checklist' will contain a list of dictionaries for each key (WARNING, ERROR).
    ##Each dictionary will consist of the following fields.
        ## 'name' : Name of warning or error.
        ## 'desc' : Verbose description of warning or error.
        ## 'func' : Function used to check the warning or error.  Can be defined in list or elsewhere.
    ##We can let items in WARNING slide, items in ERROR will prevent data from going into the database or prodution environment.

    checklist = {
        "WARNING" : [], #Can have some but not all.
        "ERROR" : [] #Cannot have any.
            }

    def __init__(self):
        self.erroneous = defaultdict(pd.DataFrame)
        self.table = pd.DataFrame()
        self.length = 0

    def __radd__(self, other):
        self.table = other.table
        return self

    def __add__(self, other):
        self.table['count'] += other.table['count']
        self.length += other.length
        for k, v in other.erroneous:
            self.erroneous[k].append(v)
        return self

    @classmethod
    def finderrors(cls, df):
        return cls().evaluate(df)

    @property
    def errors(self):
        return self.table.level == 'ERROR'

    @property
    def warnings(self):
        return self.table.level == 'WARNING'

    @property
    def danger(self):
        return (
            any(self.table.loc[self.warnings] / self.length == 0.75) or
            any(self.table.loc[self.errors, 'count'] > 0)
                )

    def runchecks(self, df):
        __ = []
        self.length += len(df)
        for level, items in self.checklist.items():
            __.extend([
                mergedicts(item, level = level, mask = item['func'](df))
                for item in items
                    ])
        return __

    def locatedata(self, mask, name, df):
        __ = df.loc[mask].drop_blankfields()
        if not __.empty:
            self.erroneous[name].append(__)
        return __

    def parse(self, item, df):
            __ = self.locatedata(item['mask'], item['name'], df)
            count = len(__)
            return {
                'description' : item['desc'],
                'count' : count,
                'level' : ("Ok!" if count == 0 else item['level'])
                    }

    def evaluate(self, df):
        items = []
        for item in self.runchecks(df):
            items.append(self.parse(item, df))
        self.table = pd.DataFrame(items)
        return self

    def showresults(self, sortkeys = ['level', 'count'], **kwds):
        print self.table.sort_values(by = sortkeys, ascending = False).prettify()

class Stage(GenericBase):
    CONFIGDIR = mkdir(OSPath.dirname(__file__), 'config')
    SCHEMADIR = mkdir(CONFIGDIR, 'schema')
    FIELDSDIR = mkdir(CONFIGDIR, 'fieldsmap')
    FIELDSPATH = mkpath(FIELDSDIR, 'fieldsconfig.json')

    def __init__(self, schema_name, fieldspath = '', errorchecker = Errorcheck, **kwds):
        self.schema_name = schema_name
        super(Stage, self).__init__(schema_name)
        self.fieldspath = self.FIELDSPATH
        if fieldspath:
            self.fieldspath = fieldspath

        self.load()
        self.rowsdropped = defaultdict(int)
        self.report = {}
        self.normalized = 0
        self.errorchecker = errorchecker
        self.errors = []

    @classmethod
    def findschema(cls, schema_name):
        return mkpath(cls.SCHEMADIR, '{}.json'.format(schema_name))

    @classmethod
    def getconfig(cls, path):
        if not OSPath.exists(path):
            to_json(path, {})
        return from_json(path)

    @classmethod
    def registerschema(cls, name, fields = [], datetime_fields = [], text_fields = [], numeric_fields = [], converters = {}):
        template = cls.getconfig(cls.getschema_namepath('template'))
        template['fields'] = fields
        template['datetime_fields'] = datetime_fields
        template['text_fields'] = text_fields
        template['numeric_fields'] = numeric_fields
        to_json(cls.getschema_namepath(name), template)
        print "'{}' registered".format(name)

    @classmethod
    def get_schemaconfig(cls, schema_name):
        path = cls.findschema(schema_name)
        if not OSPath.exists(path):
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
                    fieldspath = mkpath(cls.FIELDSDIR, "{}_fieldsmap.json".format(kwds['schema_name']))
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

    @classmethod
    def processfile(cls, path, filekwds = {}, *args, **kwds):
        outdir = kwds.pop('outfile'); outdir = kwds.pop('outdir')
        return cls(*args, **kwds).parsefile(path, outdir = outdir, outfile = outfile, **filekwds)

    @classmethod
    def processfiles(cls, dirname, filepattern = '', outdir = '', outfile = '', dataset = False, *args, **kwds):
        filenames = getfiles(dirname, pattern = filepattern)
        filekwds = kwds.pop('filekwds', {})
        reports = defaultdict(list)
        try:
            if dataset:
                stager = cls(*args, **kwds)
                for filename in filenames:
                    reports[filename].append(
                        stager.parsefile(filename,
                            outdir = outdir,
                            outfile = outfile,
                            **filekwds))
            else:
                for filename in filenames:
                    reports[filename].append(cls.processfile(path,
                        filekwds = filekwds,
                        outdir = outdir,
                        outfile = outfile,
                        *args, **kwds))

        except ImproperCsvError as e:
            self.info("Encountered some bad data in '%s'.  This issue needs to be handled immediately." % filename)
            reports['badcsvs'] = {filename : reports.pop(filename)}
        return reports

    @property
    def fieldgroups(self):
        return {k : v for k, v in self.__dict__.items() if
                isinstance(v, list) and k.endswith('_fields')}

    @property
    def learnerkwds(self):
        return dict(fieldspath = self.fieldspath,
            table = self.schema_name,
            fields = self.fields)

    @property
    def unusedfields(self):
        return [k for k, v in self.fieldsmap.items() if v not in self.fields]

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
            'mask' : lambda df: df.lackingdata(thresh = thresh),
            'msg' : "%s rows will be dropped due to lack of data.  Valid data must have at least %s populated fields."
                }})

        for reason, cnfg in conditions.items():
            mask = cnfg['mask'](df)
            count = mask.sum()
            if count > 0:
                if reason == 'Not enough data':
                    msg = cnfg['msg'] % (count, 1 if not thresh else thresh)
                else:
                    msg = cnfg['msg'] % count
                self.warning(msg)
                self.rowsdropped[reason] += count
                df = df.loc[~(mask)] #Where condition (an undesirable one) is NOT met.
        return df

    def patchmissing(self, df, exclude = []):
        return df.patchmissing(exclude = exclude)

    def to_string(self, df, **kwds):
        while True:
            try:
                return df.to_csv(index = False, header = False, **kwds)
            except UnicodeEncodeError as e:
                kwds.update(encoding = 'utf-8')

    def parse(self, df, to_strip = '', strip_default = True, learnfields = False, **kwds):
        if learnfields:
            self.learnfields(df, fieldsmap = self.fieldsmap, **self.learnerkwds)

        df.rename(columns = self.fieldsmap, inplace = True)
        defaultchars = ('\\', '=', '|')
        if not strip_default:
            defaultchars = tuple()

        to_strip = [to_strip] + list(defaultchars)
        df = self.patchmissing(df.clean(*to_strip),
            exclude = kwds.pop('exclude', []))

        self.info('The following characters have been stripped (where present): "%s"' % ''.join(to_strip))

        for name, fields in self.fieldgroups.items():
            flds = df.filterfields(items = fields).astype(str)
            func = self._getfunc(name)

            if flds.any():
                if name == 'address_fields':
                    self.info("Data contains address information.  Attempting to extract street, city, state, and zip.")
                    df = df.to_address()

                if not func:
                    continue

                self.info("Applying function '%s' to fields '%s'" % (func.func_name, ', '.join(flds)))
                df[flds] = df[flds].apply(func)

        df = self.droprows(self._conform(df))
        #self.errors.append(self.errorchecker.finderrors(df))
        return df

    def parsefile(self, path, outfile = '', outdir = 'processed', learnfields = True, **kwds):
        mkdir(outdir)
        if 'converters' not in kwds:
            kwds.update(converters = self.converters)

        _file = File.guess(path, **getcsvkwds(kwds))
        self.filename = _file.basename()

        if not outfile:
            stem = OSPath(self.filename).stem
            outfile = "{}_output.csv".format(stem)

        outfile = mkpath(outdir, outfile)
        writedata(outfile, ','.join(self.fields) + "\n")

        if not learnfields:
            kwds.update(learnfields = True)

        dfreader = _file.dfreader(fieldsmap = self.fieldsmap,
            learnfields = learnfields, **self.learnerkwds)

        for df in dfreader:
            try:
                df = self.parse(df, **filterdict(kwds, 'strip|learn'))
                self.normalized += len(df)
                appendData(outfile, self.to_string(df))
            except CParserError:
                self.warning("%s contains embedded delimters that are not escaped.  Attemping to locate culprit rows." % self.filename)
                badlines = Csv.getbadlines(_file.path,
                    delimiter = _file.delimiter)
                try:
                    raise ImproperCsvError
                finally:
                    return badlines

        self.info("Normalized data written to '%s'" % outfile)
        self.rowsdropped['Rows skipped (before and including column header)'] += _file.rowsdropped

        __ = _file.properties
        __.udpate(rows_normalized = self.normalized)
        errors = sum(self.errors)
        return mergedicts(self.report,
            fileproperties = __,
            rowsdropped = self.rowsdropped,
            errors = errors,
            valid = errors.danger)
