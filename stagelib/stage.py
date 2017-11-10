import os, gc
import pandas as pd

from generic import GenericBase, mergedicts, filterdict
from fileIO import (OSPath, File, Tabular,
                    writedata, from_json, to_json,
                    appendData, mkpath, mkdir, getcsvkwds)
import dataframe
import record

arrowfmt = "'{}' --> '{}'.".format

class SchemaNotRegistered(Exception):
    pass

class SchemaNotSpecified(Exception):
    pass

# Safety net.!!
class Itemcounter(object):
    pass

class SafetyCheck(object):
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
        self.errors = defaultdict(pd.DataFrame)
        self.table = pd.DataFrame()

    def __add__(self, df_or_other):
        if isinstance(df_or_other, type(self)):
            df_or_other = df_or_other.table
        __ = self.createtable(df_or_other)
        if self.table.empty:
            self.table = __
        else:
            self.table['count'] += __['count']
        return self

    @property
    def errors(self):
        return self.table.level == 'ERROR'

    @property
    def warnings(self):
        return self.table.level == 'WARNING'

    def setup(self, df):
        __ = []
        for level, items in self.checklist.items():
            __.extend([mergedicts(item,
                level = level,
                mask = item['func']()) for item in items])
        return __

    def locate(self, mask, name, df):
        __ = df.loc[mask].drop_blankfields()
        if not __.empty:
            self.errors[name] = self.errors[name].append(__)
        return __

    def createtable(self, item, df):
            __ = self.locate(item['mask'], item['name'], df)
            count = len(__)
            return {
                'description' : item['desc'],
                'count' : count,
                'level' : ("Ok!" if count < 1 else item['level'])
                    }

class Stage(GenericBase):
    CONFIGDIR = mkdir(OSPath.dirname(__file__), 'config')
    SCHEMADIR = mkdir(CONFIGDIR, 'schema')
    FIELDSDIR = mkdir(CONFIGDIR, 'fieldsmap')
    FIELDSPATH = mkpath(FIELDSDIR, 'fieldsconfig.json')

    def __init__(self, schema_name, fieldspath = '', **kwds):
        self.schema_name = schema_name
        self.fieldspath = self.FIELDSPATH
        if fieldspath:
            self.fieldspath = fieldspath

        self.load()
        super(Stage, self).__init__()

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
        schema_name = kwds.pop('table', '')
        fieldspath = kwds.pop('fieldspath', '')
        fieldsmap = kwds.pop('fieldsmap', {})
        if not fieldsmap:
            if not fieldspath:
                if usedefault:
                    fieldspath = cls.FIELDSPATH
    
                if not schema_name:
                    raise SchemaNotSpecified, "Either a 'fieldsmap' path or schema_name name needs to be specified."
                else:
                    fieldspath = mkpath(cls.CONFIGDIR, "%s_fieldsmap.json".format(schema_name))
                    if OSPath.notexists(fieldspath):
                        to_json({}, fieldspath)
            fieldsmap = cls.getconfig(fieldspath)

        fieldsmap = Tabular.learnfields(df, fieldsmap,
            fieldspath = fieldspath, table = schema_name, **kwds)

        to_json(fieldspath, fieldsmap)
        return fieldsmap

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
        return df.rename(
            columns = fieldsmap
                ).ix[:, fields]

    @classmethod
    def processfile(cls, path, prsrkwds = {}, *args, **kwds):
        return cls(*args, **kwds).parsefile(path, **prsrkwds)

    @classmethod
    def processfiles(cls, dirname, lookfor = '', outdir = '', dataset = False, *args, **kwds):
        pathnames = Folder.listdir(dirname,
            pattern = lookfor,
            files_only = True)

        prsrkwds = getcsvkwds(kwds)
        kwds = filterdict(kwds, inverse = True, *prsrkwds.keys())
        if dataset:
            properties = []
            prsr = cls(*args, **kwds)
            for path in pathnames:
                properties.append(prsr.parsefile(path, outdir = outdir, **prsrkwds))
        else:
            for path in pathnames:
                properties.append(cls.processfile(path, fieldspath, prsrkwds = prsrkwds, *args, **kwds))
        return properties

    @property
    def fieldgroups(self):
        return {k : v for k, v in self.__dict__.items() if
                isinstance(v, list) and k.endswith('_fields')}

    @property
    def learnerkwds(self):
        return dict(fieldsmap = self.fieldsmap,
            fieldspath = self.fieldspath,
            table = self.schema_name,
            fields = self.fields)

    def load(self, fieldspath = ''):
        if not fieldspath:
            fieldspath = self.fieldspath

        self.fieldsmap = self.getconfig(self.fieldspath)
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
        fname = "to_{}".format(name.split('_')[0])
        if not hasattr(pd.Series, fname):
            return None
        return getattr(pd.Series, "to_{}".format(name.split('_')[0]))

    def patchmissing(self, df, exclude = []):
        return df.patchmissing(exclude = exclude)

    def to_string(self, df, **kwds):
        tries = 0
        while True:
            try:
                return df.to_csv(index = False, header = False, **kwds)
            except UnicodeEncodeError as e:
                tries += 1; kwds.update(encoding = 'utf-8')
            if tries == 2:
                break

    def parse(self, df, to_strip = '', strip_default = True, learnfields = True, **kwds):
        if learnfields:
            self.learnfields(df, path = kwds.pop('path', ''), **self.learnerkwds)
            df = df.rename(columns = self.fieldsmap)

        defaultchars = ('\\', '=')
        if not strip_default:
            defaultchars = tuple()

        to_strip = [to_strip] + list(defaultchars)
        df = self.patchmissing(df.clean(*to_strip), exclude = kwds.pop('exclude', [])) #LOG LOG LOG!!!!!!
        self.info("The following characters have been stripped (if present): '{}'.".format(''.join(to_strip)))

        for name, fields in self.fieldgroups.items():
            if fields:
                flds = df.filterfields(items = fields)
                func = self._getfunc(name)
                if name == 'address_fields':
                    self.info("Data contains address information.  I will attempt to extract street, city, state, and zip.")
                    df = df.to_address()
                if not func:
                    continue

                self.info("Applying function '{}' to fields '{}'".format(func.func_name, ', '.join(map(str, flds))))
                df[flds] = df[flds].apply(func)

        return self._conform(df)

    def parsefile(self, path, outfile = '', outdir = 'processed', learnfields = True, **kwds):
        if 'converters' not in kwds:
            kwds.update(converters = self.converters)

        _kwds = getcsvkwds(kwds)
        _file = File.guess(path, **_kwds)
        if not outfile:
            _ = OSPath(_file.basename()).stem
            outfile = "{}_output.csv".format(_)

        mkdir(outdir); outfile = mkpath(outdir, outfile)
        normalized = 0
        _learnfields = False
        if not learnfields:
            _learnfields = True
            kwds['path'] = path

        __ = _file.dfreader(learnfields = learnfields, **self.learnerkwds)
        writedata(outfile, "{}\n".format(','.join(self.fields)))
        for df in __:
            df = self.parse(df, learnfields = _learnfields,
                **filterdict(kwds, *_kwds.keys()))
            normalized += len(df)
            appendData(outfile, self.to_string(df))
        self.info("Normalized data written to '{}'".format(outfile))
        return mergedicts(_file.properties,
            rows_normalized = normalized)
