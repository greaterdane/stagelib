import os, gc
import pandas as pd

from generic import GenericBase, mergedicts
from fileIO import OSPath, File, chunkwriter, from_json, to_json, mkpath, mkdir, get_readcsvargs
import dataframe
from learner import learn_fields

learnlogfmt = "'{field}' --> '{choice}'.".format

class SchemaNotRegistered(Exception):
    pass

class SchemaNotSpecified(Exception):
    pass

class Stage(GenericBase):
    CONFIGDIR = mkdir(OSPath.dirname(__file__), 'config')
    SCHEMADIR = mkdir(CONFIGDIR, 'schema')
    FIELDSPATH = mkpath(CONFIGDIR, 'fieldsconfig.json')

    def __init__(self, schname, fieldspath = '', **kwds):
        self.name = schname
        self.fieldspath = self.FIELDSPATH
        if fieldspath:
            self.fieldspath = fieldspath

        self.load()
        super(Stage, self).__init__()

    @classmethod
    def getschemapath(cls, name):
        return mkpath(cls.SCHEMADIR, '{}.json'.format(name))

    @classmethod
    def getconfig(cls, path):
        if not OSPath.exists(path):
            raise SchemaNotRegistered, "'{}' not found.  Please register desired configuration.".format(path)
        return from_json(path)

    @classmethod
    def registerschema(cls, name, fields = [], datetime_fields = [], text_fields = [], numeric_fields = [], converters = {}):
        template = cls.getconfig(cls.getschemapath('template'))
        template['fields'] = fields
        template['datetime_fields'] = datetime_fields
        template['text_fields'] = text_fields
        template['numeric_fields'] = numeric_fields
        to_json(cls.getschemapath(name))
        self.info("'{}' registered".format(name))

    @classmethod
    def get_schemaconfig(cls, name):
        return cls.getconfig(cls.getschemapath(name))

    @classmethod
    def getfields(cls, name):
        __ = cls.get_schemaconfig(name)
        if 'fields' not in __:
            raise SchemaNotRegistered, "{} is missing 'fields' attribute, please re-register or edit config path '{}'.".format(name, cls.getschemapath)
        return __['fields']

    @classmethod
    def learnfields(cls, df, fieldspath = '', usedefault = False, name = '', **kwds):
        if not fieldspath:
            if usedefault:
                fieldspath = cls.FIELDSPATH
            if not name:
                raise SchemaNotSpecified, "Either a 'fieldsmap' path or schema name needs to be specified."
            else:
                fieldspath = mkpath(cls.CONFIGDIR, "%s_fieldsmap.json".format(schema))
                to_json({}, fieldspath)

        __ = cls.getconfig(fieldspath)
        fieldsmap = learn_fields(df, __, **kwds)
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
    def normalize(cls, df, schname, fieldspath = '', **kwds):
        return cls(schname, fieldspath = fieldspath).normdf(df)

    @classmethod
    def normalizefile(cls, path, schname, fieldspath = '', **kwds):
        return cls(schname, fieldspath = '').normfile(**kwds)

    @property
    def fieldgroups(self):
        return {k : v for k, v in self.__dict__.items() if
            isinstance(v, list) and k.endswith('_fields')}

    def load(self, fieldspath = ''):
        if not fieldspath:
            fieldspath = self.fieldspath

        self.fieldsmap = self.getconfig(self.fieldspath)
        __ = self.get_schemaconfig(self.name)
        for k, v in __.items():
            setattr(self, k, v)

    def _learnfields(self, df, **kwds):
        __ = self.learnfields(df,
            table = self.name,
            fieldspath = self.fieldspath, **kwds)

        self.fieldsmap.update(__)
        for k, v in self.fieldsmap.items():
            if k in df.columns:
                self.info(learnlogfmt(field = k, choice = v))

    def _conform(self, df, **kwds):
        return self.conform(df, self.name,
            fieldsmap = self.fieldsmap,
            fields = self.fields, **kwds)

    def _getfunc(self, name):
        fname = "to_{}".format(name.split('_')[0])
        if not hasattr(pd.Series, fname):
            return None
        return getattr(pd.Series, "to_{}".format(name.split('_')[0]))

    def normdf(self, df, omit = '', omitdefault = True, **kwds):
        _defaultchars = ('\\', '=')
        if not omitdefault:
            _defaultchars = tuple()

        to_omit = [omit] + list(_defaultchars)
        self._learnfields(df, path = kwds.pop('path', ''))
        df = df.rename(columns = self.fieldsmap).clean(*to_omit)
        self.info("The following characters have been omitted: '{}'.".format(''.join(to_omit)))

        for name, fields in self.fieldgroups.items():
            if fields:
                flds = df.filter_fields(items = fields)
                func = self._getfunc(name)
                if not func:
                    continue
                df[flds] = df[flds].apply(func)
        gc.collect()
        return self._conform(df)

    def normfile(self, path, outfile = '', outdir = 'normalized', **kwds):
        csvkwds = {k : v for k, v in kwds.items() if k in get_readcsvargs()}
        tfile = File.guess(path, **csvkwds)
        normalized = 0
        if not outfile:
            _ = OSPath(tfile.basename()).stem
            outfile = "{}_output.csv".format(_)

        outfile = mkpath(mkdir(outdir), outfile)
        for df in tfile.dfreader():
            header = True
            if tfile.chunkidx > 1:
                header = False
            df = self.normdf(df, path = path, **kwds)
            normalized += len(df)
            chunkwriter(outfile,
                df.to_csv(index = False))

        self.info("Normalized data written to '{}'".format(outfile))
        return mergedicts(tfile.properties,
            rows_normalized = normalized)
