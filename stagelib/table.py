import os
import gc
import logging
import pandas as pd

from generic import GenericBase, mergedicts, attrlist
from fileio import OSPath, File, Folder, datawriter, from_json_if_exists, to_json, mkpath, movepath, mkdir
from db import Database
import dataframe
from learner import learn_fields

renformat = "'{field}' --> '{choice}'.".format

class Table(GenericBase):

    def __init__(self, path, outfile = '', preprocess = True, *args, **kwds):
        self.path = path
        super(Table, self).__init__(path, outfile = outfile)
        self.file = File.guess_type(path, **kwds)

        if not outfile:
            self.outfile = "%s_output.csv" % (self.file.stem)
        if preprocess:
            self.preprocess()

    def __repr__(self):
        return "%s --> Table" % self.file.__repr__()

    def __iter__(self):
        if not hasattr(self, 'properties'):
            self.preprocess()

        self.chunkidx = 0
        self.rows_processed = 0
        for item in self.file.items:
            df = self.read(**item)
            self.rows_processed += len(df)
            self.chunkidx += 1
            gc.collect()
            yield df

        self.info("%s rows processed." % self.rows_processed)
        gc.disable()
        gc.collect()

    @classmethod
    def processfile(cls, path, dirname = '', outdir = 'processed', **kwds):
        obj = cls(path, **kwds)
        map(obj.write, obj)
        movepath(obj.outfile, outdir)
        return obj

    def preprocess(self):
        map(None, self.file.preprocess())
        self.properties = self.file.properties

        self.samples = {i : pd.DataFrame(item.pop('sample')) for i,
                            item in enumerate(self.file.items)}
    
    def read(self, data = '', kwds = {}):
        return pd.read_csv(data, **kwds)

    def write(self, df, **kwds):
        if self.chunkidx > 1:
            kwds.update({'header' : False})

        datawriter(self.outfile, df.to_csv(index = False, **kwds))
        self.info("data written to '%s'" % self.outfile)

class StageTable(Table):
    CONFIGDIR = os.path.join(os.path.dirname(__file__), 'config')
    DEFAULT_FIELDSPATH = mkpath(CONFIGDIR, 'fields_config.json')

    def __init__(self, path, fields_path = '', learn = False, table = '', omit = ('\\', '='), **kwds):
        self.learn = learn
        self.table = table
        self.fields_path = self.DEFAULT_FIELDSPATH
        if fields_path:
            self.fields_path = fields_path
        self.omit = omit
        self.load()
        super(StageTable, self).__init__(path)

    @staticmethod
    def get_config(path):
        __ = from_json_if_exists(path)
        if not __:
            return {}
        return __

    @staticmethod
    def get_table_config(table):
        _ = mkpath(StageTable.CONFIGDIR, 'schema', table + '.json')
        return StageTable.get_config(_)

    @staticmethod
    def get_table_fields(table, login = {}):
        __ = StageTable.get_table_config(table)
        if 'fields' not in __:
            return Database(login = login).list_fields(table)
        return __['fields']

    @staticmethod
    def learnfields(df, fields_path = '', table = '', **kwds):
        if not fields_path:
            fields_path = mkpath(StageTable.CONFIGDIR,
                'fields_map',
                "%s_fields_map.json" % table)

        __ = StageTable.get_config(fields_path)
        fields_map = learn_fields(df, __, **kwds)
        to_json(fields_path, fields_map)
        return fields_map

    @staticmethod
    def conform(df, table, learn = False, fields_path = '', fields = [], fields_map = {}, **kwds):
        if not fields:
            fields = StageTable.get_table_fields(table, **kwds)

        if learn:
            fields_map.update(StageTable.learnfields(df,
                fields_path = fields_path,
                table = table,
                fields = fields))
        df.rename(columns = fields_map, inplace = True)
        return df.ix[:, fields]

    @property
    def fieldgroups(self):
        return [i for i in attrlist(self) if isinstance(i[1], list) and i[0].endswith('_fields')]

    def _conform(self, df, **kwds):
        return self.conform(df, self.table, fields_map = self.fields_map, **kwds)

    def __iter__(self):
        for df in super(StageTable, self).__iter__():
            yield self.normalize(df)
            gc.collect()
        gc.disable()
        gc.collect()

    def load(self):
        self.fields_map = self.get_config(self.fields_path)
        config = self.get_table_config(self.table)
        if not config:
            try:
                raise Exception, "No config file for table '%s'" % self.table
            except:
                return

        for k, v in config.items():
            setattr(self, k, v)

    def field_mapper(self):
        for i, df in self.samples.items():
            __ = self.learnfields(df,
                table = self.table,
                fields_path = self.fields_path)
            self.fields_map.update(__)

    def preprocess(self):
        super(StageTable, self).preprocess()
        if self.learn:
            self.field_mapper()

    def getfunc(self, name):
        return getattr(pd.Series, "to_{}".format(name.split('_')[0]))

    def normalize(self, df, *args, **kwds):
        df = df.rename(columns = self.fields_map).clean(*self.omit)
        for k, v in self.fields_map.items():
            if v in df.columns:
                self.info((renformat(field = k, choice = v)))

        for name, fields in self.fieldgroups:
            if fields:
                cols = df.filter_fields(items = fieldlist)
                df[cols] = df[cols].apply(self.getfunc(name))
        return self.conform(df)
