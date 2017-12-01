import os
import sys
from warnings import filterwarnings
import MySQLdb
from functools import wraps, partial
import numpy as np
import pandas as pd

from generic import GenericBase, mergedicts, remove_non_ascii, attrlist
from fileIO import Csv, isearch, mkdir, mkpath, writedata
from timeutils import utcnow

def dbfunc(dbclass):
    def decorator(func):
        @wraps(func)
        def inner(*args, **kwds):
            args = list(args) + [dbclass()]
            return func(*args, **kwds)
        return inner
    return decorator

a_dangerous_operation = isearch('(?:^truncate|^delete|^drop)\s+')
updtformat = 'UPDATE `{0}` SET\n{1}\nWHERE {2} = "{3}";\n\n'.format

def backtick(fields):
    return ','.join("`%s`" % s for s in fields)

def df_2_update_query(df, table, indexcol = None):
    if not indexcol:
        __ = df.copy()
        indexcol = 'id'

    __ = df.set_index(indexcol)
    update_queries = []
    for k, v in __.fillna('').to_dict(orient = 'index').items():
        updtcols = ',\n'.join('\t`%s` = "%s"' % (k1,
            str(remove_non_ascii(v1)).replace('"','')
                ) for k1, v1 in v.items() if v1 != '')

        update_queries.append(updtformat(
            table,updtcols, indexcol, k))

    return '\n'.join(update_queries)

def df_2_update_file(df, table, outdir = '', **kwds):
    outfile = mkpath(mkdir(outdir, '%s_updates' % table),
        "update_%s.sql" % utcnow()\
        .strftime("%Y-%m-%d_%I.%M.%S"))
    writedata(outfile,
        df_2_update_query(df,
            table, **kwds))

class Database(GenericBase):
    _udeftables = {} #list of dicts [{'table' : {'fields' : {'field' : 'data type' }, 'constraints' : ('a', 'b', 'c', )}}]
    def __init__(self, login = {}, *args, **kwds):
        super(Database, self).__init__(*args, **kwds)
        self._logging_extra = {'db' : login.get('db', 'DB not specified.')}
        self._login = login

        for table, _settings in self._udeftables.items():
            self.create_table(table,
                _settings['fields'],
                constraint_fields = _settings['constraints'])

    def connected(func):
        def inner(self, *args, **kwds):
            connection = MySQLdb.connect(**self._login)
            cursor = connection.cursor(MySQLdb.cursors.DictCursor)
            filterwarnings('ignore', category = cursor.Warning)
            try:
                return func(self, cursor, *args, **kwds)
            finally:
                cursor.close()
                connection.close()
        return inner

    @connected
    def create_table(self, cursor, table, fields, id_field = 'id', constraint_fields = []):
        if not isinstance(fields[0], tuple):
            _ = ["`%s` VARCHAR(150) DEFAULT NULL," %
                 f for f in fields]
        else:
            _ = ["`%s` %s," % f for f in fields] # fields is a list of tuples
        fieldlist = '\n  '.join(_)
        query = (
            '''CREATE TABLE IF NOT EXISTS %s (
            %s int(11) NOT NULL AUTO_INCREMENT,\n  %s\nPRIMARY KEY (%s)\n)
                ''') % (table, id_field, fieldlist, id_field)
        cursor.execute(query)
        if constraint_fields:
            self.add_constraints(table, constraint_fields)
        return table

    @connected
    def add_constraints(self, cursor, table, fields, name = ''):
        if not name:
            name = "_".join(fields)
        try:
            cursor.execute("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)" %\
                (table, name, ', '.join("`%s`" % i for i in fields)))
        except MySQLdb.OperationalError as e:
            if not e.args[0] == 1061:
                raise MySQLdb.OperationalError

    @connected
    def describe(self, cursor, table):
        cursor.execute("describe %s;" % table)
        return cursor.fetchall()

    @connected
    def showtables(self, cursor):
        cursor.execute("show tables;")
        return cursor.fetchall()

    @connected
    def affectrows(self, cursor, query, force = False, *args, **kwds): #frame = True,
        if not force:
            assert not a_dangerous_operation(query),\
                "Cannot perform operation;\n%s'" % query
        n = cursor.execute(query)
        self.info("%s rows affected." % n)
        return n

    def list_fields(self, table):
        return [i['Field'].lower() for i in self.describe(table)]

    def load_csv(self, path, table, overwrite = False, fields = [], lineterminator = '\n'):
        """Load csvfile into MySql database.
            **WARNING USE WITH CAUTION!!!**
        """
        if sys.platform == 'win32':
            path = path.replace(os.sep, os.altsep)
        if not fields:
            fields = Csv(path).testrows[0]
        if overwrite:
            self.warning("Overwriting table '%s' with contents of '%s'." % (table, path))
            self.affectrows('truncate %s' % table)

        self.info("Importing '%s' into '%s'" % (path, table))
        rows_imported = self.affectrows("""
            LOAD DATA LOCAL INFILE "{path}"
            INTO TABLE {table}
            FIELDS TERMINATED BY ','\n
            Optionally ENCLOSED BY '"'
            ESCAPED BY '\\\\'
            LINES TERMINATED BY '{lineterminator}'
            IGNORE 1 LINES\n\n({fields});
                """.format(path = path,
                    table = table,
                    fields = backtick(fields),
                    lineterminator = lineterminator
                        ))

        return rows_imported

    def insert(self, df, table, **kwds):
        chunksize = len(df) / 100000
        if chunksize == 0:
            chunksize = 1

        for data in np.array_split(df, chunksize):
            keys = ', '.join("`%s`" % i for i in data.columns)
            vals = "%s;" % ',\n\t'.join(
                "(%s)" % ', '.join(map(lambda x: '"%s"' % str(x).replace('"', ''), list(i)[1:])) for
                    i in data.fillna('').itertuples()
                        ).replace(r'\"', '"')
            try:
                self.info("%s rows of data queued for '%s'" % (len(data), table))
                self.affectrows("INSERT INTO %s\n\t(%s)\nVALUES\n\t%s" % (table, keys, vals))
            except MySQLdb.IntegrityError as e:
                self.error(e)

    def tableupdate(self, df, table, **kwds):
        self.warning("Performing inplace update on '%s' for %s records" % (table, len(df)))
        return self.affectrows(df_2_update_query(df, table, **kwds))

    @connected
    def retrieve(self, cursor, query, frame = True, *args, **kwds):
        cursor.execute(query)
        data = cursor.fetchall()
        if not frame:
            return data
        try:
            return pd.DataFrame([
                i for i in data
                    ]).clean('=').clean_fields()
        except AttributeError:
            return pd.DataFrame()

    def select(self, table, fields = None, subquery = '', *args, **kwds):
        if fields:
            fields = backtick(fields)
        else:
            fields = '*'
        return self.retrieve(
            'select %s from %s\n%s;' % (
                fields, table, subquery),
                    **kwds)

    def get_mapper(self, table, valfield, idfield = 'id'):
        return self.select(table, fields = [idfield, valfield]).get_mapper(valfield, idfield)

    def select_grouped_aggregate(self, table, indexcol, aggcol, aggfunc = "max", fields = [], **kwds):
        if not fields:
            fields = 't1.*'
        else:
            fields = ', '.join("t1.%s" % f for f in fields)
        query = ('''
        SELECT {4} FROM {0} t1
        INNER JOIN (
            SELECT {3}, {1}({2}) as {2}
            FROM {0}
            GROUP BY {3}
                ) t2 ON t1.{2} = t2.{2} and t1.{3} = t2.{3}'''
                    ).format(table, aggfunc, aggcol, indexcol, fields)

        return self.retrieve(query, **kwds)

    def iterselect(self, table, size = 100000, *args, **kwds):
        offset = 0
        qformat = "{subquery} limit {limit} offset {offset}".format
        subquery = kwds.pop('subquery', '')
        while True:
            subquery2 = qformat(subquery = subquery,
                limit = size,
                offset = offset)

            rows = self.select(table, subquery = subquery2, **kwds)
            offset += (size + 1)
            yield rows
            if len(rows) < size:
                break

class DatabaseTable(GenericBase):
    def __init__(self, table, id = None, login = {}, id_field = 'id', *args, **kwds):
        self.table = table
        self.id = id
        self.id_field = id_field
        super(DatabaseTable, self).__init__(*args, **kwds)
        self.db = Database(login = login)

    def __repr__(self):
        return "Table(%s) --> %s: %s" % (self.table, self.id_field, self.id)

    @classmethod
    def instance(cls, func):
        def inner(self, *args, **kwds):
            _id = id = kwds.pop('id', None)
            return cls(func(self), id = _id, login = self._login, *args, **kwds)
        return inner

    @property
    def records(self):
        return self.select()

    def subquery(self, query = ''):
        return "where {0} = {1}\n{2}".format(self.id_field, self.id, query)

    def select(self, **kwds):
        if self.id:
            _ = kwds.pop('subquery', '')
            kwds.update({'subquery' : self.subquery(_)})

        return self.db.select(
                self.table,
                fields = kwds.pop('fields', None),
                **kwds)

    def update(self, df, **kwds):
        self.db.tableupdate(self.table, df, **kwds)
