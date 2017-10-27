import logging
from functools import wraps
import pandas as pd
from peewee import *
from playhouse.shortcuts import RetryOperationalError

from generic import reversedict, logging_setup, chunker
from fileIO import from_json

db_logger = logging_setup(name = 'db', level = logging.INFO)
#peewee_logger = logging_setup(name = 'peewee', level = logging.INFO)

def _get_credentials(path = 'login.json'):
    return from_json(path)

def getdb(dbname, flavor = 'mysql', path = 'login.json', hostalias = 'localhost', **kwds):
    if not kwds:
        kwds = _get_credentials(path)[hostalias]
    return dbclasses()[flavor](dbname, **kwds)

def dbclasses():
    return {'mysql' : CustomMySQLDatabase, 'sqllite' : SqliteDatabase}

def connected(func):
    def inner(database, *args, **kwds):
        conn = database._create_connection()
        cursor = conn.cursor()
        try:
            return func(database, cursor, *args, **kwds)
        finally:
            cursor.close()
            conn.close()
    return inner

def _dataframe(func):
    @wraps(func)
    def inner(cls, *args, **kwds):
        return cls.to_dataframe(func(*args, **kwds))
    return inner

def _insertdecorator(func):
    @wraps(func)
    def inner(cls, rows, chunksize = 2500):
        tablename = cls._meta.db_table
        db_logger.info("Insertion queued for table '{}' ({} rows)".format(tablename, len(rows)))
        rowgroups = chunker(rows, chunksize)
        inserted = 0
        with cls._meta.database.atomic():
            for _rows in rowgroups:
                db_logger.info("Inserted {} rows...".format(len(_rows)))
                _inserted = func(cls, _rows)
                inserted += _inserted
                db_logger.info("{} rows inserted".format(_inserted))
        db_logger.info("{} rows successfully inserted into '{}'".format(inserted, tablename))
        return inserted
    return inner

def get_basemodel(database):
    dbproxy = Proxy()
    class BaseModel(Model):
        class Meta:
            database = dbproxy

        @classmethod
        @_insertdecorator
        def bulkinsert(cls, rows, **kwds):
            return cls.insert_many(rows).execute()

        @classmethod
        @_insertdecorator
        def tryinsert(cls, rows, **kwds):
            inserted = []
            for row in rows:
                try:
                    inserted.append(cls.insert(**row).execute())
                except IntegrityError as e:
                    db_logger.error(e)
                except OperationalError as e:
                    db_logger.error(e)
            return len(inserted)

        @classmethod
        def insertdf(cls, df, extrafields = [], bulk = False, **kwds):
            fields = [field for field in df.filter_fields(items = cls._meta.fields.keys())
                      if field not in extrafields]

            rows = df.dropna(subset = fields, how = 'all')\
                .filter(items = fields + extrafields)\
                .fillna('').to_dict(orient = 'records')

            if not rows:
                db_logger.warning("Nothing to insert.  All fields ('{}') are blank.".format(', '.join(fields)))
                return

            if bulk:
                return cls.bulkinsert(rows, **kwds)
            return cls.tryinsert(rows, **kwds)

        @classmethod
        def to_records(cls, query):
            return list(query.dicts().execute())
        
        @classmethod
        def to_dataframe(cls, query):
            return pd.DataFrame(cls.to_records(query))

        @classmethod
        def getdict(cls, field, reversed = False):
            __ = {row.id : getattr(row, field) for row in cls.select()}
            if reversed:
                return reversedict(__)
            return __
            

    dbproxy.initialize(database)
    return BaseModel

class CustomMySQLDatabase(RetryOperationalError, MySQLDatabase):

    @connected
    def loadcsv(self, cursor, path, table, overwrite = False, fields = [], lineterminator = '\n'):
        """Load csvfile into MySql database.
            **WARNING USE WITH CAUTION!!!**
        """
        if sys.platform == 'win32':
            path = path.replace(os.sep, os.altsep)
        if not fields:
            fields = Csv(path).testrows[0]
        if overwrite:
            db_logger.warning("Overwriting table '%s' with contents of '%s'." % (table, path))
            db_logger.warning('truncate %s' % table)
    
        db_logger.info("Importing '%s' into '%s'" % (path, table))
        
        return cursor.execute("""
            LOAD DATA LOCAL INFILE "{path}"
            INTO TABLE {table}
            FIELDS TERMINATED BY ','\n
            Optionally ENCLOSED BY '"'
            ESCAPED BY '\\\\'
            LINES TERMINATED BY '{lineterminator}'
            IGNORE 1 LINES\n\n({fields});
                """.format(path = path,
                    table = table,
                    fields = ','.join("`%s`" % s for s in fields),
                    lineterminator = lineterminator
                        ))
