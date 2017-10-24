import logging
from functools import wraps
from peewee import *

from fileIO import from_json

db_logger = logging.getLogger('db')
peewee_logger = logging.getLogger('peewee')

def getlogin(path = 'login.json'):
    return from_json(path)

def getdb(dbname, flavor = 'mysql', path = 'login.json', hostalias = 'localhost', **kwds):
    if not kwds:
        kwds = getlogin(path)[hostalias]
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

def to_df(queryset):
    return pd.DataFrame(list(queryset.dicts()))

def qsdataframe(func):
    @wraps(func)
    def inner(*args, **kwds):
        return to_df(func(*args, **kwds))
    return inner

def getbasemodel(database):
    dbproxy = Proxy()
    class BaseModel(Model):
        class Meta:
            database = dbproxy

        @classmethod
        def bulkinsert(cls, data):
            db_logger.info("{} rows queued for insert into {}".format(len(data), cls._meta.db_table))
            with cls._meta.database.atomic():
                cls.insert_many(data).execute()

        @classmethod
        def tryinsert(cls, **kwds):
            try:
                return cls.insert(**kwds).execute()
            except IntegrityError as e:
                db_logger.info(e); pass

    dbproxy.initialize(database)
    return BaseModel

class CustomMySQLDatabase(MySQLDatabase):

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
        
        rows_imported = cursor.execute("""
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

        return rows_imported
