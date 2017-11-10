from __future__ import division
import os, sys, io, csv, re, gc, xlrd, json, zipfile
import xml.etree.cElementTree as ET
import shutil, subprocess, hashlib, contextlib
from subprocess import PIPE
from datetime import date
from string import punctuation
from cStringIO import StringIO
from collections import Counter, defaultdict
from functools import partial, wraps

from generic import *
from timeutils import utcnow
from learner import learn_fields

pd = None
isearch = partial(lambda x: re.compile(x, re.I).search)

re_NONFIELD = re.compile('^(?:\s+)?(?:(?:\$)?\d+(?:[-\/\.\s,]+|$)|[%s]|[\|,\t]+(?:\s+)?|$)' % punctuation)
PATHFUNCS = dir(os.path)

def importpandas(func):
    @wraps(func)
    def inner(*args, **kwds):
        global pd
        if not pd:
            import pandas as pd
        return func(*args, **kwds)
    return inner

def pathdeco(func):
    @wraps(func)
    def inner(path, *args, **kwds):
        return func(OSPath(path), *args, **kwds)
    return inner

def presence(exists = True):
    def decorator(func):
        @wraps(func)
        def inner(path, *args, **kwds):
            if OSPath.exists(path) is exists:
                return func(str(path), *args, **kwds)
        return inner
    return decorator

if_exists = presence()
if_not_exists = presence(exists = False)

@contextlib.contextmanager
def fileopener(fh, mode = "rb", *args, **kwds):
    if isinstance(fh, (basestring, OSPath, Csv, Excel)):
        fh = io.open(str(fh), mode, **kwds)
    try:
        yield fh
    finally:
        fh.close()

def filehandler(mode = "rb"):
    def decorator(func):
        @wraps(func)
        def inner(fh, *args, **kwds):
            with fileopener(fh, mode = kwds.pop('mode', mode), **kwds) as fh:
                return func(fh, *args, **kwds)
        return inner
    return decorator

@pathdeco
def defaultdir(path):
    if path.isfile():
        return path.join(OSPath.dirname(path.abspath()), path.stem)
    return path.dirname()

@pathdeco
def mkpath(path, *args):
    return path.join(*map(str, args))

@pathdeco
def mktspath(path, datefmt = "%Y-%m-%d_%I.%M.%S", *args):
    return "{0}_{1}_.{2}".format(path.stem,
        utcnow().strftime(datefmt), path.ext)

def mkdir(dirname, *args):
    _dirname = mkpath(dirname, *args)
    if not OSPath.exists(_dirname):
        os.mkdir(_dirname)
    return _dirname
    
def get_homedir():
    return OSPath.dirname(__file__)

def is_zipfile(path):
    return zipfile.is_zipfile(path) and r'.xls' not in path

def filezip(newzipfile, path, mode = 'w', **kwds):
    with zipfile.ZipFile(newzipfile, mode = 'w', **kwds) as zf:
        zf.write(path, OSPath.basename(path))

filezip64 = partial(filezip,
    allowZip64 = True,
    compression = zipfile.ZIP_DEFLATED)

def fileunzip(zipname, outdir = None, recursive = False, **kwds):
    if not is_zipfile(zipname):
        return
    if not outdir:
        outdir = defaultdir(zipname)
    with zipfile.ZipFile(zipname) as zf:
        zf.extractall(outdir, **kwds)
    if recursive:
        for path in Folder.listdir(outdir):
            fileunzip(path)
    return outdir

@filehandler()
def getmd5(fh):
    _md5 = hashlib.md5()
    while fh.tell() != OSPath.getsize(fh.name):
        _md5.update(fh.read(658760))
    return _md5.hexdigest()

@filehandler(mode = "rb")
def read(fh):
    return fh.read()

@filehandler(mode = 'wb')
def writedata(fh, data):
    fh.write(data)

@filehandler(mode = 'ab')
def appendData(fh, data):
    fh.write(data)

if_not_exists_write = if_not_exists(writedata)
if_exists_append = if_exists(appendData)

def chunkwriter(fh, data): ##for writing csv data in chunks
    if_exists_append(fh, data)
    if_not_exists_write(fh, data)

@filehandler(mode = 'r')
def from_json(fh):
    return json.loads(fh.read())

@if_exists
def from_json_if_exists(fh):
    return from_json(fh)

@filehandler(mode = 'wb')
def to_json(fh, data):
    fh.write(json.dumps(data, sort_keys = True, indent = 4,))

def xmlLoop(path, tagstart, tagstop):
    iterxml = ET.iterparse(path)
    data = []
    while True:
        event, elem = iterxml.next()
        if elem.tag != tagstart:
            data.append({elem.tag : elem.items()})
        if elem.tag == tagstop:
            yield data
            data = []

def parsexml(path, tagstart, tagstop):
    rows = []
    for data in xmlLoop(path, tagstart, tagstop):
        row = {}
        for item in data:
            row.update(dict(item.values()[0]))
        rows.append(row)
    return rows

@importpandas
def readtable(path, **kwds):
    func = pd.read_excel
    while True:
        if is_zipfile(path):
            with zipfile.ZipFile(path) as zf:
                fh = zf.open(zf.filelist[0].filename)
                x = fh.peek()
        else:
            fh = open(path)
            x = fh.readline()
            fh.seek(0,0)
        try:
            return func(fh, **kwds)
        except xlrd.XLRDError:
            kwds.update({'delimiter' : Csv.sniff(x), 'quoting' : 1, 'low_memory' : False, 'error_bad_lines' : False})
            func = pd.read_csv
            continue
        finally:
            fh.close()

@importpandas
def df2excel(output_file, **kwds):
    xlwriter = pd.ExcelWriter(output_file)
    for sheet_name, df in kwds.items():
        df.to_excel(xlwriter,
            sheet_name = sheet_name,
            index = False,
            encoding = 'utf-8')
    xlwriter.save()

@importpandas
def getcsvkwds(kwds):
    return filterdict(kwds, pd.read_csv.func_code.co_varnames)

def is_nonfield(x):
    return re_NONFIELD.search(x)

def locateheader(rows):
    lens = map(len, [filter(lambda x: x != '', row) for row in rows])
    ml = max(lens)
    ix = lens.index(ml)
    for i, row in enumerate(rows[ix:], ix):
        blanks = map(str.strip, row).count('')
        if blanks/ml >= 0.5 or any(is_nonfield(x) for x in row):
            continue
        return i + 1, row
    else:
        return ix, Tabular.createheader(ml)

class OSPathMeta(type):
    _methods = attrdict(os.path)
    def __getattr__(cls, name):
        if name in cls._methods:
            return cls._methods[name]
        raise AttributeError

class OSPath(GenericBase):
    __metaclass__ = OSPathMeta
    _iskey = isearch(r'((?<=get)(size|[a-z]time)|([a-z]+name|^ext$))')

    def __init__(self, path, setuplogging = False, *args, **kwds):
        self.path = path
        super(OSPath, self).__init__(path, setuplogging = setuplogging, *args, **kwds)
        grabfunctions(self, os.path, 'path')
        self.stem, self.ext = map(lambda x: x.strip('.'), self.splitext())

    def __str__(self):
        return self.path
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.path)

    @property
    def properties(self):
        if not hasattr(self, '_properties'):
            self._properties = {}
            for k, v in self.__dict__.items():
                if self._iskey(k):
                    k = self._iskey(k).group(1)
                    if callable(v):
                        v = v()
                        if 'time' in k:
                            v = date.fromtimestamp(v).strftime("%Y-%m-%d %I:%M:%S")
                    self._properties.update({k : v})
        return self._properties

class File(OSPath):
    def __init__(self, path, setuplogging = False, mode = "rb", chunksize = 5 * (1024*1024), **kwds):
        super(File, self).__init__(path, setuplogging = setuplogging, mode = mode, chunksize = chunksize, **kwds)
        self.kwds = kwds

    def __iter__(self):
        with open(self.path, self.mode) as fh:
            for i, data in enumerate(chunker(fh, chunksize = self.chunksize)):
                yield ''.join(data)

    @classmethod
    def countrows(cls, path, **kwds):
        return cls.guess(path)._countrows(**kwds)

    @staticmethod
    def guess(path, **kwds):
        try:
            return Excel(path, **kwds)
        except xlrd.XLRDError:
            return Csv(path, **kwds)
            
    def _countrows(self, chunksize = 500000, mode = "U"):
        if not hasattr(self, '_nrows'):
            if isinstance(self, Excel):
                self._nrows = sum(i.nrows for i in self.sheets)
                return self._nrows

            rows, offset, size = 0, 0, self.getsize()
            with open(self.path, mode = mode) as fh:
                while offset < size:
                    data = fh.read(chunksize)
                    offset = fh.tell()
                    rows += data.count('\n')
                self._nrows = rows; return rows
        return self._nrows

class Tabular(File):
    NULLS = ['null', 'NULL', 'None', 'none']

    def __init__(self, path, setuplogging = True, *args, **kwds):
        super(Tabular, self).__init__(path, setuplogging = setuplogging, *args, **kwds)
        if 'na_values' not in kwds:
            kwds['na_values'] = self.NULLS
        else:
            kwds['na_values'].extend(self.NULLS)
        self.kwds = mergedicts(kwds, self.kwds, lineterminator = '\n')

    @staticmethod
    def createheader(length = 10):
        return map(lambda x: "field.%s.of.%s" % (x[0], length),
            enumerate(xrange(1, length + 1), 1))

    @staticmethod
    def learnfields(df, fieldsmap, **kwds):
        fieldspath = kwds.pop('fieldspath', 'fieldsconfig.json')
        fieldsmap = mergedicts(from_json(fieldspath),
            learn_fields(df, fieldsmap, **kwds))
        to_json(fieldspath, fieldsmap)
        return fieldsmap

    @staticmethod
    def renamefields(df, names, **kwds):
        __ = Tabular.learnfields(df, kwds.pop('fieldsmap', {}), **kwds)
        return [__[name] for name in names]

    @property
    def properties(self):
        return mergedicts(rows_original = self._countrows(),
            **super(Tabular, self).properties)

    def preprocess(self, learnfields = False, **kwds):
        self.kwds = filterdict(self.kwds, ['mode', 'lineterminator'], inverse = True)
        self._countrows()

    def checkheader(self, rows):
        if not ('header' in self.kwds or 'skiprows' in self.kwds or 'names' in self.kwds):
            __ = locateheader(rows)
            return dict(skiprows = __[0], names = __[1], header = None)
        return {}

    @classmethod
    @importpandas
    def _iterdataframe(cls, func):
        def inner(self, *args, **kwds):
            self.kwds = filterdict(self.kwds, 'lineterminator', 'mode')
            self.preprocess(**kwds)
            self.rows = 0
            for df in func(self, *args):
                self.rows += len(df)
                yield df; gc.collect()
            self.info("{} rows read.".format(self.rows))
            gc.disable(); gc.collect()
        return inner

class Csv(Tabular):
    DELIMITERS = '|,\t;:'
    re_BADTAIL = re.compile(r'(^.*?"),"\n', re.M)

    def __init__(self, path, mode = "U", chunksize = 79650, **kwds):
        super(Csv, self).__init__(path, mode = mode, chunksize = chunksize, **kwds)
        self.fixcsv = '","\n' in self.testraw
        self.delimiter = self.sniff(self.testraw)

    @classmethod
    def sniff(cls, x):
        for k, v in Counter(x).most_common():
            if k in cls.DELIMITERS:
                return str(k)
        raise csv.Error, "Delimiter undetermined."

    @property
    def testraw(self):
        return self.head()

    @property
    def testrows(self):
        __ = self.testraw
        if self.fixcsv: __ = self.fix(__)
        return self.reader(__)

    @property
    def rules(self):
        if not hasattr(self, '_rules'):
            self._rules = mergedicts(delimiter = self.delimiter,
                **self.checkheader(self.testrows))
        return self._rules

    @filehandler(mode = "U")
    def head(self, n = 50):
        for i in chunker(self, n): return ''.join(i)

    def fix(self, data):
        return re_BADTAIL.sub(r'\1\n', data)

    def preprocess(self, **kwds):
        super(Csv, self).preprocess()
        if kwds.pop('learnfields', False):
            df = pd.read_csv(StringIO(self.testraw), nrows = 20, **self.rules)
            self._rules['names'] = self.renamefields(df, self._rules['names'],
                **mergedicts(path = self.path, **kwds))

        if self.fixcsv:
            tmpfile = self.path.replace('.csv', '.temp')
            for data in self:
                chunkwriter(tmpfile, self.fix(data)) #chunkwrite to tempfile, delete original, rename back to original
                gc.disable(); gc.collect()
            os.remove(self.path)
            os.rename(tmpfile, self.path)

    def reader(self, data):
        buf = StringIO(remove_non_ascii(data))
        return [i for i in csv.reader(buf,
            delimiter = self.delimiter, quoting = 1)]

    @Tabular._iterdataframe
    def dfreader(self, **kwds):
        if not 'chunksize' in self.kwds:
            self.kwds['chunksize'] = self.chunksize
        for df in pd.read_csv(self.path, **mergedicts(self.rules, self.kwds)):
            yield df

class IncompleteExcelFile(Exception):
    def __init__(self):
        super(IncompleteExcelFile, self).__init__("This sheet contains exactly 65536 rows.  Data may be incomplete.")

class Excel(Tabular):
    def __init__(self, path, mode = 'rb', **kwds):
        super(Excel, self).__init__(path, mode = mode, **kwds)
        self.wb = xlrd.open_workbook(self.path, on_demand = True)

    def _csvbuffer(self, rows):
        buf = StringIO()
        cw = csv.writer(buf, quoting = csv.QUOTE_ALL, lineterminator = '\n')
        cw.writerows(rows)
        return StringIO(buf.getvalue())

    def reader(self, sheet):
        return [[x.encode('utf-8') if isinstance(x, unicode) else
                x for x in sheet.row_values(n)] for n in xrange(sheet.nrows)]
    @property
    def sheets(self):
        return list(self.wb.sheets())

    @property
    def sheetnames(self):
        return [s.name for s in self.sheets]

    def preprocess(self, **kwds):
        super(Excel, self).preprocess()
        self._sheets = {}
        learnfields = kwds.pop('learnfields', False)

        for sheet in self.sheets:
            if sheet.nrows > 1:
                rows = self.reader(sheet)
                testrows = rows[0:50]
                _rules = mergedicts(self.checkheader(testrows), nrows = sheet.nrows, **self.kwds)

                if learnfields:
                    df = pd.read_csv(self._csvbuffer(testrows), **_rules)
                    _rules['names'] = self.renamefields(df, _rules['names'], **kwds)

                self._sheets.update({sheet.name :  _rules})
            elif sheet.nrows == 65536:
                raise IncompleteExcelFile

    @Tabular._iterdataframe
    def dfreader(self, **kwds):
        for name, rules in self._sheets.items():
            yield pd.read_excel(self.path, sheetname = name, **rules)

class Folder(OSPath):
    def __init__(self, path, pattern = '', recursive = False, files_only = False, setuplogging = True, **kwds):
        rgx = re.compile(pattern)
        self.search = lambda x, rgxpattern = rgx: rgxpattern.search(x)
        if self.search(path, rgxpattern = re.compile(r'(?:\.|^$)')):
            path = os.getcwd()
        self.duplicatefiles = set()
        self.distinctfiles = set()
        self.recursive = recursive
        self.files_only = files_only
        super(Folder, self).__init__(path, setuplogging = setuplogging)

    @classmethod
    def listdir(cls, dirname, **kwds):
        _ = cls(dirname, **kwds)
        if not kwds.get('recursive', _.recursive):
            return list(_)
        return (i for i in cls(dirname, **kwds))

    @classmethod
    def unzipfiles(cls, dirname, recursive_unzip = False, extractdir = 'unzipped', **kwds):
        for zipname in filter(is_zipfile, cls.listdir(dirname, **kwds)):
            fileunzip(zipname, recursive = recursive_unzip, outdir = extractdir)
        return extractdir

    @classmethod
    @importpandas
    def table(cls, *args, **kwds):
        return pd.DataFrame([
            OSPath(path).properties for path in Folder.listdir(*args, **kwds)
                ])

    @classmethod
    def get_distinctfiles(cls, dirname, **kwds):
        extractdir = kwds.pop('extractdir', 'unzipped')
        folder = cls(dirname, **kwds)
        return [path for path in
                folder._get_distinctfiles(extractdir = extractdir)
                    ]

    def _get_distinctfiles(self, **kwds):
        __ = defaultdict(list)
        self.unzipfiles(self.path, recursive_unzip = True, **kwds)
        self.recursive = True
        self.files_only = True
        for fname in self:
            if not is_zipfile(fname):
                __[getmd5(fname)].append(fname)
        for k, v in __.items():
            if len(v) > 1:
                self.info("Duplicate file found: '{}'".format(v[1]))
                self.duplicatefiles.add(v[1])
            self.distinctfiles.add(v[0])
            yield v[0]
        self.info("Total distinct files: {}.".format(len(self.distinctfiles)))
        self.info("Total duplicate files: {}.".format(len(self.duplicatefiles)))

    def _walk(self):
        if not self.recursive:
            for path in (self.join(i) for i in os.listdir(self.path) if self.search(i)):
                yield path
        else:
            for root, dirs, files in os.walk(self.path):
                if not self.files_only:
                    files.extend(dirs)
                for fname in sorted(files):
                    path = mkpath(root, fname)
                    if not self.search(path):
                        continue
                    yield path

    def __iter__(self):
        for path in self._walk():
            yield path
