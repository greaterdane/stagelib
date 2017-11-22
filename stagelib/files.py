from __future__ import division
import os, sys, io, csv, re, gc, xlrd, json, zipfile
import xml.etree.cElementTree as ET
import shutil, subprocess, hashlib, contextlib
from datetime import date
from string import punctuation
from cStringIO import StringIO
from collections import Counter, defaultdict
from functools import partial, wraps

from generic import *
from timeutils import utcnow
from learner import learn_fields

pd = None
re_NONFIELD = re.compile('^(?:\s+)?(?:(?:\$)?\d+(?:[-\/\.\s,]+|$)|[%s]|[\|,\t]+(?:\s+)?)' % punctuation)
re_ERROR = re.compile(r'^Skipping line (?P<line>\d+): expected (?P<expected_length>\d+) fields, saw (?P<length>\d+)$')
logger = logging_setup(name = __name__)

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
        return func(ospath(path), *args, **kwds)
    return inner

@contextlib.contextmanager
def fileopener(fh, mode = "rb", *args, **kwds):
    if isinstance(fh, (basestring, ospath, Csv, Excel)):
        fh = io.open(str(fh), mode, **kwds)
    try:
        yield fh
    finally:
        fh.close()

def filehandler(mode = "rb"):
    def decorator(func):
        @wraps(func)
        def inner(fh, *args, **kwds):
            with fileopener(fh, mode = kwds.pop('mode', mode)) as fh:
                return func(fh, *args, **kwds)
        return inner
    return decorator

@pathdeco
def defaultdir(path):
    if path.isfile():
        return path.join(ospath.dirname(path.abspath()), path.stem)
    return path.dirname()

@pathdeco
def joinpath(path, *args):
    return path.join(*map(str, args))

@pathdeco
def timestamped_path(path, datefmt = "%Y-%m-%d_%I.%M.%S", *args):
    return "{0}_{1}_.{2}".format(path.stem, utcnow().strftime(datefmt), path.ext)

def get_homedir():
    return ospath.dirname(__file__)

def newfolder(dirname, *args):
    _dirname = joinpath(dirname, *args)
    if not ospath.exists(_dirname):
        os.mkdir(_dirname)
    return _dirname

def is_zipfile(path):
    return zipfile.is_zipfile(path) and r'.xls' not in path

@pathdeco
def filezip(path, newzipfile, mode = 'w', allowZip64 = True, compression = zipfile.ZIP_DEFLATED, **kwds):
    logger.info("Compressing '%s' to '%s'" % (path.basename(), newzipfile))
    with zipfile.ZipFile(newzipfile,
        mode = mode,
        allowZip64 = allowZip64,
        compression = compression,
        **kwds) as zf:
        zf.write(path.abspath(), path.basename())

def fileunzip(zipname, outdir = None, recursive = False, **kwds):
    logger.info("Extracting contents of '%s'" % zipname)
    if not is_zipfile(zipname):
        return

    if not outdir:
        outdir = defaultdir(zipname)

    with zipfile.ZipFile(zipname) as zf:
        zf.extractall(outdir, **kwds)
        logger.info("Contents of '%s' extracted to '%s'" % (ospath.basename(zipname), outdir))

    if recursive:
        for path in Folder.listdir(outdir):
            fileunzip(path)
    return outdir

@filehandler()
def getmd5(fh):
    _md5 = hashlib.md5()
    while fh.tell() != ospath.getsize(fh.name):
        _md5.update(fh.read(658760))
    return _md5.hexdigest()

@filehandler(mode = 'r')
def readjson(fh):
    return json.loads(fh.read())

@filehandler(mode = 'wb')
def writejson(fh, data):
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
def xml2df(path, tagstart, tagstop):
    return pd.DataFrame(
        parsexml(path, tagstart, tagstop)
            )

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
    lens = map(len, rows)
    ml = max(lens)
    ix = lens.index(ml)
    for i, row in enumerate(rows[ix:], ix):
        blanks = map(str.strip, row).count('')
        if blanks/ml >= 0.5 or any(is_nonfield(x) for x in row):
            continue
        return i + 1, row
    else:
        return ix, Tabular.createheader(ml)

@filehandler(mode = 'wb')
def createcsv(fh, fields, delimiter = ',', quoting = csv.QUOTE_ALL):
    csvwriter = csv.writer(fh)
    csvwriter.writerow(fields)

class ospathMeta(type):
    _methods = attrdict(os.path)
    def __getattr__(cls, name):
        if name in cls._methods:
            return cls._methods[name]
        raise AttributeError

class ospath(GenericBase):
    __metaclass__ = ospathMeta
    _iskey = partial(search, r'((?<=get)(size|[a-z]time)|([a-z]+name|^ext$))')

    def __init__(self, path, setuplogging = False, *args, **kwds):
        self.path = path
        super(ospath, self).__init__(path, setuplogging = setuplogging, *args, **kwds)
        grabfunctions(self, os.path, 'path')
        self.stem, self.ext = map(lambda x: x.strip('.'), self.splitext())

    def __str__(self):
        return self.path
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.path)

    @staticmethod
    def get_outfile(dirname, filename):
        return ospath.join(dirname, "%s_output.csv" % ospath(filename).stem)

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

class File(ospath):
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
    def move(filename, destination = 'processing', overwrite = False, auto_rename = True):
        if overwrite:
            auto_rename = False
    
        name = ospath.basename(filename)
        destfile = ospath(joinpath(destination, name))
        movefile = True
        number = 1
    
        while True:
            if not destfile.exists():
                break
            else:
                logger.info("File '%s' exists in '%s'." % (destfile.basename(), destination))
                if auto_rename:
                    stem, ext = destfile.splitext()
                    _ = "{}_{}{}".format(re.sub(r'_\d+$', '', stem), number, ext)
                    destfile = ospath(_)
                    logger.info("Auto-renaming '%s' to '%s'" % (name, destfile.basename()))
                    number += 1
                elif overwrite:
                    logger.info("'%s' will be overwritten." % (name))
                else:
                    movefile = False; break
    
        if movefile:
            logger.info("Moving '%s' to '%s'" % (filename, destination))
            shutil.move(filename, destfile.path)
    
        return destfile.path

    @staticmethod
    @filehandler(mode = "rb")
    def read(fh):
        return fh.read()
    
    @staticmethod
    @filehandler(mode = 'wb')
    def write(fh, data):
        fh.write(data)
    
    @staticmethod
    def append(fh, data, mode = 'ab'):
        File.write(fh, data, mode = mode)

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
    NULLS = ['null', 'NULL', 'None', 'none', '<none>', '<None>'] + list(punctuation)

    def __init__(self, path, setuplogging = True, *args, **kwds):
        super(Tabular, self).__init__(path, setuplogging = setuplogging, *args, **kwds)
        if 'na_values' not in kwds:
            kwds['na_values'] = self.NULLS
        else:
            kwds['na_values'].extend(self.NULLS)

        self.kwds = mergedicts(kwds, self.kwds)
        self.rowsdropped = 0
        self.preprocessed = False

    @staticmethod
    def createheader(length = 10):
        return map(lambda x: "field.%s.of.%s" % (x[0], length),
            enumerate(xrange(1, length + 1), 1))

    @staticmethod
    def learnfields(df, **kwds):
        fieldspath = kwds.pop('fieldspath', 'fieldsconfig.json')
        fieldsmap = mergedicts(
            readjson(fieldspath),
            learn_fields(df, kwds.pop('fieldsmap', {}), **kwds)
                )

        writejson(fieldspath, fieldsmap)
        return fieldsmap

    @staticmethod
    def renamefields(df, names, **kwds):
        _ = Tabular.learnfields(df, **kwds)
        return [_[name] for name in names]

    @property
    def properties(self):
        return mergedicts(rows_original = self._countrows(),
            **super(Tabular, self).properties)

    def preprocess(self, learnfields = True, **kwds):
        self.kwds = filterdict(self.kwds, ['mode', 'lineterminator'], inverse = True)
        self._countrows()
        self.preprocessed = True
        return self

    def checkheader(self, rows):
        if not ('header' in self.kwds or 'skiprows' in self.kwds or 'names' in self.kwds):
            skiprows, names = locateheader(rows)
            self.rowsdropped += skiprows
            return dict(skiprows = skiprows, names = names, header = None)
        return {}

    @classmethod
    @importpandas
    def _iterdataframe(cls, func):
        def inner(self, *args, **kwds):
            if not self.preprocessed:
                self.preprocess(**kwds)

            rows = 0
            for df in func(self, *args):
                rows += len(df)
                yield df; gc.collect()
            gc.disable(); gc.collect()
        return inner

class ImproperCsvError(Exception):
    pass

class Csv(Tabular):
    DELIMITERS = '|,\t;:'
    re_BADTAIL = re.compile(r'(^.*?"),"\n', re.M)

    def __init__(self, path, mode = "U", chunksize = 185000, **kwds):
        super(Csv, self).__init__(path, mode = mode, chunksize = chunksize, **kwds)
        self.fixcsv = search(self.re_BADTAIL, self.testraw)
        self.delimiter = self.sniff(self.testraw)
        self.kwds.update(low_memory = False)

    @classmethod
    def sniff(cls, x):
        for k, v in Counter(x).most_common():
            if k in cls.DELIMITERS:
                return str(k)
        raise csv.Error

    @staticmethod
    def errorparse(errortext):
        for error in errortext.split('\n'):
            __ = getdict(re_ERROR, error)
            if not __:
                continue
            yield mergedicts(__, index = __['line'] - 1)

    @staticmethod
    @importpandas
    def getbadlines(path, **kwds):
        buf = StringIO()
        kwds['header'] = None
        if 'names' in kwds:
            kwds.pop('header')

        with RedirectStdStreams(stdout = buf, stderr = buf):
            dfreader = pd.read_csv(path,
                error_bad_lines = False,
                chunksize = 185000, **kwds)

            for df in dfreader:
                continue

        _ = buf.getvalue()
        return list(Csv.errorparse(_))

    @property
    def testraw(self):
        return self.head()

    @property
    def testrows(self):
        __ = self.testraw
        if self.fixcsv: __ = self.fix(__)
        return self.reader(__)

    @property
    def tempfile(self):
        return re.sub(r'$', '.temp', self.path)

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
        return self.re_BADTAIL.sub(r'\1\n', data)

    def fileswap(self):
        os.remove(self.path)
        os.rename(self.tempfile, self.path)

    def preprocess(self, learnfields = False, **kwds):
        self = super(Csv, self).preprocess()
        if learnfields:
            buf = StringIO(self.testraw)
            df = pd.read_csv(buf, nrows = 20, **self.rules)
            __ = self._rules['names']
            self._rules['names'] = self.renamefields(df, __, **kwds)

        if self.fixcsv:
            for data in self:
                self.append(self.tempfile, self.fix(data))
                gc.disable(); gc.collect()
            self.fileswap()
        return self

    def reader(self, data):
        buf = StringIO(remove_non_ascii(data))
        return [i for i in csv.reader(buf,
                delimiter = self.delimiter, quoting = 1)]

    @Tabular._iterdataframe
    def dfreader(self, **kwds):
        if not 'chunksize' in self.kwds:
            self.kwds['chunksize'] = self.chunksize

        __ = pd.read_csv(self.path, 
            **mergedicts(self.rules, self.kwds))

        for df in __:
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

    def reader(self, sheet, nrows = None):
        if not nrows:
            nrows = sheet.nrows

        return [[x.encode('utf-8') if isinstance(x, unicode) else
                x for x in sheet.row_values(n)] for n in xrange(nrows)]
    @property
    def sheets(self):
        return list(self.wb.sheets())

    def preprocess(self, learnfields = False, **kwds):
        self = super(Excel, self).preprocess()
        self._sheets = {}

        for sheet in self.sheets:
            if sheet.nrows > 1:
                rows = self.reader(sheet, nrows = 50)
                rules = mergedicts(self.checkheader(rows),
                    nrows = sheet.nrows, **self.kwds)

                if learnfields:
                    df = pd.read_csv(self._csvbuffer(rows), **rules)
                    rules['names'] = self.renamefields(df, rules['names'], **kwds)

                self._sheets.update({sheet.name :  rules})
            elif sheet.nrows == 65536:
                raise IncompleteExcelFile
        return self

    @Tabular._iterdataframe
    def dfreader(self, **kwds):
        for name, rules in self._sheets.items():
            yield pd.read_excel(self.path, sheetname = name, **rules)

class Folder(ospath):
    def __init__(self, path, pattern = '', recursive = False, files_only = False, **kwds):
        self.search = partial(isearch, pattern = pattern)
        if self.search(pattern = r'^(?:\.)?$', x = path):
            path = os.getcwd()

        self.duplicatefiles = []
        self.distinctfiles = set()
        self.recursive = recursive
        self.files_only = files_only
        super(Folder, self).__init__(path,
            setuplogging = kwds.pop('setuplogging', True))

    @staticmethod
    def compressfiles(filenames, newzipfile = '', **kwds):
        if not newzipfile:
            newzipfile = 'original.zip'

        for path in filenames:
            filezip(path, ospath.abspath(newzipfile), mode = 'a', **kwds)

    @staticmethod
    def unzipfiles(zipfiles, recursive = False, outdir = None):
        for zipname in zipfiles:
            outdir = fileunzip(zipname, recursive = recursive, outdir = outdir)
        return outdir

    @classmethod
    def listdir(cls, dirname, **kwds):
        folder = cls(dirname, **kwds)
        if not folder.recursive:
            return list(folder)
        return (i for i in folder)

    @classmethod
    @importpandas
    def table(cls, *args, **kwds):
        return pd.DataFrame([cls(path).properties for path
                             in cls.listdir(*args, **kwds)])

    @classmethod
    def movefiles(cls, fromdir, destination, **kwds):
        return cls(fromdir)._movefiles(destination, **kwds)

    @property
    def zipfiles(self):
        return filter(is_zipfile, self)

    def listdistinct(self):
        self.recursive = True
        self.files_only = True
        self.unzipped_to = self.unzipfiles(self.zipfiles, recursive = True)

        __ = defaultdict(list)
        for filename in self:
            if not is_zipfile(filename):
                _md5 = getmd5(filename)
                self.info("MD5: '%s'" % _md5)
                __[_md5].append(filename)

        for k, v in __.items():
            if len(v) > 1:
                for filename in v[1:]:
                    self.info("Duplicate file found: '%s'" % filename)
                    self.duplicatefiles.append(filename)
            self.distinctfiles.add(v[0])
            yield v[0]

        self.info("Total distinct files: %s." % len(self.distinctfiles))
        self.info("Total duplicate files: %s." % len(self.duplicatefiles))

    def _movefiles(self, destination, distinct = False, newzipfile = '', **kwds):
        if distinct:
            filelist = [i for i in self.listdistinct()]
        else:
            self.files_only = True
            filelist = filter(lambda x: not is_zipfile(x), self)
        
        moved = []
        countfiles = len(filelist)

        if not self.zipfiles:
            self.compressfiles(filelist, newzipfile)

        for filename in filelist:
            moved.append(File.move(filename, destination = destination, **kwds))

        countmoved = len(moved)
        try:
            assert countmoved == countfiles
        except AssertionError:
            self.warning("Only %s of %s files were moved to '%s'" % (countmoved, countfiles, destination))
            return [name for name in filelist if
                    not ospath.basename(name) in
                    map(ospath.basename, moved)]

        self.info("All files (%s of %s) have been successfully moved to '%s'" % (countmoved, countfiles, destination))
        if ospath.exists(self.unzipped_to):
            os.rmdir(self.unzipped_to)
        return moved

    def _walk(self):
        if not self.recursive:
            for path in (self.join(i) for i in os.listdir(self.path) if self.search(x = i)):
                if self.files_only and ospath.isdir(path):
                    continue
                yield path
        else:
            for root, dirs, files in os.walk(self.path):
                if not self.files_only:
                    files.extend(dirs)
                for fname in sorted(files):
                    path = joinpath(root, fname)
                    if not self.search(x = path):
                        continue
                    yield path

    def __iter__(self):
        for path in self._walk():
            yield path
