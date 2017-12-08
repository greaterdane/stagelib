from __future__ import division
import os, sys, io, csv, re, gc, xlrd, json, zipfile
import xml.etree.cElementTree as ET
import shutil, subprocess, hashlib, contextlib
from datetime import date
from string import punctuation
from cStringIO import StringIO
from collections import Counter, defaultdict, OrderedDict
from functools import partial, wraps

from generic import *
from fieldlearner import locatefields
from timeutils import utcnow

pd = None
np = None
DELIMITERS = '|,\t;:'
re_ERROR = re.compile(r'^Skipping line (?P<line>\d+): expected (?P<expected_length>\d+) fields, saw (?P<length>\d+)$')
re_KEY = re.compile(r'((?<=get)(size|[a-z]time)|([a-z]+name|^ext$))')
re_BADTAIL = re.compile(r'(^.*?"),"\n', re.M)
files_logger = logging_setup(name = __name__)

#DECORATORS
def importpandas(func):
    @wraps(func)
    def inner(*args, **kwds):
        global pd, np
        
        if not pd:
            import pandas as pd

        if not np:
            import numpy as np

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

#PATHS
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
    return "{0}_{1}_.{2}".format(path.stem,
                                 utcnow().strftime(datefmt),
                                 path.ext)
@pathdeco
def filegetter(path, dest, **kwds):
    if path.isfile():
        func = File.shuttle
    elif path.isdir():
        func = Folder.shuttlefiles
    return func( str(path), dest, **kwds )

filemover = partial(filegetter, method = 'move')
filecopier = partial(filegetter, method = 'copy')
move_distinct_files = partial(filemover, distinct = True)

def newfolder(dirname, *args):
    nf = joinpath(dirname, *args)
    if not ospath.exists(nf):
        os.mkdir(nf)
    return nf

#ZIPFILES
@pathdeco
def filezip(path, newzipfile, mode = 'w', allowZip64 = True, compression = zipfile.ZIP_DEFLATED, **kwds):
    files_logger.info("Compressing '%s' to '%s'" % (path.basename(), newzipfile))
    with zipfile.ZipFile(newzipfile,
                         mode = mode,
                         allowZip64 = allowZip64,
                         compression = compression,
                         **kwds) as zf:
        zf.write(path.abspath(),
                 path.basename())

def fileunzip(zipname, outdir = None, recursive = False, **kwds):
    files_logger.info("Extracting contents of '%s'" % zipname)
    if not is_zipfile(zipname):
        return

    if not outdir:
        outdir = defaultdir(zipname)

    with zipfile.ZipFile(zipname) as zf:
        zf.extractall(outdir, **kwds)
        files_logger.info("Contents of '%s' extracted to '%s'" % (ospath.basename(zipname), outdir))

    if recursive:
        for path in Folder.listdir(outdir):
            fileunzip(path)
    return outdir

def is_zipfile(path):
    return zipfile.is_zipfile(path) and r'.xls' not in path

def not_zipfile(path):
    return not is_zipfile(path)

#JSON/XML
@filehandler(mode = 'r')
def readjson(fh):
    return json.loads(fh.read())

@filehandler(mode = 'wb')
def writejson(fh, data):
    fh.write(json.dumps(data,
                        sort_keys = True,
                        indent = 4,))

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
            row.update(  dict(item.values()[0])  )
        rows.append(row)        
    return rows

#PANDAS HELPERS
@importpandas
def xml2df(path, *args):
    return pd.DataFrame(  parsexml(path, *args)  )

@importpandas
def df2excel(outfile, keepindex = False, df = None, **kwds):
    xlwriter = pd.ExcelWriter(outfile)
    if df is not None:
        kwds = mergedicts({'Sheet1' : df}, kwds)

    for sheetname, _df in kwds.items():
        if keepindex:
            _df.reset_index(inplace = True)

        _df.to_excel(xlwriter,
            sheet_name = sheetname,
            index = False,
            encoding = 'utf-8')
    xlwriter.save()

#MISC
@filehandler()
def getmd5(fh): #multiprocess this in Folder
    _md5 = hashlib.md5()
    while fh.tell() != ospath.getsize(fh.name):
        _md5.update(fh.read(658760))
    return _md5.hexdigest()

@filehandler(mode = 'wb')
def createcsv(fh, fields, **kwds):
    csvwriter = csv.writer(fh, **kwds)
    csvwriter.writerow(fields)

def tsparse(timestamp, strfmt = "%Y-%m-%d %I:%M:%S"): #parse file timestamp
    return date.fromtimestamp(timestamp).strftime(strfmt)

class ospathMeta(type):
    _methods = attrdict(os.path)
    def __getattr__(cls, name):
        if name in cls._methods:
            return cls._methods[name]
        raise AttributeError

class ospath(GenericBase):
    __metaclass__ = ospathMeta

    def __init__(self, path, setuplogging = False, *args, **kwds):
        self.path = path
        super(ospath, self).__init__(path,
                                     setuplogging = setuplogging,
                                     *args, **kwds)
        grabfunctions(self, os.path, 'path')
        self.stem, self.ext = map(lambda x: x.strip('.'),
                                  self.splitext())
    def __str__(self):
        return self.path

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.path)

    @staticmethod
    def get_outfile(filename, dirname = ''):
        _ = ospath(filename).stem
        return ospath.join(dirname,
                           "%s_output.csv" % _)
    @property
    def properties(self):
        if not hasattr(self, '_properties'):
            return self._getproperties()
        return self._properties

    def _getproperties(self):
        self._properties = {}
        for k, v in self.__dict__.items():
            if not re_KEY.search(k):
                continue

            if callable(v):
                v = v()
                if 'time' in k:
                    v = tsparse(v)

            k = getsearch(re_KEY, k)
            self._properties.update({k : v})
        return self._properties

class NotSupported(Exception):
    def __init__(self, path, *args):
        self.extension = ospath(path).ext
        self.path = path
        if args:
            for i, arg in enumerate(args):
                setattr(self, "error_%s" % i, arg)
        super(NotSupported, self).__init__("File extension '%s' is currently not supported." % self.extension)

class File(ospath):
    def __init__(self, path, setuplogging = False, mode = "rb", chunksize = 5 * (1024*1024), **kwds):
        super(File, self).__init__(path, setuplogging = setuplogging, mode = mode, chunksize = chunksize, **kwds)
        self.kwds = kwds

    def __iter__(self):
        with open(self.path, self.mode) as fh:
            for i, data in enumerate(chunker(fh, chunksize = self.chunksize)):
                yield ''.join(data)

    @staticmethod
    def shuttle(filename, dest, overwrite = False, auto_rename = True, method = 'move'):
        if method not in ['move', 'copy']:
            raise Exception, "Valid methods are 'move' or 'copy'."

        if overwrite:
            auto_rename = False
    
        name = ospath.basename(filename)
        destfile = ospath(  joinpath(dest, name)  )
        flag = True
        number = 1
    
        while True:
            if not destfile.exists():
                break
            else:
                files_logger.info("File '%s' exists in '%s'." % (destfile.basename(), dest))
                if auto_rename:
                    stem, ext = destfile.splitext()
                    _ = "{}_{}{}".format(re.sub(r'_\d+$', '', stem), number, ext)
                    destfile = ospath(_)
                    files_logger.info("Auto-renaming '%s' to '%s'" % (name, destfile.basename()))
                    number += 1
                elif overwrite:
                    files_logger.info("'%s' will be overwritten." % (name))
                else:
                    flag = False; break
        if flag:
            action = re.sub(r'(^.*?)(?:e|$)',
                            r'\1ing', method).capitalize()

            files_logger.info("%s '%s' to '%s'" % (action, filename, dest))
            getattr(shutil, method)(filename, destfile.path)
        return [destfile.path]

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
        except xlrd.XLRDError as e:
            try:
                return Csv(path, **kwds)
            except (csv.Error, UnicodeDecodeError, Exception) as e:
                raise NotSupported(path, e)

    @filehandler(mode = "rb")
    def head(self, n = 50, **kwds):
        for i in chunker(self, n): return ''.join(i)

    @filehandler(mode = "U")
    def _countrows(self, chunksize = 600000):
        count, offset, size = 0, 0, os.stat(self.name).st_size
        while offset < size:
            count += self.read(chunksize).count('\n')
            offset = self.tell()
        return count

    def countrows(self, **kwds):
        if not hasattr(self, 'rows_original'):
            if isinstance(self, Excel):
                self.rows_original = sum(i.nrows for i in self.sheets)
            else:
                self.rows_original = self._countrows(**kwds)
        return self.rows_original

class Tabular(File):
    def __init__(self, path, setuplogging = True, *args, **kwds):
        super(Tabular, self).__init__(path, setuplogging = setuplogging, *args, **kwds)
        self.kwds.update(kwds, keep_default_na = False)
        self.rowsdropped = 0
        self.preprocessed = False

    @classmethod
    @importpandas
    def _iterdataframe(cls, func):
        def inner(self):
            if not self.preprocessed:
                self.preprocess()

            for df in func(self):
                yield df; gc.collect()
            gc.disable(); gc.collect()
        return inner

    @staticmethod
    def createheader(length = 10):
        return map(lambda x: "field.%s.of.%s" % (x[0], length),
                   enumerate(xrange(1, length + 1), 1))
    @property
    def properties(self):
        return mergedicts(rows_original = self.countrows(),
                          **super(Tabular, self).properties)
    @property
    def dfreader(self):
        return self._dfreader()

    def preprocess(self):
        self.countrows()
        for i in ['mode', 'lineterminator']:
            self.kwds.pop(i, '')
        return self

    def getrules(self, rows):
        if not any(i in self.kwds for i in ['header', 'skiprows', 'names']):
            skiprows, names = locatefields(rows)
            self.rowsdropped += skiprows
            
            if 'converters' not in self.kwds:
                self.kwds.update(converters = {i : str for i in names})
            return dict(skiprows = skiprows, names = names, header = None)
        return {}

    def _dfreader():
        raise NotImplementedError

class Csv(Tabular):
    def __init__(self, path, mode = "U", chunksize = 185000, **kwds):
        super(Csv, self).__init__(path, mode = mode, chunksize = chunksize, **kwds)
        self.fixcsv = re_BADTAIL.search(self.testraw)
        self.delimiter = self.sniff(self.testraw)
        self.kwds.update(low_memory = False,
                         error_bad_lines = False)

    @classmethod
    def sniff(cls, x):
        for k, v in Counter(x).most_common():
            if k in DELIMITERS:
                return str(k)
        raise csv.Error

    @staticmethod
    def errorparse(errortext):
        for error in errortext.split('\n'):
            __ = getdict(re_ERROR, error)
            if not __:
                continue
            yield dictupgrade(__, int)

    @staticmethod
    @importpandas
    def locate_badlines(path, **kwds):
        buf = StringIO()
        with RedirectStdStreams(stdout = buf, stderr = buf):
            dfreader = pd.read_csv(path, header = None,
                error_bad_lines = False, chunksize = 285000, **kwds)

            for df in dfreader:
                continue

        return list(Csv.errorparse( buf.getvalue() ))

    @staticmethod
    @filehandler(mode = 'rb')
    def getlines(fh, linenumbers):
        return [[i] + row for i, row in enumerate(csv.reader(fh), 1)
                if i in linenumbers]

    @staticmethod
    @importpandas
    def savebadlines(path, badlines, outfile = '', **kwds):
        if not outfile:
            outfile = "{}_badlines.xlsx".format(path)

        df = pd.DataFrame(badlines)
        for length, data in df.groupby('length'):
            __ = pd.DataFrame(
                Csv.getlines(path, data.line.values))

            __.rename(columns = {0 : 'line'},
                      inplace = True)

            sheets.update({ "length_{}".format(length) :  __})

        df2excel(outfile,
                 **OrderedDict( mergedicts(sheets, kwds)) )

    @property
    def testraw(self):
        return remove_non_ascii(self.head(mode = "U"))

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
                                     **self.getrules(self.testrows))
        return self._rules

    def fix(self, data):
        return re_BADTAIL.sub(r'\1\n', data)

    def fileswap(self):
        os.remove(self.path)
        os.rename(self.tempfile, self.path)

    def preprocess(self):
        super(Csv, self).preprocess()
        if self.fixcsv:
            for data in self:
                self.append(self.tempfile, self.fix(data))
                gc.disable(); gc.collect()
            self.fileswap()

    def reader(self, data):
        buf = StringIO(remove_non_ascii(data))
        return [i for i in csv.reader(buf,
                delimiter = self.delimiter, quoting = 1)]

    @Tabular._iterdataframe
    def _dfreader(self):
        self.kwds['chunksize'] = self.kwds.pop('chunksize', self.chunksize)
        __ = pd.read_csv(self.path,
                         **mergedicts(self.rules,
                                      self.kwds))
        for i, df in enumerate(__, 1):
            print; self.info("ITERATION: %s" % i)
            yield df

class IncompleteExcelFile(Exception):
    def __init__(self, sheetname):
        super(IncompleteExcelFile, self).__init__("Sheet '%s' contains exactly 65536 rows.  Data may be incomplete." % sheetname)
        self.sheetname = sheetname

class Excel(Tabular):
    def __init__(self, path, mode = 'rb', **kwds):
        super(Excel, self).__init__(path, mode = mode, **kwds)
        self.wb = xlrd.open_workbook(self.path, on_demand = True)
        self.emptysheets = []

    @property
    def sheets(self):
        return list(self.wb.sheets())

    def _csvbuffer(self, rows):
        __ = StringIO()
        cw = csv.writer(__, quoting = csv.QUOTE_ALL, lineterminator = '\n')
        cw.writerows(rows)
        return StringIO(__.getvalue())

    def reader(self, sheet, nrows = None):
        _nrows = sheet.nrows
        if not nrows or nrows > _nrows:
            nrows = _nrows

        return [map(lambda x: remove_non_ascii(str(x)),
                sheet.row_values(n)) for n in xrange(nrows)]

    def preprocess(self):
        super(Excel, self).preprocess()
        self._sheets = {}
        for sheet in self.sheets:
            if sheet.nrows > 1:
                rows = self.reader(sheet, nrows = 50)
                rules = mergedicts(self.getrules(rows),
                                   nrows = sheet.nrows, **self.kwds)
                self._sheets.update({sheet.name :  rules})

    @Tabular._iterdataframe
    def _dfreader(self):
        for name, rules in self._sheets.items():
            print; self.info("SHEET: '%s'" % name)
            if rules['nrows'] == 65536:
                raise IncompleteExcelFile(sheet.name)

            df = pd.read_excel(self.path, sheetname = name,
                               **mergedicts(rules, self.kwds))
            if df.empty:
                self.warning("'%s' CONTAINS NO DATA" % name)
                self.emptysheets.append(name); continue
            yield df
        self.wb.release_resources()

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
            filezip(path, ospath.abspath(newzipfile),
                    mode = 'a', **kwds)

    @staticmethod
    def unzipfiles(zipfiles, recursive = False, outdir = None):
        for zipname in zipfiles:
            outdir = fileunzip(zipname,
                               recursive = recursive,
                               outdir = outdir)
        return outdir

    @classmethod
    def listdir(cls, dirname, **kwds):
        fldr = cls(dirname, **kwds)
        if fldr.recursive:
            return (i for i in fldr)
        return list(fldr)

    @classmethod
    @importpandas
    def table(cls, *args, **kwds):
        return pd.DataFrame([cls(path).properties for path
                             in cls.listdir(*args, **kwds)])

    @classmethod
    def shuttlefiles(cls, dirname, dest, distinct = False, files_only = True, pattern = '', **kwds):
        fldr = cls(dirname,
                   files_only = files_only,
                   pattern = pattern)
        if distinct:
            fl = [i for i in fldr.listdistinct()]
        else:
            setattr(fldr, 'files_only', True)
            fl = filter(not_zipfile, fldr)
        return fldr._shuttlefiles(fl, dest, **kwds)

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

        self.info("Total distinct files: %s" % len(self.distinctfiles))
        self.info("Total duplicate files: %s" % len(self.duplicatefiles))

    def _shuttlefiles(self, filelist, dest, newzipfile = '', archive = True, **kwds):       
        shuttled = []
        countfiles = len(filelist)

        if kwds.get('method') == 'copy':
            archive = False

        if not self.zipfiles and archive:
            self.compressfiles(filelist, newzipfile)

        for filename in filelist:
            shuttled.extend(
                File.shuttle(filename,
                             dest = dest,
                             **kwds)); print
        count = len(shuttled)
        try:
            assert count == countfiles
        except AssertionError:
            self.warning("Only %s of %s files have been relocated to '%s'" % (count, countfiles, dest))
            return [name for name in filelist if
                    not ospath.basename(name) in
                    map(ospath.basename, shuttled)]

        if hasattr(self, 'unzipped_to') and kwds.get('method', 'move') is 'move':
            if ospath.exists(self.unzipped_to):
                self.warning("Removing '%s'" % self.unzipped_to)
                shutil.rmtree(self.unzipped_to)
        return shuttled

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
