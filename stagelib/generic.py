from __future__ import division

import os, re
import string
from datetime import datetime
import logging
import logging.handlers
from collections import MutableMapping
from itertools import islice
from functools import wraps, partial

LOGDIR = os.path.join(os.path.dirname(__file__), 'logs')

def removehandlers(logger):
    for handler in logger.handlers:
        logger.removeHandler(handler)
        handler.flush()
        handler.close()

def configurelogging(logger, ch = None, fh = None, formatter = '', level = logging.DEBUG, logdir = LOGDIR, extrakeys = []):
    removehandlers(logger)
    fmtstring = "%(levelname)s|%(message)s|%(asctime)s"
    if extrakeys:
        fmtstring = fmtstring + "|" + '|'.join("%" + "(%s)s" % k for k in extrakeys)
    if formatter:
        fmtstring = fmtstring + "|" + formatter

    _formatter = logging.Formatter(fmtstring)
    if not fh:
        logfile = os.path.join(logdir, logger.name + '.log')
        fh = logging.handlers.RotatingFileHandler(logfile, encoding = 'utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(_formatter)

    if not ch:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(_formatter)

    logger.setLevel(level)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def mergedicts(*dictionaries, **kwds):
    result = {}
    for d in dictionaries:
        result.update(d)
    result.update(kwds)
    return result

def reversedict(dictionary):
    return {v : k for k, v in dictionary.items()}

def chunker(iterable, chunksize = 675000):
    _ = iter(iterable)
    while True:
        __ = list(islice(_, chunksize))
        if not __:
            raise StopIteration
        yield __

def loadcontainer(func, container = dict):
    def inner(*args, **kwds):
        return container(func(*args, **kwds))
    return inner

def attribute_generator(obj, private_attrs = False, callables_only = False, key = None):
    """Takes an object, obj (can be any object), and yields
        key value pair (attribute name _> value).

    Parameters:
    -----------
    obj : Can be a class, module, string or any other data structure/object with attributes.

    [private_attrs] : Flag to additionally take private attributes.  Defaults to False.  bool
    [callables_only] : Flag to take callable items only.  Defaults to False.  bool
    [key] : Get a specific attribute.  key will take
        precedence over all flags.Defaults to None.  str
    """
    for name in dir(obj):
        if key and key != name:
            continue
        elif not private_attrs and name.startswith('_'):
            continue
        try:
            __ = getattr(obj, name)
            if not callable(__) and callables_only:
                continue
            yield name, __
        except AttributeError:
            continue

attrlist = loadcontainer(attribute_generator, container = list)
attrdict = loadcontainer(attribute_generator)

def grabfunctions(obj, module, attname):
    for name, func in attrlist(module, callables_only = True):
        setattr(obj, name, partial(func, getattr(obj, attname)))

def textstring(func):
    @wraps(func)
    def inner(x, *args, **kwds):
        try:
            return func(x, *args, **kwds)
        except (AttributeError, TypeError):
            return x
    return inner

def numberclean(x):
    if isinstance(x, unicode):
        x = str(x)
    return x.translate(None, r'$=(),%*')\
        .rstrip("-%s" % string.ascii_letters)

def numeric(func):
    @wraps(func)
    def inner(x, force = False):
        try:
            return func(x)
        except ValueError:
            try:
                return func(numberclean(x))
            except ValueError:
                if not force:
                    return x
                return
        except TypeError:
            return x
    return inner

@textstring
def strip(x, *args): return x.strip(" \t%s" % ''.join(args))

@textstring
def to_single_space(x): return re.compile(r'\s{2,}').sub(' ', x)

@textstring
def remove_non_ascii(x): return ''.join(i for i in x if ord(i)<128)

def fuzzyprep(x):
    """Remove whitespace, punctuation, and non-ascii characters
        from x in preparation for fuzzy text matching.

    Parameters:
    -----------
    x : Item or string to parse. str

    """
    x =  remove_non_ascii(x)
    if not isinstance(x, str):
        x = str(x)

    return ''.join(re.split(r'\s+', x\
        .translate(None, string.punctuation).lower()))

@numeric
def integer(x, **kwds):
    return int((x if isinstance(x, str) else str(x)).split('.')[0])

@numeric
def floating_point(x, **kwds):
    return float(x)

class idict(MutableMapping):
    """A case-insensitive dict-like object.
    Taken from "https://github.com/requests/requests-docs-it/blob/master/requests/structures.py"
    to avoid the unecessary import. Thanks requests!
    """
    def __init__(self, data = None, **kwds):
        self._store = dict()
        if not data:
            data = {}
        self.update(data, **kwds)

    @staticmethod
    def _lower(key):
        if isinstance(key, (str, unicode)):
            return key.lower()
        return key

    def __repr__(self):
        return str(dict(self.items()))
    def __setitem__(self, key, value):
        self._store[self._lower(key)] = (key, value)
    def __getitem__(self, key):
        return self._store[self._lower(key)][1]
    def __delitem__(self, key):
        del self._store[self._lower(key)]
    def __iter__(self):
        return (casedkey for casedkey, mappedvalue in self._store.values())
    def __len__(self):
        return len(self._store)
    def __eq__(self, other):
        if isinstance(other, collections.Mapping):
            return dict(self.lower_items()) == dict(idict(other).lower_items())
        return NotImplemented
    def copy(self):
        return idict(self._store.values())

    def lower_items(self):
        return [(lowerkey, keyval[1]) for
            (lowerkey, keyval) in self._store.items()]

class EasyInit(object):
    def __init__(self, *args, **kwds):
        self._kwds = {}
        self.kwds = kwds

    @staticmethod
    def get_logger_name(obj):
        return obj.__class__.__name__.lower()

    @staticmethod
    def add_logging_methods(obj, logger = None, extra = {}, **kwds):
        if not logger:
            obj._logger = logging.getLogger(EasyInit.get_logger_name(obj))
        else:
            obj._logger = logger

        removehandlers(obj._logger)
        configurelogging(obj._logger, extrakeys = extra.keys())
        for level in ['info', 'debug', 'warning', 'error', 'critical']:
            setattr(obj, level, lambda msg, level = level: getattr(obj._logger, level)(msg, extra = extra))
        return obj

    def __call__(self, func):
        @wraps(func)
        def inner(slf, *args, **kwds):
            setuplogging = kwds.pop('setuplogging', True)
            func(slf, *args, **kwds)
            if setuplogging:
                extra = {k : v for k, v in slf.__dict__.items() if v in args}
                self.add_logging_methods(slf, extra = extra)
            __ = mergedicts(kwds, self.kwds, self._kwds)
            for k, v in __.items():
                if '_logging' not in k:
                    setattr(slf, k, v)
        return inner

class GenericBase(object):
    @EasyInit()
    def __init__(self, setuplogging = True, *args, **kwds):
        if hasattr(self, '_logger'):
            self._logger.handlers = []

class Test(GenericBase):
    def __init__(self, path, setuplogging = False, *args, **kwds):
        self.path = path
        super(Test, self).__init__(path, setuplogging = setuplogging, *args, **kwds)
