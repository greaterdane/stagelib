import re
import datetime
import logging
from dateutil.relativedelta import relativedelta
import pytz
import pandas as pd
from pandas.tslib import Timestamp

from generic import mergedicts, attribute_generator, logging_setup
#from files import importpandas

#pd = None
date_logger = logging.getLogger(__name__)
logging_setup(logger = date_logger)
re_DATE = re.compile(r'.*?(2\d{3})(?:[-\.\/])?(\d{2})(?:[-\.\/])?(\d{2}).*')
re_EPOCH = re.compile(r'^\d{5}(?:\.0)?$')

DATE_FORMAT_LIST = ["%m%d%Y", "%Y%m%d"]

def utcnow():
    return Timestamp(pytz.utc.localize(datetime.datetime.now()))

def n_months_ago(n):
    return datetime.datetime.today() - relativedelta(months = n)

def epoch_to_datetime(epoch):
    return Timestamp(datetime.date(1900,1,1) + datetime.timedelta(float(epoch) - 2))

def is_dayfirst(date):
    """
    Date.is_dayfirst('24/12/2015') == True
    """
    _ = re.compile(r'[-\.\/]').split(str(date))
    month = _[0]
    if len(month) > 2:
        try:
            month = _[1]
        except IndexError:
            return False
    try:
        if int(month) > 12:
            return True
        return False
    except ValueError:
        return False

def try_date_formats(date):
    for _ in DATE_FORMAT_LIST:
        try:
            return datetime.datetime.strptime(date, _)
        except ValueError as e:
            pass #; date_logger.error(e)

class BadDate(Exception):
    pass

class Date(object):
    FIELDMAP = {'mon' : 'month', 'mday': 'day', 'min': 'minute', 'sec': 'second'}

    def __init__(self, date, strfmt = '%Y-%m-%d', **kwds):
        self.strfmt = strfmt
        self.date = self.to_datetime(date, **kwds)

    def __repr__(self):
        return str(self)

    def __str__(self):
        if hasattr(self.date, 'strftime'):
            return self.date.strftime(self.strfmt)
        raise BadDate(self.date)

    @classmethod
    def parse(cls, date, fmt = False, disect = False, force = False, **kwds):
        _ = cls(date, **kwds)
        if fmt:
            try:
                return str(_)
            except BadDate as e:
                if force:
                    date_logger.warning("Value '{}' truncated.".format(date))
        elif disect:
            return _.disect()
        else:
            return _.date

    @staticmethod
    def is_epoch(date):
        return True if re_EPOCH.search(date) else False

    def to_datetime(self, date, dayfirst = False, **kwds):
        try:
            assert isinstance(date, str)
        except:
            if not date:
                return
            date = str(date)

        if Date.is_epoch(date):
            return epoch_to_datetime(date)
        try:
            return pd.to_datetime(date, dayfirst = dayfirst, **kwds)
        except ValueError as e:
            return pd.to_datetime(try_date_formats(date))
        return date

    def disect(self):
        __ = map(lambda x: (x[0].split('_')[1], x[1]),
                 attribute_generator(self.date.timetuple()))

        _ = {self.FIELDMAP.get(k, k) : v for (k, v) in __}
        return mergedicts(_, quarter = self.date.quarter)
