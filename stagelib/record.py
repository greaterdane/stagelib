import os, sys, re
from collections import OrderedDict
from functools import partial
import numpy as np
import pandas as pd
from more_itertools import unique_everseen as uniq
import usaddress
from nameparser import HumanName

from generic import strip, to_single_space, remove_non_ascii, idict
from files import ospath, readjson, joinpath, newfolder
import dataframe
from dataframe import dtypeobject
from dataframe import quickmapper

re_GARBAGEPHONE = re.compile(r'[\.\-\(\)\s]+')
re_PHONE = re.compile(r'^\d+$')
re_1800NUMBER = re.compile(r'^1-8\d{2}-')

newfolder = partial(newfolder, ospath.dirname(__file__))
LABELDIR = newfolder('config', 'addresslabels')
ZIPCODEDIR = newfolder('data', 'zipcodes')

#phone
def get_phoneorfax(x):
    number = re_GARBAGEPHONE.sub('', x)
    if re_PHONE.match(number) and not re_1800NUMBER.match(x):
        phoneorfax = "({}) {}-{}".format(number[0:3], number[3:6], number[6:10])
        if len(number) > 10:
            phoneorfax += " ext.{}".format(number[10:])
        return phoneorfax
    return x

@quickmapper
def to_phone(x):
    return get_phoneorfax(x)

#names
def getname(name):
    h = HumanName(name); h.capitalize()
    return {'firstname' : "%s %s" % (h.first, h.middle),
            'lastname' : "%s %s" % (h.last, h.suffix)}

def to_name(self):
    return pd.DataFrame([
        d for d in self.modify(
            self.notnull(),
            self.quickmap(getname)
                )], index = self.index).clean()

#address
def get_zipdata():
    __ = pd.read_csv(joinpath(ZIPCODEDIR, 'zipcodes.zip'), dtype = 'object')
    return __.assign(State = __['State']\
            .fillna('State Abbreviation')\
            .fillna('Place Name'))

def get_zipcodes(df):
    if not hasattr(df, 'state'):
        return df

    for state, data in get_zipdata().groupby('State.1'):
        msk = (df.state == state) & (no_zipcode(df))
        zipmap = idict(
            data.getmapper('Place Name', 'Zip Code'))

        df.loc[msk, 'zip'] = df.loc[msk, 'city'].map(lambda x: zipmap.get(x))
    return df

def is_valid_us_address(df):
    return (df['zip'].str.contains(r'^\d{5}(?:-\d{4})?$')) & (df.valid)

def no_zipcode(df):
    if not 'zip' in df.columns:
        df['zip'] = None
    return df.zip.isnull()

def addressconcat(df):
    return df.joinfields(fields = USAddress.fields)

@dtypeobject
def addressdisect(addresses):
    """
     Takes a series containing joined address strings as values, e.g. '1234 Main st. CITY, ST 12345-0000',
     and attempts to disect each one into individual components (address1, address2, city, state, zip).

     Parameters
     ----------
     addresses : pd.Series
    """
    disected = addresses\
        .dropna()\
        .quickmap(USAddress.parse)\
        .to_dict()

    df = get_zipcodes(
        pd.DataFrame(disected.values(),
                     index = disected.keys()))

    __ = (~no_zipcode(df)) &\
         (is_valid_us_address(df))

    if any(__):
        return df.loc[__]\
                .reindex(df.index)\
                .ix[:, USAddress.fields]

    return df.assign(
        address1 = df.address1.combine_first(addresses))

class USAddress(object):
    cnfg = readjson(joinpath(LABELDIR, 'addresslabels.json'))
    labels = {k:([v] if not isinstance(v,list) else v)
              for k,v in cnfg['labels'].items()}

    fields = sorted(labels.keys())
    states = idict(get_zipdata().getmapper('State', 'State.1'))

    def __init__(self, address):
        self.orig = address
        self.prepped = self.preclean(address)
        self.components = self.disect()
        self.components['fulladdress'] = self.prepped

    @classmethod
    def parse(cls, x):
        return cls(x).components

    @staticmethod
    def preclean(x):
        x = remove_non_ascii(
            ' '.join(i for i in uniq(x.split()))  )
        return to_single_space(x).strip()

    def __repr__(self):
        return self.components

    def getparts(self):
        try:
            tagged_address, self._type = usaddress.tag(self.prepped)
        except usaddress.RepeatedLabelError as e:
            x = OrderedDict(e.parsed_string)
            tagged_address, self._type = (x, "Questionable")
        return tagged_address

    def is_valid(self, components):
        try:
            assert components['zip']
        except:
            _ = self.components['state']
            if _ and (len(_) >= 2 or _ in self.states):
                return True
            return False
        return True

    def disect(self):
        d = {}
        parts = self.getparts()
        for k, v in self.labels.items():
            part = ' '.join(parts[i] for i in v if i in parts)
            if k == 'state':
                part = self.states.get(part, part)
            d.update({k : (part if part else None)})
        d.update({'valid' : self.is_valid(d), 'type' : self._type})
        return d

pd.Series.to_phone = to_phone
pd.Series.to_name = to_name
