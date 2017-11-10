import os, sys, re
from collections import OrderedDict
from functools import partial
import numpy as np
import pandas as pd
from more_itertools import unique_everseen as uniq
import usaddress
from nameparser import HumanName

from generic import strip, to_single_space, remove_non_ascii, idict
from fileIO import OSPath, from_json, mkpath, mkdir, get_homedir
import dataframe
from dataframe import quickmapper

re_GARBAGEPHONE = re.compile(r'[\.\-\(\)\s]+')
re_PHONE = re.compile(r'^\d+$')
re_1800NUMBER = re.compile(r'^1-8\d{2}-')

newfolder = partial(mkdir, get_homedir())
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
    __ = pd.read_csv(mkpath(ZIPCODEDIR, 'zipcodes.zip'), dtype = 'object')
    return __.assign(State = __['State']\
            .fillna('State Abbreviation')\
            .fillna('Place Name'))

def get_zipcodes(df):
    if not hasattr(df, 'state'):
        return df

    stategroups = get_zipdata().groupby('State.1')
    for state, data in stategroups:
        mask = (df.state == state) & (df.zip.isnull())
        zipmap = idict(data.get_mapper('Place Name', 'Zip Code'))
        df.loc[mask, 'zip'] = df.loc[mask, 'city'].map(lambda x: zipmap.get(x))
    return df

def is_validaddress(df):
    return (df['zip'].str.contains(r'^\d{5}(?:-\d{4})?$')) & (df.valid)

def to_address(df):
    """
    Attempts to parse DataFrame addresses into individual components.
    DataFrame is expected to have one or more of the following fields:
     - address1
     - address2
     - address3
     - city
     - state
     - zip
     
     Parameters
     ----------
     df : pd.DataFrame
    """
    fields = sorted(USAddress.labels.keys()) + ['fulladdress']
    fulladdresses = df.joinfields(fields = fields)
    if fulladdresses.isnull().all():
        return df

    df['fulladdress'] = fulladdresses
    _parsed = fulladdresses\
        .dropna()\
        .quickmap(USAddress.parse)\
        .to_dict()

    df[fields] = get_zipcodes(
        pd.DataFrame(_parsed.values(),
            index = _parsed.keys())
                ).loc[is_validaddress, fields
                    ].reindex(df.index).ix[:, fields]

    df['address1'] = df['address1'].combine_first(fulladdresses)
    return df

class USAddress(object):
    cnfg = from_json(mkpath(LABELDIR, 'addresslabels.json'))
    labels = {k:([v] if not isinstance(v,list) else v)
              for k,v in cnfg['labels'].items()}

    states = idict(get_zipdata().get_mapper('State', 'State.1'))

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
        return strip(remove_non_ascii(
            to_single_space(
                ' '.join(i for i in uniq(x.split()))
                    )))

    def __repr__(self):
        return "\n".join(" : ".join(map(str, [k, v])) for k,v in self.components.items())

    def getparts(self):
        try:
            tagged_address, self.type = usaddress.tag(self.prepped)
        except usaddress.RepeatedLabelError as e:
            tagged_address, self.type = (OrderedDict(e.parsed_string), "Questionable")
        return tagged_address

    def is_valid(self, addr_dict):
        try:
            assert(addr_dict['zip']); return True
        except AssertionError:
            _ = addr_dict['state']
            if _ and (len(_) >= 2 or _ in self.states):
                return True
            return False

    def disect(self):
        parts = self.getparts(); d = {};
        for mylabel, label in self.labels.items():
            part = ' '.join(parts[i] for i in label if i in parts)
            if mylabel == 'state':
                part = self.states.get(part, part)
            d.update({mylabel : (part if part else None)})
        d.update({'valid' : self.is_valid(d), 'type' : self.type})
        return d

pd.Series.to_phone = to_phone
pd.Series.to_name = to_name
pd.DataFrame.to_address = to_address
