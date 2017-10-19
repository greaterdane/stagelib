import os
import sys
import re

from collections import OrderedDict
import numpy as np
import pandas as pd
from more_itertools import unique_everseen as uniq
import usaddress
from nameparser import HumanName

from generic import strip, to_single_space, remove_non_ascii
from fileIO import OSPath, from_json, mkpath
import dataframe
from dataframe import quickmapper

def address_fields(df):
    return df.filter_fields(items = USAddr.labels.keys())
    
def parse_addresses(df):
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
    amap = df.joinfields(df[address_fields(df)])\
        .dropna()\
        .quickmap(USAddr.disect)\
        .to_dict()

    return pd.DataFrame(amap.values(),
        index = amap.keys())

def align_addresses(df):
    addresses = parse_addresses(df)
    addresses = addresses.loc[
        (addresses.valid) &\
        (addresses.zip.notnull()) &\
        (addresses.state.quickmap(lambda x: len(x) == 2))
            ]

    #####NOT DONE!!!
    #addresses.reindex(df.index).combine_first(df)
    #df.loc[addresses.index, address_fields(df)] = addresses
    #return df.combine_first(addresses)

@quickmapper
def parse_name(name):
    h = HumanName(name); h.capitalize()
    return {'firstname' : "%s %s" % (h.first, h.middle),
            'lastname' : "%s %s" % (h.last, h.suffix)}
    
def parse_names(series):
    return pd.DataFrame([
        d for d in np.where(series.isnull(),
        {'firstname' : np.nan, 'lastname' : np.nan}, parse_name(series))
            ], index = series.index).clean()

class USAddr(object):
    cnfg = from_json(mkpath(*[os.path.dirname(__file__), 'config','addresslabels','address_labels.json']))
    labels = {k:([v] if not isinstance(v,list) else v)
              for k,v in cnfg['labels'].items()}

    states = map(unicode.lower,cnfg['states'].keys() +\
                 cnfg['states'].values())

    def __init__(self,address):
        self.orig = address
        self.prepped = self.preclean(address)
        self.components = self.parse()

    @classmethod
    def disect(cls, x):
        return cls(x).components

    @staticmethod
    def preclean(x):
        return strip(remove_non_ascii(
            to_single_space(
                ' '.join(i for i in uniq(x.split()))
                    )))

    def __repr__(self):
        return "\n".join(" : ".join(map(str, [k, v])) for k,v in self.components.items())

    def get_parts(self):
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

    def parse(self):
        parts = self.get_parts(); d = {};
        for mylabel, label in self.labels.items():
            part = ' '.join(parts[i] for i in label if i in parts)
            d.update({mylabel : (part if part else None)})
        d.update({'valid' : self.is_valid(d), 'type' : self.type})
        return d