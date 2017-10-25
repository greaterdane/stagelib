import os, sys, re
from collections import OrderedDict
import numpy as np
import pandas as pd
from more_itertools import unique_everseen as uniq
import usaddress
from nameparser import HumanName

from generic import strip, to_single_space, remove_non_ascii
from fileIO import OSPath, from_json, mkpath, mkdir
import dataframe

re_GARBAGEPHONE = re.compile(r'[\.\-\(\)\s]+')
re_PHONE = re.compile(r'^\d+$')
re_1800NUMBER = re.compile(r'^1-8\d{2}-')

def get_phoneorfax(x):
    number = re_GARBAGEPHONE.sub('', x)
    if re_PHONE.match(number) and not re_1800NUMBER.match(x):
        phoneorfax = "({}) {}-{}".format(number[0:3], number[3:6], number[6:10])
        if len(number) > 10:
            phoneorfax += " ext.{}".format(number[10:])
        return phoneorfax
    return x

def getname(name):
    h = HumanName(name); h.capitalize()
    return {'firstname' : "%s %s" % (h.first, h.middle),
            'lastname' : "%s %s" % (h.last, h.suffix)}

def parseaddresses(df):
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
    fields = sorted(df.filter_fields(items = USAddress.labels.keys()))
    addressmap = df.joinfields(fields = fields)\
        .dropna()\
        .quickmap(USAddress.parse)\
        .to_dict()

    addresses = pd.DataFrame(addressmap.values(),
        index = addressmap.keys())

    #convert all states into two character units
    #obtain zip codes using cities and states before validation
    addresses = addresses.loc[
        (addresses.valid) &\
        (addresses.zip.notnull()) #&\
        #(addresses.state.quickmap(lambda x: len(x) == 2))
            ] ##HERE!!

class USAddress(object):
    LABELDIR = mkdir(*[os.path.dirname(__file__), 'config','addresslabels'])
    cnfg = from_json(mkpath(LABELDIR, 'addresslabels.json'))
    labels = {k:([v] if not isinstance(v,list) else v)
              for k,v in cnfg['labels'].items()}

    states = map(unicode.lower,cnfg['states'].keys() +\
                 cnfg['states'].values())

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
            d.update({mylabel : (part if part else None)})
        d.update({'valid' : self.is_valid(d), 'type' : self.type})
        return d
