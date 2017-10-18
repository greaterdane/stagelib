import os
import re
from time import sleep
from random import randint
from collections import defaultdict
import mechanize
import cookielib
from itertools import dropwhile
from bs4 import BeautifulSoup
from tqdm import tqdm

from generic import remove_non_ascii
from fileIO import OSPath

USER_AGENT = r"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"
ACCEPT = r"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"

def pause(start = 687, stop = 1387):
    for i in tqdm(xrange(randint(start, stop)), desc = "Paused"):
        sleep(.01)

def clean_tag(x):
    return re.sub(r'\s{2,}', ' ', remove_non_ascii(x.text)).strip()

def clean_tags(seq):
    return map(clean_tag, seq)

def check_tag(tag, name, attr_text):
    if tag.name == name:
        if attr_text in tag.attrs or re.search(attr_text, tag.text, re.I):
            return True
    return False

def get_soup(x):
    return BeautifulSoup(x, "lxml")

def read_soup_local(path):
    with open(path) as r:
        return get_soup(r.read())

def write_soup_local(soup, output_file):
    with open(output_file, 'w') as w:
        w.write(soup.prettify().encode('utf-8'))

class HomeBrowser(mechanize.Browser, object):
    def __init__(self, starturl = 'www.google.com'):
        super(HomeBrowser, self).__init__()
        self.addheaders = [('User-Agent',USER_AGENT), ('Connection','keep-alive'), ('accept', ACCEPT)]
        self.set_cookiejar = cookielib.LWPCookieJar()
        self.set_handle_equiv(True)
        self.set_handle_redirect(True)
        self.set_handle_robots(False)
        self.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time = 1)
        self.set_debug_responses(True)
        self.set_debug_redirects(True)
        self.set_debug_http(True)
        self.starturl = starturl
        self.open(starturl)

    def __repr__(self):
        return self.starturl

    @property
    def resp(self):
        return self.response()

    @property
    def status_code(self):
        return self.resp.code

    @property
    def data(self):
        return self.resp.get_data()

    @property
    def soup(self):
        return BeautifulSoup(self.data, "lxml")

    def back(self):
        try:
            super(HomeBrowser, self).back()
            if hasattr(self, 'logger'):
                self.logger.info("Back at '{}'".format(self.geturl()))
        except mechanize.BrowserStateError:
            pass

    def download(self, url, output_file = None):
        if not output_file:
            output_file = url

        if not OSPath.exists(output_file):
            self.retrieve(url, output_file)[0]

    def check_current_url(self, pattern):
        return re.search(pattern, self.geturl())

    def build_link(self, url):
        return "%s%s" % (self.starturl, url)

    def filter_links(self, pattern):
        return (x for x in self.links() if re.search(pattern, x.url))

    def browse(self, *args, **kwds):
        raise NotImplementedError

    def findtag(self, tagname, findall = False, **kwds):
        fname = 'find'
        if findall:
            fname = 'find_all'
        return getattr(self.soup, fname)(tagname, **kwds)
