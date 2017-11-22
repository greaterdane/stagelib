import os, re
from time import sleep
from random import randint
from collections import defaultdict
import mechanize
import cookielib
from itertools import dropwhile
from bs4 import BeautifulSoup
from tqdm import tqdm

from generic import remove_non_ascii
from files import ospath, isearch

USER_AGENT = r"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"
ACCEPT = r"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"

def pause(start = 687, stop = 1387):
    for i in tqdm(xrange(randint(start, stop)), desc = "Paused"):
        sleep(.01)

def cleantag(x):
    return str(re.sub(r'\s{2,}', ' ',
        remove_non_ascii(x.text)).strip().replace('&amp;', '&'))

def cleantags(seq):
    return map(cleantag, seq)

def checktag(tag, name, text):
    if tag.name == name:
        if text in tag.attrs or isearch(text)(tag.text):
            return True
    return False

def getsoup(x):
    return BeautifulSoup(x, "lxml")

def read_soup_local(path):
    with open(path) as r:
        return getsoup(r.read())

def write_soup_local(soup, output_file):
    with open(output_file, 'w') as w:
        w.write(soup.prettify().encode('utf-8'))

def find_redtext(tag):
    __ = tag.findAll('span', {'class' : ["PrintHistRed"]})
    if not __:
        return tag.findAll('font', {'color' : "#ff0000"})
    return __

def find_checkedboxes(soup):
    return soup.find_all('img', {'alt' : re.compile(r'(?<!not )(?:changed|selected|checked)', re.I)})

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
    def statuscode(self):
        return self.resp.code

    @property
    def data(self):
        return self.resp.get_data()

    @property
    def soup(self):
        return getsoup(self.data)

    @property
    def currenturl(self):
        return self.geturl()

    @property
    def throttled(self):
        return self.statuscode in [503, 403, 401]

    def back(self):
        try:
            super(HomeBrowser, self).back()
            if hasattr(self, '_logger'):
                self.logger.info("Back at '{}'".format(self.geturl()))
        except mechanize.BrowserStateError:
            pass

    def download(self, url, outfile = None):
        if not outfile:
            outfile = url

        if not ospath.exists(outfile):
            self.retrieve(url, outfile)[0]

    def checkurl(self, pattern, url):
        return isearch(pattern)(url)

    def check_currenturl(self, pattern):
        return checkurl(pattern, self.currenturl)

    def buildlink(self, url):
        return "%s%s" % (self.starturl, url)

    def filterlinks(self, pattern):
        return (x for x in self.links() if re.search(pattern, x.url, re.I))

    def browse(self, *args, **kwds):
        raise NotImplementedError
