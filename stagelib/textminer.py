import re
from collections import defaultdict
import textract

from files import ospath, File, Folder

def extract_text(path):
    return textract.process(path)

def pdf2txt(path):
    outfile = "%s.txt" % ospath(path).stem
    if not ospath.exists(outfile):
        File.write(outfile, extract_text(path))
    return outfile

class TextSearch(GenericBase):
    def __init__(self, keywords = [], *args, **kwds):
        super(TextSearch, self).__init__(*args, **kwds)
        rgxstr = "(?:\n|^)((?:.*?%s.*?))" % '.*?|.*?'.join(keywords)\
            .replace(' ', '(?:[\s]+)?') + "(?:\n|\.(?:\s+|$))"

        self.keywords = keywords
        self.regex = re.compile(rgxstr, re.I)

    def get_matches(self, data):
        return self.regex.findall(data)

    def search(self, *args, **kwds):
        raise NotImplementedError

class FolderTextSearch(TextSearch):
    def __init__(self, keywords = [], dirname = '.'):
        super(FolderTextSearch, self).__init__(dirname = dirname, keywords = keywords)

    def get_matches(self, path):
        return super(FolderTextSearch, self).get_matches(File.read(path))

    def search(self, *args, **kwds):
        for path in Folder.listdir(self.dirname, **kwds):
            if path.lower().endswith('.pdf'):
                path = pdf2txt(path)

            yield [{'keyword' : term, 'match' : match, 'path' : path}
                    for term in self.keywords for
                    match in self.get_matches(path) if term in match]
