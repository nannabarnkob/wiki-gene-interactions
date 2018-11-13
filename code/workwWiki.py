import subprocess
import xml.sax


class WikiXmlHandler(xml.sax.handler.ContentHandler):
    """Content handler for Wiki XML data using SAX"""

    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._values = {}
        self._current_tag = None
        self._pages = []

    def characters(self, content):
        """Characters between opening and closing tags"""
        if self._current_tag:
            self._buffer.append(content)

    def startElement(self, name, attrs):
        """Opening tag of element"""
        if name in ('title', 'text'):
            self._current_tag = name
            self._buffer = []

    def endElement(self, name):
        """Closing tag of element"""
        if name == self._current_tag:
            self._values[name] = ' '.join(self._buffer)

        if name == 'page':
            self._pages.append((self._values['title'], self._values['text']))


# external hard drive path
data_path = '/Volumes/Seagate Backup Plus Drive/Wikipedia/enwiki-20181101-pages-articles-multistream.xml.bz2'
# Iterate through compressed file one line at a time

# Object for handling xml
handler = WikiXmlHandler()
# Parsing object
parser = xml.sax.make_parser()
parser.setContentHandler(handler)
from datetime import datetime
print("Begin at", datetime.now())
# Iteratively process file
for line in subprocess.Popen(['bzcat'],
                             stdin=open(data_path),
                             stdout=subprocess.PIPE).stdout:
    parser.feed(line)
    if len(handler._pages) % 1000 == 1:
        print(len(handler._pages))

print("End at", datetime.now())
# I currently don't know how long it would take to process all the lines
"""
    # Stop when 3 articles have been found
    if len(handler._pages) > 2:
        break
"""
# If we inspect handler._pages , we’ll see a list, each element of which is a tuple with the title and text of one article
# Title will always be handler._pages[0] and content will be handler._pages[1]
# Thus pages[0] should be 'from' and [pages[1]...] 'to' (list of 'hits')

import mwparserfromhell

# Create the wiki article
wiki_page = mwparserfromhell.parse(handler._pages[1][1])
# Find the wikilinks
wikilinks = [x.title for x in wiki_page.filter_wikilinks()]
print("Some links in this article", wikilinks[:5])
