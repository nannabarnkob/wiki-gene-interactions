#! /usr/bin/env python
import xml.sax


class WikiXmlHandler(xml.sax.handler.ContentHandler):
    """Content handler for Wiki XML data using SAX"""
    def __init__(self, callback):
        xml.sax.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._article_count = 0
        self._values = {}
        self._current_tag = None
        self._pages = []
        self.callback = callback

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
            #self._pages.append((self._values['title'], self._values['text']))
            self._article_count += 1

            gene = self.callback(**self._values)





