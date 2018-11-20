#! /usr/bin/env python
import xml.sax
import datetime

class WikiXmlHandler(xml.sax.handler.ContentHandler):
    """Content handler for Wiki XML data using SAX"""
    def __init__(self, callback, filename):
        xml.sax.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._article_count = 0
        self._values = {}
        self._current_tag = None
        self._pages = []
        self.callback = callback
        filename = filename.split('/')[-1]
        # write to log file
        timestamp = str(datetime.datetime.now().time())
        interactions_name = "interactions_" + timestamp + filename + ".txt"
        self.fh_interactions = open(interactions_name, "a")
        log_name = "log_" + timestamp + filename + ".txt"
        self.fh_log = open(log_name, "a")

        self.starttime = datetime.datetime.now()

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
            # join together the content of
            self._values[name] = ' '.join(self._buffer)


        if name == 'page':
            # there is no reason for saving the pages in the object so following is commented out
            #self._pages.append((self._values['title'], self._values['text']))
            self._article_count += 1

            if self._article_count % 10000 == 0:
                now = datetime.datetime.now()
                self.fh_log.write("Processed " + str(self._article_count) + " articles in " + str(now - self.starttime) + '\n')

            # use callback to process 'found' page
            passed_links = self.callback(**self._values)

            if passed_links:
                self.fh_interactions.write(self._values['title'] + '\t' + ', '.join(passed_links) + '\n')







