#! /usr/bin/env python
import xml.sax
import datetime


class WikiXmlHandler(xml.sax.handler.ContentHandler):
    """
    Content handler for Wiki XML data using SAX
    This class also needs callback function for how to process each wikipedia page and a means of communication with the 
    database where output from 'scraping' is stored 
    """


    def __init__(self, callback, filename, cursor, db, log=False):
        xml.sax.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._article_count = 0
        self._values = {}
        self._current_tag = None
        self._pages = []
        self.callback = callback
        filename = filename.split('/')[-1]

        self.log = log
        # write to log file
        if self.log==True:
            timestamp = str(datetime.datetime.now().time())
            interactions_name = "interactions_" + timestamp + filename + ".txt"
            self.fh_interactions = open(interactions_name, "a")
            log_name = "log_" + timestamp + filename + ".txt"
            self.fh_log = open(log_name, "a")
            self.starttime = datetime.datetime.now()

        self.cursor = cursor
        self.db = db
        self._count_wrong_titles = 0
        self._count_wrong_interactions = 0
        self._count_raw_links = 0


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
            self._article_count += 1

            if self.log:
                if self._article_count % 10000 == 0:
                    now = datetime.datetime.now()
                    self.fh_log.write(
                        "Processed " + str(self._article_count) + " articles in " + str(now - self.starttime) + '\n')

            # use callback to process 'found' page
            #(raw_link_count, passed_links) = self.callback(**self._values)
            article_results = self.callback(**self._values)

            if article_results:
                if self.log: self.fh_interactions.write(self._values['title'] + '\t' + ', '.join(passed_links) + '\n')
                raw_link_count, passed_links = article_results
                self._count_raw_links += raw_link_count
                # add interactions to the database
                self.add_interactions(passed_links)


    def add_interactions(self, passed_links):

        # Main gene which has interactions
        main_gene = self._values['title']



        # Find gene symbols for main gene if it's an alias
        main_gene_symbols = self.cursor.execute(
            "SELECT DISTINCT CASE WHEN COUNT(1) > 0 THEN gene_symbol ELSE 0 END FROM aliases WHERE trim(gene_alias) = ? OR trim(gene_symbol) = ?",
            (main_gene, main_gene)).fetchall()

        if main_gene_symbols[0][0] == 0:
            # check for main symbol
            gene_table_symbols = self.cursor.execute(
                "SELECT DISTINCT CASE WHEN COUNT(1) > 0 THEN gene_symbol ELSE 0 END FROM gene_table WHERE trim(gene_symbol) = ?",
                (main_gene, )).fetchall()
            # this means  that
            if gene_table_symbols[0][0] == 0:
                self._count_wrong_titles += 1
                # was not found in the database at all
                return
            else:
                # if main symbol was not found, the correct symbol is the one found in the gene table, reassign:
                main_gene_symbols = gene_table_symbols

        # Unique values of passed_links
        uniq_passed_links = set(passed_links)

        # For each gene symbol in all gene symbols coming from the passed_links
        for gene_symbol in main_gene_symbols:

            # For each link in (unique)passed_links i.e. interactions
            for link in uniq_passed_links:

                # If an interaction is an alias - find its symbol
                interaction_symbols = self.cursor.execute(
                    "SELECT DISTINCT CASE WHEN  COUNT(1) > 0 THEN gene_symbol ELSE 0 END FROM aliases WHERE trim(gene_alias) = ? OR trim(gene_symbol) = ?",
                    (link, link)).fetchall()


                if interaction_symbols[0][0] == 0:
                    check_gene_table = self.cursor.execute(
                        "SELECT DISTINCT CASE WHEN COUNT(1) > 0 THEN gene_symbol ELSE 0 END FROM gene_table WHERE trim(gene_symbol) = ?",
                        (link,)).fetchall()
                    if check_gene_table[0][0] == 0:
                        self._count_wrong_interactions += 1
                        print(link)
                        continue
                    else:
                        interaction_symbols = check_gene_table

                # For each symbol of an interaction
                for interaction in interaction_symbols:

                    # Extract the string from gene_symbol
                    gs = gene_symbol[0]

                    # We wish not to insert a gene and itself as an interaction
                    if gs != interaction[0]:

                        # Insert if the row does'nt already exist
                        self.cursor.execute(
                            "INSERT INTO interactions SELECT ?1,?2,?3,?4 WHERE NOT EXISTS(SELECT 1 FROM interactions WHERE gene_alias = ?1 AND gene_symbol = ?2 AND gene_interaction_alias = ?3 AND gene_interaction_symbol = ?4)",
                            (main_gene, gs, link, interaction[0]))
                self.db.commit()

    def get_counter(self):
        return self._count_wrong_titles, self._count_wrong_interactions