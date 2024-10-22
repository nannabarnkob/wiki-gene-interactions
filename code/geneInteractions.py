#!/usr/bin/env python3
# brugeren giver input (argparser)
# - search gene - return interaction / error
# main.py -G "BRCA1"
# main.py --gene-search "BRCA1"

# Select * from gene_interaction where


import argparse
import sqlite3
import matplotlib.pyplot as plt
import networkx as nx
from netwulf import visualize
#import pygraphviz
import pydot
from networkx.drawing.nx_pydot import graphviz_layout
import pdb
import os
import subprocess

class geneInteractions:
    def arg_parser(self):
        # build argument parser
        parser = argparse.ArgumentParser(
            description='Get and visualize interactions for a wanted gene. '
                        'Example usage: ./geneInteractions.py -gene ZWINT -print --visualize -output-fmt image')
        parser.add_argument('-db', '--database', type=str, default='gene-database',
                            help="Name of database made with buildDatabase and WikiScraper(Parralelized)")
        parser.add_argument('-gene', '--gene-name', type=str,
                            help='The gene which interactions is wanted (alias or main symbol)')
        parser.add_argument('-id', '--gene_id', default=None,
                            help="Use gene ID for search query instead of gene name.")
        parser.add_argument('-viz', '--visualize', action='store_true',
                            help='Whether or not you want to build a graph visualization from the interaction network')
        parser.add_argument('-print', '--print-interactions', action='store_true', default=True,
                            help='Whether or not you want to see the interactions for the gene printed to stdout')
        parser.add_argument('-quiet', action='store_true',
                            help="Suppress interaction output both in terms of stdout and sif-file")
        parser.add_argument('-output-fmt', '--output-format', type=str, default='image',
                            help="The format of your graph, choices are image and d3. Image will use networkx "
                                 "and is better with smaller graphs.")
        parser.add_argument('-output-method', '--output-method', type=str, default='display',
                            help='Whether you want to visualize the graph as pop-up or save it for later. '
                                 'Only works with image.')
        parser.add_argument('-output-name', '--output-name', type=str, default="gene_name_" + "interactions")
        parser.add_argument('-levels',default=1,
                            help="Number of neighboring genes you wish to include in the visualization")
        parser.add_argument('-sif', '--sif', action='store_true',
                            help="Write to sif. If this flag is not given, the interactions are written to stdout")
        self.args = parser.parse_args()
        try:
            self.args.levels = int(self.args.levels)
        except ValueError:
            pass


    def connect_to_database(self):
        """ Set of connection to sqlite """
        db = sqlite3.connect(self.args.database)
        self.cursor = db.cursor()

    def convert_ID_to_genesymbol(self):
        """ Look up ID to convert to main gene symbol """
        self.args.gene_name = self.cursor.execute(
            'select gene_symbol from gene_table where trim(gene_id) = ?',
            (self.args.gene_id,)).fetchall()[0][0]
        print(self.args.gene_name)

    def get_interactions(self, gene_name):
        """ Returns interaction partners for gene query """
        interactions = self.cursor.execute(
            'select gene_symbol, gene_interaction_symbol from interactions where trim(gene_symbol) = ?', (gene_name, )).fetchall()
        # interactions is a list of tuples
        return interactions


    def print_interactions(self, gene_name, interactions):
        """" Print interactions either to stdout or to sif file"""
        gene_list = [tuple[1] for tuple in interactions]
        # assess whether we're writing to sif-file or printing to screen
        if self.args.sif:
            for gene in gene_list:
                self.fh.write(gene_name + ' pp ' + gene + '\n')
        else:
            if len(gene_list) > 0:
                print("Interactions for", gene_name + ":\t" + ', '.join(gene_list))
            else:
                print("Interactions for", gene_name + ":\t None")

    def pretty_print(self, i):
        """ Make a nice division in the stdout output """
        if not self.args.sif:
            print("--------------------")
            print("Level", i+1 ,"neighbors")


    def find_level_interactions(self):
        """ get all interactions for query gene up to args.levels """
        new_interactions = self.all_interactions
        for i in range(1, self.args.levels):
            if self.args.print_interactions: self.pretty_print(i)
            # get neighbors from the tuples
            neighbors = list(set([neighbor[1] for neighbor in new_interactions]))
            # add the ones we haven't seen before to the "levels" dict:
            self.neighbordict[i] = [neighbor for neighbor in neighbors if neighbor not in self.nodes]
            # keep track of already 'seen' nodes
            self.nodes.update(neighbors)
            new_interactions = []
            # collect all the new interactions
            for neighbor in neighbors:
                neighbor_interaction = self.get_interactions(neighbor)
                if self.args.print_interactions: self.print_interactions(neighbor, neighbor_interaction)
                # append list of interactions to all interactions
                self.all_interactions.extend(neighbor_interaction)
                new_interactions.extend(neighbor_interaction)

        # for the last neighbors
        neighbors = list(set([neighbor[1] for neighbor in new_interactions]))
        self.neighbordict[self.args.levels] = list(
            set([neighbor for neighbor in neighbors if neighbor not in self.nodes]))
        self.nodes.update(neighbors)

        print("Total number of nodes:", len(self.nodes))


    def find_all_interactions(self):
        """ Get all interactions for  a gene """
        new_interactions = self.all_interactions
        i = 0
        neighbor_count = 0
        while len(new_interactions) is not 0:
            i += 1
            if self.args.print_interactions: print("Total number of unique neighbors:", len(self.nodes))


            # get neighbors from the tuples
            neighbors = list(set([neighbor[1] for neighbor in new_interactions]))

            neighbor_count += len(neighbors)


            # add the ones we haven't seen before to the "levels" dict:
            self.neighbordict[i] = [neighbor for neighbor in neighbors if neighbor not in self.nodes]
            # keep track of already 'seen' nodes
            self.nodes.update(neighbors)
            new_interactions = []


            if len(neighbors) == 0 or len(self.nodes) >= 500:
                print("Node count exceeded 500 at level", i, "\nWe recommend using the level parameter")
                break

            if self.args.print_interactions: self.pretty_print(i)
            for neighbor in self.neighbordict[i]:
                # get interactions for new neighbors
                neighbor_interaction = self.get_interactions(neighbor)
                # print neighbors as we go
                if self.args.print_interactions: self.print_interactions(neighbor, neighbor_interaction)
                new_interactions.extend(neighbor_interaction) # append only new neighbors
                # keep all interactions in this:
                self.all_interactions.extend(neighbor_interaction)

        # for the last neighbors
        neighbors = list(set([neighbor[1] for neighbor in new_interactions]))
        self.neighbordict[self.args.levels] = list(
            set([neighbor for neighbor in neighbors if neighbor not in self.nodes]))
        self.nodes.update(neighbors)
        print("Total number of nodes:", len(self.nodes))



    def visualize(self):
        """ Visualize the interaction data either as a d3 graph or a networkx image """
        G = nx.DiGraph()
        # first build graphs
        for interaction in self.all_interactions:
            G.add_edge(interaction[0], interaction[1])

        if self.args.output_format == 'image':
            print("Visualizing using networkx")

            nlayout = graphviz_layout(G, prog="neato")

            # make conditional coloring
            color_map = []

            for i, key in enumerate(self.neighbordict):
                [color_map.append(i) for node in self.neighbordict[key]]

            """
            # conditional coloring where only center node i colored
            for node in G:
                if node == self.args.gene_name:
                    color_map.append('lightgreen')
                else:
                    color_map.append('lightblue')
            """

            nx.draw(G, nlayout, with_labels=True, node_size=1200, font_size=10, node_color=color_map,  cmap=plt.cm.summer)

            if self.args.output_method == 'display':
                plt.show()
            elif self.args.output_format == 'save':
                plt.savefig(self.args.output_name+ ".png")

        elif self.args.output_format == 'd3':
            print("Visualizing using D3")
            print("Use ctrl+c to quit")
            visualize(G, config={
                'Node size': 11,
                'Charge strength' : -85.0,
                'Link distance' : 32,
                'Link width' : 1,
                'Collision' : True,
            })


    def find_interactions(self):
        """ Wrapper for finding all interactions up to args.levels or 'all' """
        # get and print level 0
        self.all_interactions = self.get_interactions(self.args.gene_name)  # all interactions at this point
        self.neighbordict = dict()
        self.neighbordict[0] = [self.args.gene_name]
        self.nodes = set([self.args.gene_name])


        if self.args.print_interactions:
            print("### Interactions for", self.args.gene_name, " ###")
            self.pretty_print(0)
            self.print_interactions(self.args.gene_name, self.all_interactions)

        # function call based on whether level arg is an integer or 'all'
        if type(self.args.levels) == int:
            self.find_level_interactions()
        elif self.args.levels == 'all':
            self.find_all_interactions()

    def process_args(self):
        """ Process input arguments """
        if self.args.gene_id:
            self.convert_ID_to_genesymbol()

        # open sif-file
        if self.args.sif:
            self.fh = open('gene-interactions-temp.sif', 'w')

        # turn off printing
        if self.args.quiet:
            self.args.print_interactions = False

    def close_sif(self):
        """ Make all lines in the sif file unique """
        # close temporary file
        self.fh.close()
        pwd = os.getcwd()
        # only keep unique interactions
        command = 'sort -u "{oldfile}" > "{newfile}"'.format(oldfile=pwd + '/gene-interactions-temp.sif',
                                                             newfile=pwd + '/gene-interactions.sif')
        subprocess.call(command, shell=True)
        # remove the temporary file
        os.remove(pwd + '/gene-interactions-temp.sif')


    def main(self):
        self.arg_parser()
        # print namespace for args:
        print("Got arguments:", self.args)
        self.connect_to_database()
        self.process_args()
        self.find_interactions()

        if self.args.visualize:
            self.visualize()
        if self.args.sif:
            self.close_sif()


if __name__ == '__main__':
    print("### Running GeneInteractions 1.0 ###")
    genes = geneInteractions()
    genes.main()

