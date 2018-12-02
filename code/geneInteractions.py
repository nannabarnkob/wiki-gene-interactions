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

class geneInteractions:
    def arg_parser(self):
        # build argument parser
        parser = argparse.ArgumentParser(
            description='Get and visualize interactions for a wanted gene. '
                        'Example usage: ./geneInteractions.py --gene-name DNAH8 --visualize -output_format image')
        parser.add_argument('-db', '--database', type=str, default='gene-database', help="Name of database made with buildDatabase and WikiScraper(Parralelized)")
        parser.add_argument('-gene', '--gene-name', type=str, help='The gene which interactions is wanted')
        parser.add_argument('-viz', '--visualize', action='store_true', help='Whether or not you want to build a graph visualization from the interaction network')
        parser.add_argument('-print-interactions', action='store_true',
                            help='Whether or not you want to see the interactions for the gene')
        parser.add_argument('-output-fmt', '--output-format', type=str, default='image', help="The format of your graph, choices are image and d3")
        parser.add_argument('-output-method', '--output-method', type=str, default='display', help='Whether you want to visualize the graph as pop-up or save it for later')
        parser.add_argument('-output-name', '--output-name', type=str, default="gene_name_" + "interactions")
        parser.add_argument('-levels',default=1,help="Number of neighboring genes you wish to include in the visualization")
        parser.add_argument('-id','--gene_id', default=None, help="ID for search")
        self.args = parser.parse_args()
        try:
            self.args.levels = int(self.args.levels)
        except ValueError:
            pass


    def connect_to_database(self):
        db = sqlite3.connect(self.args.database)
        self.cursor = db.cursor()

    def convert_ID_to_genesymbol(self):
        self.args.gene_name = self.cursor.execute(
            'select gene_symbol from gene_table where trim(gene_id) = ?',
            (self.args.gene_id,)).fetchall()[0][0]
        print(self.args.gene_name)
        # interactions is a list of tuples

    def get_interactions(self, gene_name):
        """ Returns interaction partners for gene query """
        interactions = self.cursor.execute(
            'select gene_symbol, gene_interaction_symbol from interactions where trim(gene_symbol) = ?', (gene_name, )).fetchall()
        # interactions is a list of tuples
        return interactions


    def print_interactions(self, gene_name, interactions):
        message = [tuple[1] for tuple in interactions]
        if len(message) > 0:
            print("Interactions for", gene_name + ":\t", message)
        else:
            print("Interactions for", gene_name + ":\t None")


    def find_all_interactions(self):
        self.neighbordict = dict()
        self.neighbordict[0] = [self.args.gene_name]
        self.nodes = set([self.args.gene_name])

        # get all interactions for query gene up to args.levels
        if self.args.levels >= 1:
            new_interactions = self.all_interactions
            for i in range(1, self.args.levels):
                print("--------------------")
                print("Level", i, "neighbors")
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

        """
        # Get all interactions for  a gene
        elif self.args.levels == 'all':
            new_interactions = self.all_interactions

            while len(new_interactions) is not 0:
                new_neighbors = [neighbor[1] for neighbor in new_interactions]
                new_interactions = []
                for neighbor in new_neighbors:

                    # get interactions for new neighbors
                    interactions = self.get_interactions(neighbor)

                    new_interactions.extend(interactions) # append only new neighbors

                    # keep all interactions in this:
                    self.all_interactions.extend(interactions)

        """


    def visualize(self, format='image'):
        G = nx.DiGraph()
        G.add_node(self.args.gene_name, node_color='red')
        # first build graphs
        for interaction in self.all_interactions:
            G.add_edge(interaction[0], interaction[1])
        if format == 'image':
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

        elif format == 'd3':
            print("Visualizing using D3")
            print("Use ctrl+c to quit")
            visualize(G, config={
                'Node size': 11,
                'Charge strength' : -85.0,
                'Link distance' : 32,
                'Link width' : 1,
                'Collision' : True,
            })



    def main(self):
        self.arg_parser()
        # print namespace for args:
        print("Got arguments:", self.args)
        self.connect_to_database()
        if self.args.gene_id:
            self.convert_ID_to_genesymbol()

        # get and print level 0
        self.all_interactions = self.get_interactions(self.args.gene_name)

        if self.args.print_interactions:
            print("--------------------")
            print("Level 0 neighbors")
            self.print_interactions(self.args.gene_name, self.all_interactions)
        self.find_all_interactions()

        if self.args.visualize:
            self.visualize(format=self.args.output_format)


if __name__ == '__main__':
    print("### Running GeneInteractions 1.0 ###")
    genes = geneInteractions()
    genes.main()

