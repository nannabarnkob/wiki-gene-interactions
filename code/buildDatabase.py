#! /usr/bin/env python

from BloomFunctions import BloomFunctions

bloomfilter = BloomFunctions('../data/gene_symbol_list.txt')

for line in XML:
    if bloomfilter.classify(word):
        "INSERT word INTO interactiontable.partners WHERE gene == y"
