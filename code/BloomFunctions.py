import mmh3
import numpy as np
import sys
import random


class BloomFunctions:
    def __init__(self, filename):
        # received from input set S (m = |S|)
        self.load(filename)
        self.main()

    def load(self, filename):
        """ Load training data """
        self.goodData = []
        with open(filename) as fd:
            x = fd.readline()
            line = x.replace("\n", "")
            while line:
                self.goodData.append(line)
                x = fd.readline()
                line = x.replace("\n", "").replace("\r", "")

        self.m = len(self.goodData)
        self.n = 20*self.m
        self.k = int(self.n/self.m)
        self.A = np.zeros(self.n)

        print("Read ", len(self.goodData), " good genes")

    def main(self):
        self.trainAll()
        self.check()

    def trainAll(self):
        print("Training filter...")
        for gene in self.goodData:
            self.train(gene)

    def train(self, gene):
        # compute hash values for all genes in S
        index_j = []
        for seed in range(self.k):
            hash_val = mmh3.hash(gene, seed) % self.n
            index_j.append(hash_val)
        self.A[index_j] = 1

    # Should return true if the gene is good, otherwise false.
    def classify(self, gene):
        """ Classifies gene according to filter """
        index_j = []
        for seed in range(self.k):
            hash_val = mmh3.hash(gene, seed) % self.n
            index_j.append(hash_val)
        if self.A[index_j].all() == 1:
            return True

    def createGene(self):
        """ Create random genes for testing """
        # Beginning and end of the alphabet for random gene generation
        Astart = 97
        Zend = 122
        return "".join(map(lambda i: chr(random.randint(Astart, Zend)), range(random.randint(4, 8)))).upper()

    def check(self):
        """ Makes a series of random genes for testing false positive/negative rate """
        gene = ""
        ok = False
        falsePos = 0
        falseNeg = 0
        checkSize = 2 * len(self.goodData)

        print("Checking filter...")

        for i in range(1, checkSize):
            if i % 50000 == 0:
                print((i / checkSize * 100), "percent done")
                print("Classifications: ", i)
                print("False negative rate:", falseNeg / i)
                print("False positive rate:", falsePos / i)

            r = random.randint(0, 10)
            # test random gene from the good data
            if(r == 7):
                idx = random.randint(0, self.m - 1)
                gene = self.goodData[idx]

                if not self.classify(gene):
                    falseNeg += 1
            else:
                gene = self.createGene()

                if self.classify(gene):
                    falsePos += 1

        print("Total classifications: ", checkSize)
        print("False negative rate:", falseNeg / checkSize)
        print("False positive rate:", falsePos / checkSize)

        if falseNeg > 0:
            print("FAIL: some good strings were classified as bad")
        elif (falsePos / checkSize) > 0.05:
            print("FAIL: the rate of false positives is larger than 0.05:",
                  (falsePos / checkSize))
        else:
            print("Correct")
