import mmh3
import numpy as np


class BloomFilter:
    # Setup useful variables in here
    def __init__(self, m):
        # received from input set S (m = |S|)
        self.m = m
        self.n = 10*self.m
        self.k = int(self.n/self.m)
        self.A = np.zeros(self.n)

    # Called once for all good genes
    def train(self, gene):
        # compute hash values for all url in S
        index_j = []
        for seed in range(self.k):
            hash_val = mmh3.hash(gene, seed) % self.n
            index_j.append(hash_val)
        self.A[index_j] = 1

    # Should return true if the gene is good, otherwise false.
    def classify(self, gene):
        index_j = []
        for seed in range(self.k):
            hash_val = mmh3.hash(url, seed) % self.n
            index_j.append(hash_val)
        if self.A[index_j].all() == 1:
            return True
