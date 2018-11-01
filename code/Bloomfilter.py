import BitVector
import mmh3
import numpy as np
import pdb


class UrlBloomFilter:
    # Setup useful variables in here
    def __init__(self, m):
        # received from input set S (m = |S|)
        self.m = m
        self.n = 10*self.m
        self.k = int(self.n/self.m)
        self.A = np.zeros(self.n)

    # Called once for all good URLs
    def train(self, url):
        # compute hash values for all url in S
        index_j = []
        for seed in range(self.k):
            hash_val = mmh3.hash(url, seed) % self.n
            index_j.append(hash_val)
        self.A[index_j] = 1
#        np.save('filter_obj', self.A)

    # Should return true if the URL is good, otherwise false.
    def classify(self, url):
        index_j = []
        for seed in range(self.k):
            hash_val = mmh3.hash(url, seed) % self.n
            index_j.append(hash_val)
        if self.A[index_j].all() == 1:
            return True
