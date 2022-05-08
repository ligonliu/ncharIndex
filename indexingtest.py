#!/usr/bin/env python3
import os
import subprocess

if __name__=='__main__':

    for text_len in [500, 1000, 5000, 10000, 20000, 30000]:
        for n_rows in [5000, 10000, 20000, 50000, 100000]:
            for N in [2]:
                subprocess.call('cmake-build-release/indexingtestM {0} {1} {2}'.format(text_len,n_rows,N),shell=True)

