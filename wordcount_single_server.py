import logging
import worker
from collections import Counter
import cPickle as pickle

with open('test_word/test_short', 'r') as f:
    c = Counter([i.strip().lower() for i in f.read().split()])

con = ''
for i in c.items():
    con += str(i[0]) +': ' + str(i[1]) + '\n'
with open('short_output_single', 'w+') as f:
    f.write(con)