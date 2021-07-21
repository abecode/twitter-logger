#!/usr/bin/env python3

import json
import gzip
import fastparquet as fp
import pandas as pd

import re

import sys
if len(sys.argv) != 2 and len(sys.argv) != 3:
    raise RuntimeError("need one or two arguments, a gzipped ndjson file and an optional destination")
if not re.match(r'.*log\.gz$', sys.argv[1]):
    raise RuntimeError("need one or two arguments, a gzipped ndjson file and an optional destination")

jsonfname = sys.argv[1]

f = gzip.open(jsonfname)
json_data = []
for line in f.readlines():
    if line:
        json_data.append(json.loads(line))

tab = pd.json_normalize(json_data)

if len(sys.argv) == 3:
    destination = sys.argv[2]
else: # if destination not specified, change extension to parquet
    destination = jsonfname
    #destination = re.sub(r'/twitter/', '/twitter_parquet/', destination)
    destination = re.sub(r'log.gz$', 'parq', destination)


from fastparquet import write
write(destination, tab, compression='GZIP')


