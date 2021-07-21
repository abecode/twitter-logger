#!/usr/bin/env python3

"""Hi Bron, I may have been overthinking this.  You may have better
luck trying to use aws cmd instead of boto3 and just using the
json2parquet.py file
- Abe
"""

import json
import gzip
import io
import os
import re
#import fastparquet as fp
from fastparquet import write
import pandas as pd
import boto3
import sys

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ["S3_PREFIX"]



def get_all_s3_objects(s3, **base_kwargs):
    """ c.f. https://stackoverflow.com/questions/54314563/how-to-get-more-than-1000-objects-from-s3-by-using-list-objects-v2 """
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')

session = boto3.Session()
#session = boto3.Session(profile_name="ust")
client = session.client('s3')
s3 = boto3.resource('s3')
for obj in get_all_s3_objects(client, Bucket=S3_BUCKET, Prefix=S3_PREFIX):
    # get object
    bytes_io = io.BytesIO()
    s3objectname = obj['Key']
    print(s3objectname)
    # next line ignores some of the earlier files that might have
    # defects (e.g. corrupted gz or bigger than 100M)
    if s3objectname < "twitter/tweets-2019-08-20T08:30:02Z.log.gz":
        continue
    s3.Object(S3_BUCKET, s3objectname).download_fileobj(bytes_io)
    bytes_io.seek(0) # I think there was a problem that the buffer
                     # wasn't rewound

    #next line would do it in memory
    decompressedFile = gzip.GzipFile(fileobj=bytes_io)
    # next lines would write to disk
    #with open("out.tmp", "wb") as of:
    #    of.write(bytes_io.getbuffer())
    # process object
    json_data = []
    try:
        for line in decompressedFile:
            if line:
                json_data.append(json.loads(line))
    except json.decoder.JSONDecodeError:
        with open("json2parq_processing.log", 'a') as log:
            print(s3objectname, "json.decoder.JSONDecodeError")
            print(s3objectname, "json.decoder.JSONDecodeError", file=log)
        continue
    tab = pd.json_normalize(json_data)
    write("out.tmp.parq", tab, compression='GZIP')
    # upload object
    destination = re.sub(r'/twitter/', 'twitter_parquet', s3objectname)
    destination = re.sub(r'log\.gz$', 'parq', destination)
    client.upload_file("out.tmp.parq", S3_BUCKET, destination)
    
    # save object name
    with open("json2parq_processing.log", 'a') as log:
        print(s3objectname, destination)
        print(s3objectname, destination, file=log)


