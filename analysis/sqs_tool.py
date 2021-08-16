#!/usr/bin/env python3

"""
Commands: 
load-sqs: loads sqs with all the json objects in the s3 bucket
process-sql: reads json object from sqs, converts it to parqet (in loop)

Environment variables:
#SQS_URL: url for the sqs queue
SQS_QUEUE: name for the sqs queue
S3_BUCKET_IN: S3 bucket containing the input json files
S3_PREFIX_IN: prefix of the S3 bucket containing the json files
S3_BUCKET_OUT: bucket to save the output parq files
S3_PREFIX_OUT: prefix to save the output parquet files

also
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
"""

import json
import gzip
import os
import re
import sys
import boto3
from fastparquet import write
import pandas as pd

SQS_QUEUE = os.environ.get("SQS_QUEUE", "json2parq")
S3_BUCKET_IN = os.environ.get("S3_BUCKET_IN", "abestockmon")
S3_PREFIX_IN = os.environ.get("S3_PREFIX_IN", "twitter")
S3_BUCKET_OUT = os.environ.get("S3_BUCKET_OUT", "abestockmon")
S3_PREFIX_OUT = os.environ.get("S3_PREFIX_OUT", "twitter_parquet")




def s3_keys(bucket_name, prefix='/', delimiter='/'):
    """ cf. https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3 """
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))

def load_sqs():
    # list objects in S3_BUCKET_IN
    keys = [x for x in s3_keys(S3_BUCKET_IN, prefix=S3_PREFIX_IN)]
    q = sqs.get_queue_by_name(QueueName="json2parq")
    # send each object to SQS
    session = boto3.session()
    sqs = session.resource('sqs')
    q = sqs.get_queue_by_name(QueueName="json2parq")
    for k in keys:
        q.send_message(MessageBody=k)
    pass

def get_sqs_iter(q):
    # read message from SQS
    while True:
        messages = q.receive_messages(MaxNumberOfMessages=1)
        if len(messages) == 1:
            yield messages[0]
        else:
            print("seems to be done")
            
def process_sqs_message(m):
    # setup variables
    session = oboto3.session.Session()
    s3 = session.resource('s3')
    b_in = s3.Bucket(S3_BUCKET_IN)
    b_out = s3.Bucket(S3_BUCKET_OUT)
    jsonfile = re.sub(r'.*/(.*gz)', r'\1', m.body)
    parqfile = jsonfile
    parqfile = re.sub(r'log.gz$', 'parq', parqfile)
    output_key = "twitter_parquet/" + parqfile
    # check if file is already done:
    if len(list(b_out.objects.filter(Prefix=output_key))) > 0:
        print(f"looks like {m.body} has already been converted to {output_key}")
        return m
    # skip early files that were too big
    if m.body < "twitter/tweets-2019-08-20T08:30:02Z.log.gz":
        print(f"skipping {m.body} for now: too big")
        return m
    # get json object from S3
    b_in.download_file(m.body, jsonfile)
    # convert to parquet
    try:
        json2parquet(jsonfile, parqfile)
    except:
        print(f"error converting {jsonfile} to  {parqfile}")
        return m
    # upload parquet object to S3_BUCKET_OUT
    b_out.upload_file(parqfile, output_key)
    # delete files
    os.remove(jsonfile)
    os.remove(parqfile)
    # return message for deletion
    return m

def json2parquet(ifname, ofname):
    """ converts fname to parquet format
    
    fname should end in log.gz, output will change log.gz to .parq
    """
    with gzip.open(ifname) as f:
        json_data = []
        for line in f.readlines():
            if line:
                json_data.append(json.loads(line))
        tab = pd.json_normalize(json_data)
        write(ofname, tab, compression='GZIP')

def process_sqs():
    session = boto3.session.Session()
    sqs = session.resource('sqs')
    q = sqs.get_queue_by_name(QueueName="json2parq")
    sqs_iter = get_sqs_iter(q)
    for m in sqs_iter:
        print(m.body)
        m_out = process_sqs_message(m)
        q.delete_messages(Entries=[{"Id":"1", "ReceiptHandle":m_out.receipt_handle}])        
if __name__ ==  "__main__":
    if sys.argv[1] == "process_sqs":
        process_sqs()
    if sys.argv[1] == "load_sqs":
        load_sqs()
    else:
        print('argument must be either process_sqs or load_sqs')
        sys.exit(-1)
            
