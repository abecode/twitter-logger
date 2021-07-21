#!/usr/bin/env python3

""" takes a twitter account, gets the friends and then listens to them """

import gzip
import json
import os
import signal
import sys
import urllib3
import tweepy

twitter_user = os.environ['TWITTER_USER'] # e.g. abestockmon
consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
access_token = os.environ['ACCESS_TOKEN']
access_token_secret = os.environ['ACCESS_TOKEN_SECRET']
outfile = "/home/ec2-user/twitter-logger/twitter.log.gz"

f = gzip.open(outfile, 'a')

def receiveHup(signalNumber, frame):
    """processes the HUP signal by opening a new file handle and closing
    the old one, relying on global interpreter log for syncronization
    """
    global f
    f_old = f
    f = gzip.open(outfile, 'a')
    f_old.close()


class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        """ write every tweet to gzipped json"""
        # note: this is adding an extra "\n", ie not a control
        # character but two regular characters
        f.write(json.dumps(status._json).encode('utf-8'))
        f.write("\n".encode('utf-8'))

def filter(stream, followlist):
    """ takes a tweepy stream and list of user ids, as strings """
    try:
        stream.filter(followlist, is_async=True)
    except (KeyboardInterrupt, SystemExit):
        raise
    #except urllib3.exceptions.ReadTimeoutError:
    except Exception as e:
        print("restarting stream", e, file=sys.stderr)
        filter(stream, followlist)

def main():
    """ main function """
    signal.signal(signal.SIGHUP, receiveHup)
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, retry_count=10, retry_delay=5, timeout=300)
    res = api.search_users(["abestockmon"])
    user = res[0]
    followlist = []
    for uid in tweepy.Cursor(api.friends_ids, id=user.id).items():
        followlist.append(uid)
    listener = MyStreamListener()
    stream = tweepy.Stream(auth = api.auth, listener=listener)
    followliststr = map(str, followlist)
    filter(stream, followliststr)

if __name__ == "__main__":
    main()
