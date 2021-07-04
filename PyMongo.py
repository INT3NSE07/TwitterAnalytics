#!/usr/bin/env python

from twython import Twython
from pymongo import MongoClient
from bson.json_util import dumps
from nltk.tokenize import TweetTokenizer
from collections import Counter
import configparser
import sys
import json
import os
import re

config = configparser.ConfigParser()
config.read('config.ini')

API_KEY = config['twitter_access_token']['API_KEY']
API_SECRET = config['twitter_access_token']['API_SECRET']

api = Twython(API_KEY, API_SECRET)

if not api:
    print('Authentication failed')
    sys.exit(1)

tweets = []
requests = 100
num_tweets = int(input("Enter number of tweets to be Extracted.."))
max_id = None
since_id = None
tweets_count = 0

client = MongoClient()
db = client['twitterdb']
collection = db['twitter_dataset']

query = "geocode:20.5937,78.9629,800mi"

while tweets_count < num_tweets:
    try:
        if max_id:
            if not since_id:
                tweets_fetched = api.search(q=query, count=requests)
            else:
                tweets_fetched = api.search(q=query, count=requests, since_id=since_id)
        else:
            if not since_id:
                tweets_fetched = api.search(q=query, count=requests, max_id=max_id)
            else:
                tweets_fetched = api.search(q=query, count=requests, max_id=max_id, since_id=since_id)

        if not tweets_fetched:
            print("No more tweets found")
            break

        for tweet in tweets_fetched["statuses"]:
            tweets.append({"created_at": tweet["created_at"], "id": tweet["id"], "x": tweet["id_str"],
                           "text": tweet["text"]})

        tweets_count += len(tweets_fetched['statuses'])

        print('Number of downloaded Tweets: {0} '.format(tweets_count))

        max_id = tweets_fetched['search_metadata']["max_id_str"]

    except Exception as e:
        print("Exception encountered : " + str(e))
        sys.exit(1)

with open('twitter_dataset.json', 'w') as file:
    json.dump(tweets, file, indent=4, ensure_ascii=False)

with open('twitter_dataset.json', 'r', encoding='utf-8') as file:
    data = json.loads(file.read())
    collection.insert(data)

try:
    os.remove('twitter_dataset.json')
except OSError:
    pass

tokenizer = TweetTokenizer()

tokens = tokenizer.tokenize(dumps(db.twitter_dataset.find({}, {"text": ""})))

re_hashtags = re.compile(r'(?:\#+[\w_]+[\w\'_\-]*[\w_]+)', re.VERBOSE | re.IGNORECASE)
re_mentions = re.compile(r'(?:@[\w_]+)', re.VERBOSE | re.IGNORECASE)

hashtags = re_hashtags.findall("".join(tokens))
mentions = re_mentions.findall("".join(tokens))

print("Hashtags")
hashtags_count = Counter(hashtags)
for token, count in hashtags_count.most_common(5):
    print("{0}: {1}".format(token, count))

print("Mentions")
mentions_count = Counter(mentions)
for token, count in mentions_count.most_common(5):
    print("{0}: {1}".format(token, count))
