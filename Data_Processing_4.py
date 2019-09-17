#!/usr/bin/env Python
#-*- coding:utf-8 -*-
from time import sleep
import pandas as pd
import glob
from api import *
import csv
import tweepy
import requests
import requests_cache

#为requests立缓存，避免每次执行都去请求一次网页，造成时间浪费
requests_cache.install_cache('demo_cache')

# Twitter API setup
CONSUMER_KEY = 'KnRCpf7tQ1GC82VKcsUCEUb4i'
CONSUMER_SECRET = 'PoNSwQJpJLtftneEq3qZJGQhhDoht4wcZJN7pYhxuTyw3lG7WG'
ACCESS_TOKEN = '1032892315852001280-ZeNMBlJTo0YUMBdIZPI7Qp1iu4kfj8'
ACCESS_TOKEN_SECRET = 'Aos1eLD2lBzN6AQXHFKFQ5JFYwetvZoTlxJVflko94Xl6'
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# create a list of user ids
def create_id_list(path,filename):
    id_con = pd.read_csv(open(path + filename, 'rU',encoding='utf-8'), encoding='utf-8', usecols = ['id'] )
    
    id_list = list(id_con['id'])
    id_list = [str(i) for i in id_list]
    items = ["False", "None", "nan"]
    id_list = filter(lambda x: x not in items, id_list)
    final_id_list = [int(i) for i in id_list]
    user_ids=final_id_list
    old_ids=[]
    error_ids=[]
    return user_ids,old_ids,error_ids

def get_all_tweets(out_path):
    num = 0
    '''
	all_tweets-->tweets
	tweets-->new_tweets
	oldest-->old
	user_id-->twitter_id
    '''
    while len(user_ids) > 1:
        try:
            twitter_id = user_ids[num]
            print('crawling user %s data...' % twitter_id)
            tweets = []
            new_tweets = api.user_timeline(user_id=twitter_id, count=200)
            tweets.extend(new_tweets)
            old = tweets[-1].id - 1
            while len(new_tweets) > 0:
                new_tweets = api.user_timeline(user_id=twitter_id, count=200, max_id=old)
                tweets.extend(new_tweets)
                old = tweets[-1].id - 1
                print('%s tweets downloaded' % (len(tweets)))

            outtweets = [[tweet.id_str, tweet.created_at, tweet.source, 
                          tweet.favorite_count, tweet.retweet_count, 
                          tweet.text.encode("utf-8")] for tweet in tweets]
            user_ids.remove(twitter_id)
            old_ids.append(twitter_id)
            new_tweets = [new_tweets + [twitter_id] for new_tweets in outtweets]
            with open(out_path + '%s_tweets.csv' % twitter_id, 'w', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["id","created_at", "source", "favorite_count", "retweet_count", "tweet_text", "id"])
                writer.writerows(new_tweets)
            print('Saved data.')
        except tweepy.TweepError as e:
            if e.reason=='Not authorized.':
                print('this user not authorized.')
                user_ids.remove(twitter_id)
                old_ids.append(twitter_id) 
                #continue
            else:
                print(e)
                user_ids.remove(twitter_id)
                old_ids.append(twitter_id)
                #continue           
            error_ids.append(twitter_id)
            continue

if __name__ == '__main__':
    global user_ids
    global old_ids
    global error_ids
    in_path='H:/Data_Processing/' #路径可修改，但要提前建好文件夹
    out_path='H:/Data_Processing/3-genuine_tweets/' ##路径可修改，但要提前建好文件夹
    in_filename='genuine_user_id.csv'
    user_ids,old_ids,error_ids=create_id_list(in_path,in_filename)
    while len(user_ids) > 0:
        get_all_tweets(out_path)


