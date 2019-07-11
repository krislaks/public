import tweepy
import psycopg2
import pandas as pd

####input your credentials here
access_token = '66927265-aA9FbbEMJqAQicRZTvndNtoz1RCEe6vXwiJ59GnrB'
access_token_secret = 'omwJwYWvFCXZHX2IC3VxGmva2SsDikewM37dn1oLXiTHA'
consumer_key = 'c9KTjWqP1PXORwBTjJXDsj3DU'
consumer_secret = 'pWd5VJmsTecLBLGjK8b6gflKivpvhuWmJBDh3Bz4NsGTdwb05P'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token,access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)

# Postgresql initialization
connection = psycopg2.connect(user = "postgres", password = "fanlytiks", host = "127.0.0.1", port = "5432",database = "postgres")
pcursor = connection.cursor()

# Print PostgreSQL Connection properties
print ( connection.get_dsn_parameters(),"\n")
# Print PostgreSQL version
pcursor.execute("SELECT version();")
record = pcursor.fetchone()
print("You are connected to - ", record,"\n")

try:
    for tweet in tweepy.Cursor(api.search,q="#indvsnz",count=100, lang="en", since="2019-06-01").items(50):
        print(tweet.id,tweet.text,tweet.author.screen_name)
        pcursor.execute("INSERT INTO tweets (tweet_id, text, screen_name, author_id, created_at, inserted_at) VALUES (%s, %s, %s, %s, %s, current_timestamp);", (tweet.id, tweet.text, tweet.author.screen_name, tweet.author.id, tweet.created_at))
        connection.commit()
except tweepy.error.TweepError:
    print ("Whoops, could not fetch tweets!")
except UnicodeEncodeError:
    pass
finally:
    pcursor.close()
    connection.close()
