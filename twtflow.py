from datetime import date, timedelta, datetime
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def imptweet():
        import tweepy
        import psycopg2
        import pandas as pd

        ####input your credentials here
        keys_file = open("twtkeys.txt")
        lines = keys_file.readlines()
        ##access_token = lines[0].rstrip()
        ##access_token_secret = lines[1].rstrip()
        ##consumer_key = lines[2].rstrip()
        ##consumer_secret = lines[3].rstrip()
        auth = tweepy.OAuthHandler(lines[2].rstrip(), lines[3].rstrip())
        auth.set_access_token(lines[0].rstrip(),lines[1].rstrip())
        api = tweepy.API(auth,wait_on_rate_limit=True)

        # Postgresql initialization
        pwd_file = open("postgrespwd.txt")
        creds = pwd_file.readlines()
        connection = psycopg2.connect(user = creds[0].rstrip(), password = creds[1].rstrip(), host = creds[2].rstrip(), port = creds[3].rstrip(),database = creds[4].rstrip())
        pcursor = connection.cursor()

        # Print PostgreSQL Connection properties
        print ( connection.get_dsn_parameters(),"\n")
        # Print PostgreSQL version
        pcursor.execute("SELECT version();")
        record = pcursor.fetchone()
        print("You are connected to - ", record,"\n")

        try:
                for tweet in tweepy.Cursor(api.search,q="#engvsnz",count=50, lang="en", since="2019-07-01").items(50):
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

def printok():
        from datetime import datetime

        now = datetime.now()
        print("The current tweet list was pulled at ",now)


with DAG(
        'twtflow',
        default_args = {
        'owner': 'airflow',
        'start_date': datetime(2019,7,14),
        'depends_on_past': False,
        'retries' : 1,
        'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(minutes=1),
) as dag:
        task_1 = PythonOperator(
                task_id='task_1',
                python_callable=imptweet

        )
        task_2 = PythonOperator(
                task_id='task_2',
                python_callable=printok
        )
        task_1 >> task_2
