#from datetime import timedelta
#from airflow import DAG
#from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import dag, task
from airflow.models import Variable
#from airflow.providers.apache.hive.operators.hive import HiveOperator
#from airflow.utils.dates import days_ago
#import snscrape.modules.twitter as sntwitter
import pendulum
# import sys
# sys.path.append("..")


'''default_args={
    'owner': 'airflow', 
    "depends_on_past": False,
    "email": ["kamlesh.sahoo20@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'queue': 'bash_queue',
    'pool': 'backfill',
    'priority_weight': 10,
    'end_date': datetime(2016, 1, 1),
    'wait_for_downstream': False,
    'sla': timedelta(hours=2),
    'execution_timeout': timedelta(seconds=300),
    'on_failure_callback': some_function,
    'on_success_callback': some_other_function,
    'on_retry_callback': another_function,
    'sla_miss_callback': yet_another_function,
    'trigger_rule': 'all_success'
}'''

#connecting to database (or schema) 'test'
mysql_hook = MySqlHook(mysql_conn_id = 'mysql_conn', schema='Airflow') 
engine = mysql_hook.get_sqlalchemy_engine()  

@dag(
    schedule= None, #timedelta(minutes=5),
    start_date=pendulum.datetime(2022, 12, 19, tz='Europe/Amsterdam'),
    catchup=False,
    tags=["twitter","sentiment-classify"]
    )
def tweet_etl():
    @task(multiple_outputs=True)
    def extract():
        from my_packages.helper import tweet_scrapper  #import requests, import pandas as pd

        current_UTC = pendulum.now(tz='UTC')
        start_date = current_UTC.subtract(days=1).format('YYYY-MM-DDTHH:mm:ss') + '.000Z' 
        # BEARER TOKEN FOR TWITTER-API
        token = Variable.get("twitter_bearer_token")

        #instantiate scrapper object
        scrapper = tweet_scrapper(start_date, token)
        final_df = scrapper.scrap(all_results=True)
        
        #check if table exists, if not create.
        sql_query="""CREATE TABLE IF NOT EXISTS raw_tweets (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                tweet_id BIGINT,
                created_at DATETIME,
                tweet TEXT,
                lang VARCHAR(15),
                retweet_cnt INT,
                like_cnt INT,
                username VARCHAR(50),
                verified BOOLEAN
                );""" 
        mysql_hook.run(sql=sql_query)
    
        final_df.to_sql(name='raw_tweets', con=engine, if_exists='append', index=False)
        return {'current_date':current_UTC.to_date_string()} #format('YYYY-MM-DD HH:mm:ss)

    @task()
    def transform_load(date_dict):
        from my_packages.helper import sentiment_classifier
        import pandas as pd
        
        classifier = sentiment_classifier()
        #get only raw_tweets that are maximum 1 day older
        df = pd.read_sql("SELECT * FROM raw_tweets WHERE created_at > DATE_SUB('{}', INTERVAL 1 DAY);".format(date_dict['current_date']), con=engine)
        
        df['polarity'] = [classifier.sentiment(tweet) for tweet in df['tweet']]
        cols_to_write = ['tweet_id', 'created_at', 'tweet', 'polarity', 'retweet_cnt', 'like_cnt', 'username', 'verified']

        #create table (if not exists) to store the polarized tweets
        sql_query="""CREATE TABLE IF NOT EXISTS polar_tweets (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                tweet_id BIGINT,
                created_at DATETIME,
                tweet TEXT,
                polarity TEXT,
                retweet_cnt INT,
                like_cnt INT,
                username VARCHAR(50),
                verified BOOLEAN
                );""" 
        mysql_hook.run(sql=sql_query)
        df[cols_to_write].to_sql(name='polar_tweets', con=engine, if_exists='append', index=False)
    scrap_tweets = extract()
    transform_load(scrap_tweets)
tweet_etl()

    

    
                






