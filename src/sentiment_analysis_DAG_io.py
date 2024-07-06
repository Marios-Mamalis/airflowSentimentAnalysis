# -*- coding: utf-8 -*-
import airflow
from airflow.providers.google.cloud.operators.bigquery import *
from airflow.operators.python import PythonOperator
import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import re
from newsdataapi import NewsDataApiClient
import datetime

from dotenv import find_dotenv, load_dotenv
import os
import yaml
import logging
import sys

logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format='%(levelname)s: %(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": YESTERDAY,
}

with airflow.DAG(
    "sentiment_analysis_DAG",
    catchup=False,
    default_args=default_args,
    schedule_interval='0 0 * * *',
    template_searchpath="home/airflow/gcs/dags/scripts"
) as dag:

    def get_data_func(**kwargs):

        load_dotenv(find_dotenv())
        NEWS_API_KEY = os.environ['news_api_key']

        with open("config.YAML") as yf:
            config = yaml.safe_load(yf)
        TOPIC = config['topic']
        articles_to_fetch = config['articles_to_fetch']  # will be rounded down to the nearest ten

        articles_to_fetch = min(articles_to_fetch, 2000)  # limit for maximum number of requests per day

        TODAY_str = datetime.datetime.now().strftime('%Y-%m-%d')
        api = NewsDataApiClient(apikey=NEWS_API_KEY)

        all_articles = api.news_api(q=TOPIC, language='en')

        if articles_to_fetch > 10 and all_articles['totalResults'] > 10:  # api supports batches of 10
            articles_to_fetch = min(all_articles['totalResults'], articles_to_fetch)

            no_loops = articles_to_fetch // 10 - 1
            next_page_id = all_articles['nextPage']
            for i in range(no_loops):
                new_articles = api.news_api(q=TOPIC, language='en', page=next_page_id)
                next_page_id = new_articles['nextPage']
                all_articles['results'].extend(new_articles['results'])

        logger.info(f"Articles fetched: {len(all_articles['results'])}")

        all_articles = [{k: v for k, v in i.items() if k in ['title', 'description', 'country', 'category', 'creator']}
                        for i in all_articles['results']]
        all_articles = [{k: v[0] if isinstance(v, list) and v is not None else v for k, v in i.items()} for i in
                        all_articles]

        df = pd.DataFrame(all_articles)
        df = df[['title', 'creator', 'description', 'country', 'category']]
        df.dropna(inplace=True)

        try:
            SentimentIntensityAnalyzer()
        except LookupError:
            nltk.download('vader_lexicon')

        sia = SentimentIntensityAnalyzer()

        # clean
        df['description'] = df['description'].apply(lambda x: str(x).lower())  # lower
        df['description'] = df['description'].apply(lambda x: re.sub(r'[^a-z]', ' ', str(x)))  # remove symbols
        df['description'] = df['description'].apply(
            lambda x: re.sub(r'\s+', ' ', str(x)).lstrip().rstrip())  # remove extra spaces

        # sentiment
        df['description_sentiment'] = df['description'].apply(lambda x: sia.polarity_scores(str(x))['compound'])

        # other data
        df['topic'] = TOPIC
        df['retrievaldate'] = TODAY_str

        sql_texts = []
        table = 'sentimentschema.sentimentnewstable'
        sql_query = f'INSERT INTO {table} ({", ".join(df.columns)}) VALUES '
        for index, row in df.iterrows():
            sql_texts.append(str(tuple(row.values)))
        sql_query += ', '.join(sql_texts)

        return sql_query

    get_data = PythonOperator(task_id="get_data", python_callable=get_data_func)

    insert_data = BigQueryInsertJobOperator(
        task_id="insert_query",
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='get_data') }}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        }
    )

    idem_create = BigQueryInsertJobOperator(
        task_id="idem_create",
        configuration={
            "query": {
                "query": "CREATE TABLE IF NOT EXISTS `sentimentschema`.`sentimentnewstable` (`title` STRING, `creator` STRING, `description` STRING, `country` STRING, `category` STRING, `description_sentiment` FLOAT64, `topic` STRING, `retrievaldate` DATE);",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        }
    )

    idem_create >> get_data >> insert_data
