## Sentiment Analysis DAG on GCP

Airflow DAG that performs sentiment analysis on API-fetched news articles.
The DAG is designed to be deployed on GCP Composer and run at a daily interval.
During the DAG Run the processes below are serially executed:

1) A table is idempotently created on Google BigQuery. Each row represents a news article, 
with the table's columns being `title`, `creator`, `description`, `country`, `category`,
`description_sentiment`, `topic` and `retrievaldate`.
2) News articles are fetched using the `newsdata.io` API client. 
The topic and volume of the articles fetched are configurable via the YAML file.
3) The articles are processed with standard NLP methods and their sentiment is calculated 
with the `Vader` `NLTK` sub-module. Sentiment scores range from 0 (negative sentiment)
to 1 (positive sentiment).
4) The results of step 3 are stored in the BigQuery table created in step 1.
