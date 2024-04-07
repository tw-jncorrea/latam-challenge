import datetime 
from typing import List, Tuple  
from google.cloud import bigquery  
from process import process_bigquery_results  

def q1_time(client: bigquery.Client) -> List[Tuple[datetime.date, str]]:
    query = """
      with
      top_dates as (
        select CAST(date AS DATE) AS tweets_date,
        count(*) AS conteo_tweets
        from tweets_dataset.tweets
        group by tweets_date
        order by conteo_tweets desc
        limit 10
      ),
      top_users_date AS (
        select TDT.tweets_date, TWS.user.username, max(TDT.conteo_tweets) AS max_conteo_tweets,count(*) AS conteo_tweets_usuario
        , ROW_NUMBER() OVER(PARTITION BY TDT.tweets_date ORDER BY max(TDT.conteo_tweets) desc, count(*) desc) AS row_tweets
        from tweets_dataset.tweets AS TWS
        inner join top_dates AS TDT
        on TDT.tweets_date = CAST(TWS.date AS DATE)
        group by TDT.tweets_date, TWS.user.username
        order by max_conteo_tweets desc, conteo_tweets_usuario desc
      )

      SELECT tweets_date, username
      FROM top_users_date
      WHERE row_tweets = 1
      """
    return process_bigquery_results(client, query)