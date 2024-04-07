import datetime 
from typing import List, Tuple  
from google.cloud import bigquery  
from process import process_bigquery_results  

def q3_time(client: bigquery.Client) -> List[Tuple[datetime.date, str]]:
    query = """
      SELECT user.username, count(*) AS conteo_menciones
      FROM tweets_dataset.tweets,
      UNNEST(mentionedUsers) AS user
      group by user.username
      order by conteo_menciones desc
      LIMIT 10
      """
    return process_bigquery_results(client, query)