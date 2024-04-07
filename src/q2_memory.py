import datetime 
from typing import List, Tuple  
from google.cloud import bigquery  
from process import process_bigquery_results  
import memory_profiler  


@memory_profiler.profile
def q2_memory(client: bigquery.Client) -> List[Tuple[datetime.date, str]]:
    query = r"""
      SELECT
          emoji AS emoji,
          CAST(COUNT(*) AS INT) AS count
      FROM (
          SELECT
              REGEXP_EXTRACT_ALL(content, r"(?:[\x{1F300}-\x{1F5FF}]|[\x{1F900}-\x{1F9FF}]|[\x{1F600}-\x{1F64F}]|[\x{1F680}-\x{1F6FF}]|[\x{2600}-\x{26FF}]\x{FE0F}?|[\x{2700}-\x{27BF}]\x{FE0F}?|\x{24C2}\x{FE0F}?|[\x{1F1E6}-\x{1F1FF}]{1,2}|[\x{1F170}\x{1F171}\x{1F17E}\x{1F17F}\x{1F18E}\x{1F191}-\x{1F19A}]\x{FE0F}?|[\\x{0023}\x{002A}\x{0030}-\x{0039}]\x{FE0F}?\x{20E3}|[\x{2194}-\x{2199}\x{21A9}-\x{21AA}]\x{FE0F}?|[\x{2B05}-\x{2B07}\x{2B1B}\x{2B1C}\x{2B50}\x{2B55}]\x{FE0F}?|[\x{2934}\x{2935}]\x{FE0F}?|[\x{3297}\x{3299}]\x{FE0F}?|[\x{1F201}\x{1F202}\x{1F21A}\x{1F22F}\x{1F232}\x{1F23A}\x{1F250}\x{1F251}]\x{FE0F}?|[\x{203C}-\x{2049}]\x{FE0F}?|[\x{00A9}-\x{00AE}]\x{FE0F}?|[\x{2122}\x{2139}]\x{FE0F}?|\x{1F004}\x{FE0F}?|\x{1F0CF}\x{FE0F}?|[\x{231A}\x{231B}\x{2328}\x{23CF}\x{23E9}\x{23F3}\x{23F8}\x{23FA}]\x{FE0F}?)") AS emojis
          FROM
              tweets_dataset.tweets
      )
      CROSS JOIN
          UNNEST(emojis) AS emoji
      GROUP BY
          emoji
      ORDER BY
          count DESC
      LIMIT
          10
      """
    return process_bigquery_results(client, query)