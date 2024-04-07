from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from typing import List, Tuple
import os

def process_bigquery_results(bigquery_client: bigquery.Client, query: str) -> List[Tuple[any, any]]:
    """
    Ejecuta consultas BigQuery y formatea la respuesta en una Lista de tuplas.

    """
    try:
        query_job = bigquery_client.query(query) #ejecuta consulta
        results = query_job.result() #Trae resultado

        if not results:
            print("No results found for the query.")
            return []  # Retorna una lista vacía

        extracted_data = [(row[0], row[1]) for row in results] #Formatea lista de tuplas
        return extracted_data

    except BadRequest as e:
        print(f"BigQuery error: {e}")
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise
      