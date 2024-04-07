import io  # Importar el módulo io para manejar archivos en memoria
from google.colab import auth  # Importar el módulo auth de Google Colab para autenticación
from googleapiclient.discovery import build  # Importar el módulo build de googleapiclient para construir el cliente
from googleapiclient.http import MediaIoBaseDownload  # Importar el módulo MediaIoBaseDownload de googleapiclient.http para descargar archivos
from google.cloud import storage  # Importar el módulo storage de google.cloud para interactuar con Google Cloud Storage
import zipfile  # Importar el módulo zipfile para trabajar con archivos ZIP
from typing import Any, Optional  # Importar el tipo Any, Optional para anotaciones de tipo
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest

def authenticate_google_drive() -> None:
    """Autentica en Google Drive usando las credenciales del usuario
    """
    try:
        auth.authenticate_user()
    except Exception as e:
        print(f"Error authenticating to Google Drive: {e}")
        raise

def download_file_from_drive(drive_service: Any, file_id: str) -> io.BytesIO:
    """Descarga un archivo desde Google Drive.

    Returns:
        io.BytesIO: El contenido del archivo descargado como un objeto BytesIO.
    """

    archivo_descargado = io.BytesIO()  # Inicializar un objeto BytesIO para almacenar el archivo descargado
    try:
        solicitud = drive_service.files().get_media(fileId=file_id)  # Crear una solicitud para obtener el contenido del archivo
        descargador = MediaIoBaseDownload(archivo_descargado, solicitud)  # Inicializar el descargador con la solicitud y el objeto BytesIO
        hecho = False  # Inicializar una bandera para indicar si la descarga ha finalizado
        while not hecho:  # Iterar hasta que la descarga esté completa
            estado, hecho = descargador.next_chunk()  # Obtener el estado de la descarga y verificar si está completa
            print(f'Descargando {int(estado.progress() * 100)}%')  # Imprimir el progreso de la descarga
        archivo_descargado.seek(0)
        return archivo_descargado  # Devolver el objeto BytesIO con el contenido del archivo
    except Exception as e:  # Capturar excepciones
        print(f"Error al descargar el archivo: {e}")  # Imprimir el mensaje de error
        raise  # Relanzar la excepción

def upload_file_to_cloud_storage(bucket: storage.Bucket, folder_name: str, downloaded_file: io.BytesIO, zip_file_name: str) -> storage.Blob:
    """Sube un archivo a Google Cloud Storage.

    Returns:
        google.cloud.storage.Blob: El objeto blob subido.
    """
    blob_carpeta: storage.Blob = bucket.blob(f"{folder_name}/")  # Obtener el blob correspondiente a la carpeta

    # Verificar y crear la carpeta si no existe
    if not blob_carpeta.exists():  # Si la carpeta no existe en el bucket
        blob_carpeta.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')  # Subir un archivo vacío para crear la carpeta

    # Subir el archivo a la carpeta especificada
    blob_archivo = bucket.blob(f'{folder_name}/{zip_file_name}')  # Obtener el blob correspondiente al archivo
    blob_archivo.upload_from_file(downloaded_file, content_type='application/zip')  # Subir el archivo desde el objeto BytesIO al blob

    print(f'Archivo subido a gs://{bucket.name}/{blob_archivo.name}')  # Imprimir el mensaje de éxito
    return blob_archivo  # Devolver el objeto blob subido

def decompress_zip_file(bucket: storage.Bucket, folder_name: str, zip_file_name: str) -> str:
    """Descomprime un archivo ZIP almacenado en Google Cloud Storage.

    Returns:
        str: El nombre del archivo descomprimido.
    """

    nombre_archivo_json = ''  # Inicializar el nombre del archivo JSON resultante
    nombre_blob = ''  # Inicializar el nombre del blob

    try:
        blob_zip = bucket.blob(f'{folder_name}/{zip_file_name}')  # Obtener el blob correspondiente al archivo ZIP
        with zipfile.ZipFile(io.BytesIO(blob_zip.download_as_string()), 'r') as z:  # Abrir el archivo ZIP como un objeto zipfile
          for file_info in z.infolist():
                with z.open(file_info) as file:
                    blob_name= f'{folder_name}/{file_info.filename}'
                    json_file_name= file_info.filename
                    json_blob = bucket.blob(blob_name)
                    json_blob.upload_from_file(file)
        print(f'File decompressed in gs://{bucket.name}/{blob_name}')
    except zipfile.BadZipFile:
        print(f'The file in gs://{bucket.name}/{folder_name}/{zip_file_name} is not a valid ZIP file.')
    except Exception as e:
        print(f'Error decompressing file: {e}')
    finally:
        return json_file_name

def authenticate_bigquery( project_id: str) -> bigquery.Client:
    """Autentica en BigQuery."""
    return bigquery.Client(project_id)


def create_dataset(client: bigquery.Client, dataset_name: str, mode: Optional[str] = 'create') -> None:
    """
    Crea un dataset en Bigquery si no existe
    """

    dataset_ref = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_ref)
        if mode == 'overwrite':
            client.delete_dataset(dataset_ref, delete_contents=True)
            client.create_dataset(dataset_ref)
            print(f"Dataset '{dataset_name}' overwritten.")
        else:
            print(f"Dataset '{dataset_name}' already exists.")
    except NotFound:
        client.create_dataset(dataset_ref)
        print(f"Dataset '{dataset_name}' created.")


def create_table(client: bigquery.Client, dataset_name: str, table_name: str, mode: Optional[str] = 'create') -> None:
    """
    Crea una tabla BigQuery si no existe
    """

    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    try:
        client.get_table(table_ref)
        print(f"Table '{table_name}' already exists.")
        if mode == 'overwrite':
            client.delete_table(table_ref)
            table = bigquery.Table(table_ref)
            table.schema = []  # Esquema vacío para que BigQuery lo infiera
            client.create_table(table)
            print(f"Table '{table_name}' overwritten.")
    except NotFound:
        table = bigquery.Table(table_ref)
        table.schema = []  # Esquema vacío para que BigQuery lo infiera
        client.create_table(table)
        print(f"Table '{table_name}' created.")


def load_data_from_storage(client: bigquery.Client, source_uri: str, dataset_name: str, table_name: str, json_file_name: str) -> None:
    """
    Carga la data desde GCS a una tabla BigQuery
    """

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True  # Detecta automaticamente el esquema
    job_config.ignore_unknown_values = True  # Ignora valores nulos

    load_job = client.load_table_from_uri(
        source_uri + json_file_name,
        client.dataset(dataset_name).table(table_name),
        job_config=job_config
    ) #Carga la tabla
    load_job.result()  # Espera hasta la finalización
