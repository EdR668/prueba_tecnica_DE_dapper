import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import psycopg2
import boto3
from botocore.exceptions import ClientError
import json
import os
from typing import Dict, Any

# Configuración de AWS Secrets Manager
SECRET_NAME = os.environ.get("SECRET_NAME", "Test")
REGION_NAME = os.environ.get("AWS_REGION", "us-east-1")

# Constantes para el scraping
ENTITY_VALUE = 'Agencia Nacional de Infraestructura'
FIXED_CLASSIFICATION_ID = 13



# Cliente de Secrets Manager
secrets_client = boto3.client('secretsmanager', region_name=REGION_NAME)

def get_secret():
    """
    Recupera las credenciales de la base de datos de AWS Secrets Manager.
    """
    try:
        get_secret_value_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        raise e

#  Clase para manejar la conexión a la base de datos y realizar operaciones de inserción de datos.
       

# Obtener el rtype_id basado en el título del documento







def lambda_handler(event, context):
    """
    AWS Lambda handler function para el scraping de normativas ANI.
    Modificado para procesar las páginas más recientes (0-8) y detectar contenido nuevo.
    """
    try:
        # Obtener parámetros del evento
        num_pages_to_scrape = event.get('num_pages_to_scrape', 9) if event else 9
        force_scrape = event.get('force_scrape', False) if event else False
        
        print(f"Iniciando scraping de ANI - Páginas a procesar: {num_pages_to_scrape}")
        
        # Verificar si hay contenido nuevo (a menos que se fuerce el scraping)
        if not force_scrape:
            has_new_content = check_for_new_content(min(3, num_pages_to_scrape))
            if not has_new_content:
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'No se detectó contenido nuevo. Scraping omitido.',
                        'records_scraped': 0,
                        'records_inserted': 0,
                        'content_check': 'no_new_content',
                        'success': True
                    })
                }
        
        # Procesar las páginas más recientes (0 a num_pages_to_scrape-1)
        start_page = 0
        end_page = num_pages_to_scrape - 1
        
        print(f"Procesando páginas más recientes desde {start_page} hasta {end_page}")
        
        # Proceso principal de scraping
        all_normas_data = []
        
        for page_num in range(start_page, end_page + 1):
            print(f"Procesando página {page_num}...")
            page_data = scrape_page(page_num)
            all_normas_data.extend(page_data)
            
            # Indicador de progreso cada 3 páginas
            if (page_num + 1) % 3 == 0:
                print(f"Procesadas {page_num + 1}/{num_pages_to_scrape} páginas. Encontrados {len(all_normas_data)} registros válidos.")
        
        if not all_normas_data:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No se encontraron datos válidos durante el scraping',
                    'records_scraped': 0,
                    'records_inserted': 0,
                    'pages_processed': f"{start_page}-{end_page}",
                    'success': True
                })
            }
        
        # Crear DataFrame
        df_normas = pd.DataFrame(all_normas_data)
        print(f"Total de registros extraídos: {len(df_normas)}")
        
        # Operaciones de base de datos
        db_manager = DatabaseManager()
        if not db_manager.connect():
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error de conexión a la base de datos',
                    'success': False
                })
            }
        
        try:
            # Insertar nuevos registros
            inserted_count, status_message = insert_new_records(db_manager, df_normas, ENTITY_VALUE)
            
            response = {
                'statusCode': 200,
                'body': json.dumps({
                    'message': status_message,
                    'records_scraped': len(df_normas),
                    'records_inserted': inserted_count,
                    'pages_processed': f"{start_page}-{end_page}",
                    'content_check': 'new_content_found' if not force_scrape else 'forced_scrape',
                    'success': True
                })
            }
            
            print(f"Operación completada: {status_message}")
            return response
            
        finally:
            db_manager.close()
        
    except Exception as e:
        error_message = f"Error en la ejecución de Lambda: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': error_message,
                'success': False
            })
        }

# Para pruebas locales
if __name__ == "__main__":
    # Evento de prueba
    test_event = {
        'num_pages_to_scrape': 3,
        'force_scrape': True
    }
    
    # Contexto de prueba (vacío para pruebas locales)
    test_context = {}
    
    # Ejecutar función
    result = lambda_handler(test_event, test_context)
    print(json.dumps(result, indent=2))