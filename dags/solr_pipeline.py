from airflow import DAG
from airflow.operators.python import PythonOperator
import pysolr
import pandas as pd
import os
from datetime import datetime, timedelta
import logging

# Configuração DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Caminhos e URLs (Resolvidos internamente pela rede do Docker)
CSV_PATH = '/opt/airflow/dags/data/alunos.csv'
PROCESSED_CSV_PATH = '/opt/airflow/dags/data/alunos_processed.csv'
SOLR_URL = 'http://solr_instance:8983/solr/alunos'

def format_data(**kwargs):
    logging.info("Iniciando leitura e formatacao dos dados CSV...")
    try:
        df = pd.read_csv(CSV_PATH)
        
        # --- Tratamento de dados ---
                # --- Tratamento de dados ---
        # Tratamento Numérico
        df['Idade'] = pd.to_numeric(df['Idade'], errors='coerce').fillna(0).astype(int)
        df['Série'] = pd.to_numeric(df['Série'], errors='coerce').fillna(0).astype(int)
        df['Nota Média'] = pd.to_numeric(df['Nota Média'], errors='coerce').fillna(0.0)
        
        # Padronização de Textos (Strings)
        df['Nome'] = df['Nome'].str.title().str.strip()
        df['Endereço'] = df['Endereço'].fillna("Endereço não informado").str.strip()
        df['Nome do Pai'] = df['Nome do Pai'].fillna("Não informado").str.title().str.strip()
        df['Nome da Mãe'] = df['Nome da Mãe'].fillna("Não informado").str.title().str.strip()
        
        # Tratamento de Data
        df['Data de Nascimento'] = pd.to_datetime(df['Data de Nascimento'], errors='coerce')
        df['Data de Nascimento'] = df['Data de Nascimento'].dt.strftime('%Y-%m-%dT00:00:00Z').fillna("1900-01-01T00:00:00Z")

        # Remove linhas sem nome do aluno
        df.dropna(subset=['Nome'], inplace=True)
        
        # --- Salva o arquivo processado temporariamente ---
        os.makedirs(os.path.dirname(PROCESSED_CSV_PATH), exist_ok=True)
        df.to_csv(PROCESSED_CSV_PATH, index=False)
        
        logging.info(f"Formatacao concluida. {len(df)} registros processados com sucesso.")
    except FileNotFoundError:
        logging.error(f"Arquivo nao encontrado no caminho: {CSV_PATH}")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado durante a formatacao dos dados: {str(e)}")
        raise e

def insert_into_solr(**kwargs):
    logging.info("Iniciando insercao de dados no Apache Solr...")
    try:
        solr = pysolr.Solr(SOLR_URL, always_commit=True, timeout=10)
        
        df = pd.read_csv(PROCESSED_CSV_PATH)
        records = df.to_dict(orient='records')
        
        chunk_size = 500
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            solr.add(chunk)
            logging.info(f"Lote de {len(chunk)} registros inserido com sucesso (Progresso: {i + len(chunk)}/{len(records)}).")
            
        logging.info("Todos os dados foram inseridos com sucesso.")
    except pysolr.SolrError as se:
        logging.error(f"Erro de comunicacao com o Solr: {str(se)}")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado durante a insercao no Solr: {str(e)}")
        raise e

# --- Define DAG ---
with DAG(
    'solr_pipeline',
    default_args=default_args,
    description='Pipeline de ingestão de CSV para Solr',
    schedule=None,
    catchup=False,
    tags=['desafio', 'solr', 'csv']
) as dag:

    task_format_data = PythonOperator(
        task_id='format_csv_data',
        python_callable=format_data
    )

    task_insert_solr = PythonOperator(
        task_id='insert_solr_data',
        python_callable=insert_into_solr
    )

    # --- Ordem de execução ---
    task_format_data >> task_insert_solr