from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
from src.carga_postgres import CargaPostgres
from src.config import Configuracao
import pandas as pd

logging.basicConfig(
    level=Configuracao.NIVEL_LOG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

args_padrao = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def carregar_dados(**context):
    """Carrega os dados transformados para o PostgreSQL"""
    data_execucao = context['dag_run'].conf.get('data_execucao', context['ds'])
    logger.info(f"Iniciando carregamento dos dados para data {data_execucao}")
    
    try:
        data_execucao = datetime.strptime(data_execucao, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Formato inválido para data_execucao: {data_execucao}. Esperado 'YYYY-MM-DD'.")
        
    try:
        # Obtém o caminho do arquivo transformado
        arquivo_entrada = Configuracao.obter_caminho_arquivo(
            Configuracao.DIR_TRANSFORMACAO,
            'vendas_transformadas',
            data_execucao
        )

        # Instancia a classe de carga e carrega os dados
        carga = CargaPostgres(Configuracao.CONFIG_POSTGRES)
        carga.carregar_dados(arquivo_entrada)
        
        logger.info(f"Dados carregados com sucesso para o PostgreSQL a partir de {arquivo_entrada}")
        
    except Exception as e:
        logger.error(f"Erro durante o carregamento: {str(e)}")
        raise

dag = DAG(
    'vendas_carga_postgres',
    default_args=args_padrao,
    description='Pipeline de carga de dados de vendas para o PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['vendas', 'carga', 'postgres'],
    max_active_runs=1,
)

tarefa_carga = PythonOperator(
    task_id='carregar_dados_postgres',
    python_callable=carregar_dados,
    provide_context=True,
    dag=dag,
)
