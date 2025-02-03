from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
from src.extrator_oracle import ExtratorOracle
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
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
}

def extrair_dados(data_execucao: str, **context):    
    """Extrai dados do Oracle e salva em arquivo Parquet"""
    logger.info(f"Iniciando extração dos dados para data {data_execucao}")
    
    try:
        data_execucao = datetime.strptime(data_execucao, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Formato inválido para data_execucao: {data_execucao}. Esperado 'YYYY-MM-DD'.")
    
    Configuracao.criar_diretorios()
    
    extrator = ExtratorOracle(Configuracao.CONFIG_ORACLE)
    
    try:
        df = extrator.extrair_dados_vendas(Configuracao.TAMANHO_LOTE)
        
        caminho_arquivo = Configuracao.obter_caminho_arquivo(
            Configuracao.DIR_EXTRACAO,
            'vendas_extraidas',
            data_execucao
        )
        
        df.to_parquet(caminho_arquivo, index=False)
        logger.info(f"Dados salvos em {caminho_arquivo}")
        
        return caminho_arquivo
        
    except Exception as e:
        logger.error(f"Erro durante a extração: {str(e)}")
        raise

dag = DAG(
    'vendas_extracao',
    default_args=args_padrao,
    description='Pipeline de extração de dados de vendas',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['vendas', 'extracao'],
    max_active_runs=1,
)

tarefa_extracao = PythonOperator(
    task_id='extrair_dados_vendas',
    python_callable=extrair_dados,
    op_kwargs={'data_execucao': '{{ ds }}'},
    provide_context=True,
    dag=dag,
)

trigger_transformacao = TriggerDagRunOperator(
    task_id='trigger_transformacao',
    trigger_dag_id='vendas_transformacao',
    poke_interval=30,  # Tempo entre verificações do estado da DAG filha
    conf={"data_execucao": "{{ ds }}"},
    dag=dag,
)

tarefa_extracao >> trigger_transformacao
