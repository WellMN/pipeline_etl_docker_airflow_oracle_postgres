from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
from src.transformacao import TransformadorDadosVendas
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

def transformar_dados(**context):
    """Transforma os dados extraídos e salva em novo arquivo Parquet"""
    data_execucao = context['dag_run'].conf.get('data_execucao', context['ds'])
    logger.info(f"Iniciando transformação dos dados para data {data_execucao}")
    
    try:
        data_execucao = datetime.strptime(data_execucao, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Formato inválido para data_execucao: {data_execucao}. Esperado 'YYYY-MM-DD'.")
        
    try:
        arquivo_entrada = Configuracao.obter_caminho_arquivo(
            Configuracao.DIR_EXTRACAO,
            'vendas_extraidas',
            data_execucao
        )

        df = pd.read_parquet(arquivo_entrada)
        if df.empty:
            raise ValueError(f"O arquivo {arquivo_entrada} está vazio ou corrompido.")

        logger.info(f"Arquivo de entrada carregado com sucesso: {arquivo_entrada}")
        
        transformador = TransformadorDadosVendas()
        df_transformado = transformador.transformar_dados(df)
        
        arquivo_saida = Configuracao.obter_caminho_arquivo(
            Configuracao.DIR_TRANSFORMACAO,
            'vendas_transformadas',
            data_execucao
        )
        
        df_transformado.to_parquet(arquivo_saida, index=False)
        logger.info(f"Dados transformados salvos em {arquivo_saida}")
        
        return arquivo_saida
        
    except Exception as e:
        logger.error(f"Erro durante a transformação: {str(e)}")
        raise

dag = DAG(
    'vendas_transformacao',
    default_args=args_padrao,
    description='Pipeline de transformação de dados de vendas',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['vendas', 'transformacao'],
    max_active_runs=1,
)

tarefa_transformacao = PythonOperator(
    task_id='transformar_dados_vendas',
    python_callable=transformar_dados,
    provide_context=True,
    dag=dag,
)

# Adicionando o TriggerDagRunOperator para acionar a DAG de carga
trigger_carga = TriggerDagRunOperator(
    task_id='trigger_carga_postgres',
    trigger_dag_id='vendas_carga_postgres',  # Nome da DAG de carga
    poke_interval=30,  # Tempo entre verificações do estado da DAG filha
	conf={"data_execucao": "{{ ds }}"},  # Passa a data de execução para a DAG de carga
    dag=dag,
)

# Definindo a ordem das tarefas
tarefa_transformacao >> trigger_carga