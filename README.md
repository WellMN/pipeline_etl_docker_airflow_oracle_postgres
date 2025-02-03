# Pipeline com ETL usando Airflow, Oracle, Postgres e Docker

O projeto utiliza Docker para conteinerização do processo de ETL com Airflow, Oracle e Postgres.

<img src="img/Docker_Containers.png" alt="Containers" width="1000"/>

## 1. Processo de ETL no Airflow

Este projeto exemplifica um processo de ETL (Extração, Transformação e Carga) utilizando o Apache Airflow em um ambiente Docker. O fluxo de dados envolve a extração de dados de um banco de dados Oracle, a transformação desses dados e, finalmente, a carga no PostgreSQL.

### 1.1 Dags no Airflow

<img src="img/Dags.png" alt="Dags" width="1000"/>
---

### 1.1.1 Fluxo ETL - Estratégia por Triggers

<img src="img/estrategia_triggers.png" alt="Triggers" width="1000"/>


## 2. Extração

A extração dos dados é realizada por meio de uma DAG (Directed Acyclic Graph) no Airflow, utilizando o arquivo `dag_extracao_oracle.py`. O processo de extração é feito pela classe `ExtratorOracle`, que se conecta ao banco de dados Oracle e executa uma consulta SQL para obter os dados de vendas.



### 2.1 Código da Extração

```python
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

```

### 2.2 Funcionamento

- **Conexão com Oracle**: A classe `ExtratorOracle` estabelece a conexão com o banco de dados Oracle utilizando a biblioteca `cx_Oracle`.
- **Consulta SQL**: A consulta é realizada na tabela `SALES_TRANSACTIONS` para obter dados de vendas do dia anterior.
- **Armazenamento**: Os dados extraídos são salvos em um arquivo Parquet para uso posterior na transformação.

### 2.3 Registros no banco de dados Oracle
<img src="img/registro_banco_oracle.png" alt="Registros_Oracle" width="600"/>

### 2.4 Registros extraídos do banco de dados Oracle
<img src="img/dados_extraidos_sem_transformacao.png" alt="Dados_sem_transformacao" width="600"/>

### 2.5 Log da execução da extração do Oracle no Airflow
<img src="img/Log_extracao.png" alt="Log_extracao" width="800"/>

---

## 3. Transformação

Após a extração, os dados precisam ser transformados. Essa etapa é gerenciada pela DAG `dag-transformacao.py`, onde a classe `TransformadorDadosVendas` aplica as transformações necessárias nos dados extraídos.

### 3.1 Código da Transformação

```python
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
```

### 3.2 Funcionamento

- **Validação**: A transformação inclui validações para remover registros inválidos e duplicados.
- **Categorização**: Os dados são categorizados com base no valor da transação (LOW, MEDIUM, HIGH).
- **Armazenamento**: Os dados transformados são salvos em um novo arquivo Parquet.


### 3.3 Registros tratados e enriquecidos prontos para carga
<img src="img/dados_enriquecidos_transformados.png" alt="dados_enriquecidos" width="600"/>

### 3.4 Log da execução da transformação dos dados no Airflow
<img src="img/Log_transformacaocao.png" alt="log_transformacao" width="800"/>

---

## 4. Carga

A carga dos dados transformados para o banco de dados PostgreSQL é feita pela DAG `dag-carga_postgres.py`. A classe `CargaPostgres` estabelece a conexão com o banco e carrega os dados.

### 4.1 Código da Carga

```python
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

```

### 4.2 Funcionamento

- **Conexão com PostgreSQL**: A classe `CargaPostgres` utiliza `psycopg2` e `SQLAlchemy` para conectar ao PostgreSQL.
- **Carga em Lotes**: Os dados são carregados em lotes, garantindo eficiência e controle de erros.

### 4.3 Registros carregados no banco de dados Postgres
<img src="img/registros_carregados_postgres.png" alt="registros_carregados" width="800"/>

### 4.4 Log da execução da carga no Postgres no Airflow
<img src="img/Log_carga.png" alt="log_carga" width="800"/>

---

## 5. Arquivo de Configuração

O arquivo `config.py` contém as configurações necessárias para a conexão com os bancos de dados Oracle e PostgreSQL, tamanho dos chunks para carga, tipo do log, além de definir os diretórios para a extração e transformação dos dados.

### Exemplo de Configuração

```python
from dataclasses import dataclass
from typing import Dict, Union
import os
from datetime import datetime

@dataclass
class ConfiguracaoBanco:
    host: str
    porta: int
    banco: str
    usuario: str
    senha: str

class Configuracao:
    # Configurações dos bancos de dados
    CONFIG_ORACLE = ConfiguracaoBanco(
        host=os.getenv("ORACLE_HOST", "teste_oracle"),
        porta=int(os.getenv("ORACLE_PORT", "1521")),
        banco=os.getenv("ORACLE_DB", "FREE"),
        usuario=os.getenv("ORACLE_USER", "SYSTEM"),
        senha=os.getenv("ORACLE_PASSWORD", "oracle")
    )

    CONFIG_POSTGRES = ConfiguracaoBanco(
        host=os.getenv("PG_HOST", "postgres_teste"),
        porta=int(os.getenv("PG_PORT", "5432")),
        banco=os.getenv("PG_DB", "postgres"),
        usuario=os.getenv("PG_USER", "postgres"),
        senha=os.getenv("PG_PASSWORD", "postgres")
    )

    # Diretórios de dados temporários
    DIR_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DIR_DADOS_TEMP = os.path.join(DIR_BASE, 'dados_temp')
    DIR_EXTRACAO = os.path.join(DIR_DADOS_TEMP, 'extracao')
    DIR_TRANSFORMACAO = os.path.join(DIR_DADOS_TEMP, 'transformacao')

    # Configurações gerais
    TAMANHO_LOTE = 1000
    NIVEL_LOG = "INFO"
    
    @staticmethod
    def obter_caminho_arquivo(diretorio: str, prefixo: str, data_execucao: Union[str, datetime]) -> str:
        """Gera o caminho do arquivo baseado na data de execução"""
        
        # Converte string para datetime se necessário
        if isinstance(data_execucao, str):
            try:
                data_execucao = datetime.strptime(data_execucao, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Formato inválido para data_execucao: {data_execucao}. Esperado 'YYYY-MM-DD'.")

        nome_arquivo = f"{prefixo}_{data_execucao.strftime('%Y%m%d')}.parquet"
        return os.path.join(diretorio, nome_arquivo)

    @staticmethod
    def criar_diretorios():
        """Cria os diretórios necessários se não existirem"""
        for diretorio in [Configuracao.DIR_DADOS_TEMP, 
                         Configuracao.DIR_EXTRACAO, 
                         Configuracao.DIR_TRANSFORMACAO]:
            os.makedirs(diretorio, exist_ok=True)

```

---

## 6. Conclusão

Com isso, temos uma visão detalhada do processo de ETL utilizando o Apache Airflow em um ambiente Docker. Cada etapa do processo é gerenciada por DAGs específicas, garantindo a execução sequencial e a integridade dos dados ao longo do pipeline.

