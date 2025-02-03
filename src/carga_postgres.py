import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional
from pathlib import Path
from src.config import ConfiguracaoBanco, Configuracao

logger = logging.getLogger(__name__)

class CargaPostgres:
    def __init__(self, config: ConfiguracaoBanco):
        self.config = config
        self.conexao = None
        self.engine = None
        self.metricas = {
            'registros_inseridos': 0
        }

    def conectar(self) -> None:
        """Estabelece conexão com o banco PostgreSQL usando psycopg2 e SQLAlchemy"""
        try:
            # Conexão usando psycopg2 (para operações manuais)
            self.conexao = psycopg2.connect(
                host=self.config.host,
                port=self.config.porta,
                database=self.config.banco,
                user=self.config.usuario,
                password=self.config.senha
            )
            logger.info("Conexão com PostgreSQL (psycopg2) estabelecida com sucesso")

            # Conexão usando SQLAlchemy (para to_sql com lotes)
            self.engine = create_engine(
                f"postgresql+psycopg2://{self.config.usuario}:{self.config.senha}@{self.config.host}:{self.config.porta}/{self.config.banco}"
            )
            logger.info("Conexão com PostgreSQL (SQLAlchemy) estabelecida com sucesso")
        except Exception as erro:
            logger.error(f"Erro ao conectar com PostgreSQL: {str(erro)}")
            raise

    def criar_esquema_tabela(self) -> None:
        """
        Cria o esquema 'analytics' e a tabela 'analytics_transactions' se não existirem.
        """
        if not self.conexao:
            self.conectar()

        try:
            with self.conexao.cursor() as cursor:
                # Cria o esquema 'analytics' se não existir
                cursor.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
                
                # Cria a tabela 'analytics_transactions' se não existir
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS analytics.analytics_transactions (
                        transaction_id INTEGER PRIMARY KEY,
                        customer_id VARCHAR(50),
                        amount DECIMAL(10,2),
                        transaction_date TIMESTAMP,
                        category VARCHAR(20)
                    );
                """)
                self.conexao.commit()
                logger.info("Esquema e tabela criados ou verificados com sucesso.")
        except Exception as erro:
            logger.error(f"Erro ao criar esquema/tabela: {str(erro)}")
            if self.conexao:
                self.conexao.rollback()
            raise

    def carregar_dados(self, caminho_arquivo: str) -> None:
        """
        Carrega dados de um arquivo Parquet para o PostgreSQL em lotes.
        
        Args:
            caminho_arquivo: Caminho do arquivo Parquet com os dados transformados
        """
        if not self.engine:
            self.conectar()

        try:
            logger.info(f"Carregando dados do arquivo {caminho_arquivo} para o PostgreSQL")
            
            # Verifica e cria o esquema e a tabela, se necessário
            self.criar_esquema_tabela()
            
            # Lê o arquivo Parquet
            df = pd.read_parquet(caminho_arquivo)
            
            if df.empty:
                logger.warning("Nenhum dado para carregar")
                return

            # Carrega dados em lotes
            df.to_sql(
                name='analytics_transactions',
                schema='analytics',
                con=self.engine,
                if_exists='append',  # Adiciona os dados à tabela existente
                index=False,         # Não inclui o índice do DataFrame
                chunksize=Configuracao.TAMANHO_LOTE,  # Usa o tamanho do lote definido no config.py
                method='multi'       # Insere múltiplas linhas por vez
            )

            # Atualiza métricas
            self.metricas['registros_inseridos'] = len(df)
            self._registrar_metricas()

            logger.info(f"{len(df)} registros carregados com sucesso em lotes de {Configuracao.TAMANHO_LOTE}")

        except Exception as erro:
            logger.error(f"Erro durante o carregamento dos dados: {str(erro)}")
            raise
        finally:
            if self.conexao:
                self.conexao.close()
                logger.debug("Conexão com PostgreSQL fechada")

    def _registrar_metricas(self) -> None:
        """Registra métricas de processamento"""
        logger.info("Métricas de carga:")
        logger.info(f"- Registros inseridos: {self.metricas['registros_inseridos']}")