import logging
import cx_Oracle
import pandas as pd
from typing import Optional
from pathlib import Path
from src.config import ConfiguracaoBanco

logger = logging.getLogger(__name__)

class ExtratorOracle:
    def __init__(self, config: ConfiguracaoBanco):
        self.config = config
        self.conexao = None

    def conectar(self) -> None:
        """Estabelece conexão com o banco Oracle"""
        try:
            dsn = cx_Oracle.makedsn(
                self.config.host,
                self.config.porta,
                service_name=self.config.banco
            )
            self.conexao = cx_Oracle.connect(
                user=self.config.usuario,
                password=self.config.senha,
                dsn=dsn
            )
            logger.info("Conexão com Oracle estabelecida com sucesso")
        except Exception as erro:
            logger.error(f"Erro ao conectar com Oracle: {str(erro)}")
            raise

    def _executar_consulta(self, consulta: str, tamanho_lote: int) -> pd.DataFrame:
        """Executa consulta SQL e retorna DataFrame"""
        try:
            df = pd.read_sql(
                consulta,
                self.conexao,
                chunksize=tamanho_lote
            )
            return df
        except Exception as erro:
            logger.error(f"Erro ao executar consulta: {str(erro)}")
            raise

    def extrair_dados_vendas(self, tamanho_lote: int) -> pd.DataFrame:
        """
        Extrai dados da tabela SALES_TRANSACTIONS
        
        Args:
            tamanho_lote: Quantidade de registros por lote
            
        Returns:
            DataFrame com os dados extraídos
        """
        if not self.conexao:
            self.conectar()

        try:
            logger.info("Iniciando extração dos dados de vendas")
            
            consulta = """
                SELECT 
                    TRANSACTION_ID,
                    CUSTOMER_ID,
                    AMOUNT,
                    TRANSACTION_DATE
                FROM "SYSTEM"."SALES_TRANSACTIONS" 
                WHERE TRUNC(TRANSACTION_DATE) = TRUNC(SYSDATE - 1)
            """

            dataframes = []
            for chunk in self._executar_consulta(consulta, tamanho_lote):
                dataframes.append(chunk)
                logger.debug(f"Lote de {len(chunk)} registros processado")

            df_final = pd.concat(dataframes, ignore_index=True)
            
            # Validações básicas
            if df_final.empty:
                logger.warning("Nenhum dado encontrado para extração")
                return pd.DataFrame()
                
            logger.info(f"Extração concluída. Total de registros: {len(df_final)}")
            return df_final
            
        except Exception as erro:
            logger.error(f"Erro durante extração dos dados: {str(erro)}")
            raise
        finally:
            if self.conexao:
                self.conexao.close()
                logger.debug("Conexão com Oracle fechada")

    def salvar_dados(self, df: pd.DataFrame, caminho_arquivo: str) -> None:
        """
        Salva DataFrame em arquivo Parquet
        
        Args:
            df: DataFrame com os dados
            caminho_arquivo: Caminho completo do arquivo de saída
        """
        try:
            # Cria diretório se não existir
            Path(caminho_arquivo).parent.mkdir(parents=True, exist_ok=True)
            
            # Salva arquivo
            df.to_parquet(
                caminho_arquivo,
                index=False,
                compression='snappy'  # Bom equilíbrio entre compressão e velocidade
            )
            
            logger.info(f"Dados salvos com sucesso em: {caminho_arquivo}")
            
        except Exception as erro:
            logger.error(f"Erro ao salvar arquivo: {str(erro)}")
            raise
