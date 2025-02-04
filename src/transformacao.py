import pandas as pd
import numpy as np
import logging
from typing import Optional, Dict
from pathlib import Path

logger = logging.getLogger(__name__)

class TransformadorDadosVendas:
    def __init__(self):
        self.metricas = {
            'registros_iniciais': 0,
            'registros_invalidos': 0,
            'registros_processados': 0
        }

    def _categorizar_valor(self, amount: float) -> str:
        """
        Categoriza o valor da transação
        
        Args:
            amount: Valor da transação
            
        Returns:
            category (LOW, MEDIUM, HIGH)
        """
        if amount < 100:
            return 'LOW'
        elif amount <= 500:
            return 'MEDIUM'
        else:
            return 'HIGH'

    def _validar_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida e limpa os dados antes da transformação
        
        Args:
            df: DataFrame com os dados
            
        Returns:
            DataFrame com dados válidos
        """
        # Normaliza os nomes das colunas para evitar erro
        df.columns = df.columns.str.lower()

        # Remove registros com valores nulos
        df_valido = df.dropna(subset=['transaction_id', 'customer_id', 'amount'])
        
        # Remove duplicatas
        df_valido = df_valido.drop_duplicates(subset=['transaction_id'])
        
        
        return df_valido

    def transformar_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformações nos dados de vendas
        
        Args:
            df: DataFrame com os dados brutos
            
        Returns:
            DataFrame com dados transformados
        """
        try:
            logger.info("Iniciando transformação dos dados")
            
            self.metricas['registros_iniciais'] = len(df)
            
            # Validação inicial
            df_valido = self._validar_dados(df)
                                    
            if df_valido.empty:
                logger.warning("Nenhum dado válido para transformação")
                return pd.DataFrame()
            
            # Aplica transformações
            df_transformado = df_valido.copy()
            
            # Remove registros com valor <= 0
            df_transformado = df_transformado[df_transformado['amount'] > 0]
            
            # Normaliza ID do cliente
            df_transformado['customer_id'] = df_transformado['customer_id'].str.upper()
            
            # Adiciona categoria
            df_transformado['category'] = df_transformado['amount'].apply(self._categorizar_valor)
            
            # Adiciona data de processamento para metadados posteriormente
            #df_transformado['data_processamento'] = pd.Timestamp.now()
            
            # Registra métricas
            self.metricas['registros_invalidos'] = len(df) - len(df_transformado)

            # Atualiza métricas
            self.metricas['registros_processados'] = len(df_transformado)
            
            self._registrar_metricas()
            
            logger.info("Transformação concluída com sucesso")
            return df_transformado
            
        except Exception as erro:
            logger.error(f"Erro durante transformação dos dados: {str(erro)}")
            raise

    def _registrar_metricas(self) -> None:
        """Registra métricas de processamento"""
        logger.info("Métricas de transformação:")
        logger.info(f"- Registros iniciais: {self.metricas['registros_iniciais']}")
        logger.info(f"- Registros inválidos: {self.metricas['registros_invalidos']}")
        logger.info(f"- Registros processados: {self.metricas['registros_processados']}")

    def carregar_arquivo(self, caminho_arquivo: str) -> pd.DataFrame:
        """
        Carrega dados do arquivo Parquet
        
        Args:
            caminho_arquivo: Caminho do arquivo de entrada
            
        Returns:
            DataFrame com os dados carregados
        """
        try:
            logger.info(f"Carregando dados do arquivo: {caminho_arquivo}")
            return pd.read_parquet(caminho_arquivo)
        except Exception as erro:
            logger.error(f"Erro ao carregar arquivo: {str(erro)}")
            raise

    def salvar_arquivo(self, df: pd.DataFrame, caminho_arquivo: str) -> None:
        """
        Salva dados transformados em arquivo Parquet
        
        Args:
            df: DataFrame com dados transformados
            caminho_arquivo: Caminho do arquivo de saída
        """
        try:
            # Cria diretório se não existir
            Path(caminho_arquivo).parent.mkdir(parents=True, exist_ok=True)
            
            # Salva arquivo
            df.to_parquet(
                caminho_arquivo,
                index=False,
                compression='snappy'
            )
            
            logger.info(f"Dados transformados salvos em: {caminho_arquivo}")
            
        except Exception as erro:
            logger.error(f"Erro ao salvar arquivo transformado: {str(erro)}")
            raise
