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
