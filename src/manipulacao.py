import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import pandera as pa
from pandera import Column, DataFrameSchema

# Carrega variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Parâmetros do .env
VALOR_MINIMO = float(os.getenv("VALOR_MINIMO_COMPRA", 10.0))
INPUT_PATH = os.getenv("CAMINHO_ENTRADA", "relatorio.csv")
OUTPUT_DIR = os.getenv("CAMINHO_SAIDA", "output/")

# Garante diretório de saída
os.makedirs(OUTPUT_DIR, exist_ok=True)

def carregar_dados(caminho: str) -> pd.DataFrame:
    """Carrega o CSV com tratamento de erros."""
    try:
        df = pd.read_csv(caminho, encoding='utf-8')
        logger.info(f"Dados carregados: {df.shape[0]} linhas, {df.shape[1]} colunas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados: {e}")
        raise

def gerar_relatorio_qualidade(df: pd.DataFrame, caminho_saida: str):
    """Gera relatório automático de qualidade dos dados."""
    try:
        from ydata_profiling import ProfileReport
        profile = ProfileReport(df, title="Relatório de Qualidade dos Dados")
        profile.to_file(os.path.join(caminho_saida, "relatorio_qualidade.html"))
        logger.info("Relatório de qualidade gerado.")
    except ImportError:
        logger.warning("ydata-profiling não instalado. Relatório não gerado.")
    except Exception as e:
        logger.error(f"Erro ao gerar relatório: {e}")

def limpar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Executa todas as etapas de limpeza e transformação."""
    logger.info("Iniciando limpeza dos dados...")
    df = df.copy()

    # 1. Padronizar nomes de colunas
    df.columns = df.columns.str.strip().str.lower()

    # 2. Tratar valores ausentes e inválidos em 'valor_compra'
    df['valor_compra'] = pd.to_numeric(df['valor_compra'], errors='coerce')
    total_nulos_valor = df['valor_compra'].isna().sum()
    df = df.dropna(subset=['valor_compra'])
    logger.info(f"Removidas {total_nulos_valor} linhas com valor_compra inválido.")

    # 2.5. Remover linhas com nome ausente
    nulos_nome = df['nome'].isna().sum()
    df = df.dropna(subset=['nome'])
    logger.info(f"Removidas {nulos_nome} linhas com nome ausente.")

    # 3. Filtrar valores mínimos (regra de negócio)
    antes = len(df)
    df = df[df['valor_compra'] >= VALOR_MINIMO]
    removidas_min = antes - len(df)
    logger.info(f"Removidas {removidas_min} linhas com valor_compra < R${VALOR_MINIMO}")

    # 4. Tratar datas
    df['data_compra'] = pd.to_datetime(df['data_compra'], errors='coerce')
    nulos_data = df['data_compra'].isna().sum()
    if nulos_data > 0:
        logger.warning(f"{nulos_data} datas inválidas. Serão mantidas como NaT.")

    # 5. Remover duplicatas exatas
    antes_dup = len(df)
    df = df.drop_duplicates()
    removidas_dup = antes_dup - len(df)
    logger.info(f"Removidas {removidas_dup} linhas duplicadas.")

    # 6. Criar colunas derivadas
    df['ano'] = df['data_compra'].dt.year
    df['mes'] = df['data_compra'].dt.month
    df['categoria_valor'] = pd.cut(
        df['valor_compra'],
        bins=[0, 100, 200, np.inf],
        labels=['Baixo', 'Médio', 'Alto']
    )

    # 7. Garantir tipos otimizados
    df['nome'] = df['nome'].astype('string')
    df['categoria_valor'] = df['categoria_valor'].astype('category')

    logger.info(f"Limpeza concluída. Dataset final: {df.shape[0]} linhas.")
    return df

def exportar_dados(df: pd.DataFrame, caminho_saida: str):
    """Exporta em múltiplos formatos."""
    base = os.path.join(caminho_saida, "relatorio_limpo")
    df.to_csv(f"{base}.csv", index=False, encoding='utf-8')
    df.to_parquet(f"{base}.parquet", index=False)
    df.to_json(f"{base}.json", orient='records', date_format='iso')
    logger.info("Dados exportados em CSV, Parquet e JSON.")

def main():
    logger.info("Iniciando pipeline de manipulação de dados...")
    df_bruto = carregar_dados(INPUT_PATH)
    gerar_relatorio_qualidade(df_bruto, "docs")
    df_limpo = limpar_dados(df_bruto)
    
    # Validações finais (regras de negócio)
    assert df_limpo['valor_compra'].min() >= VALOR_MINIMO, "Valor abaixo do mínimo encontrado após limpeza!"
    assert not df_limpo['nome'].isna().any(), "Nomes nulos não devem existir!"
    
    # 🔒 Validação estrutural com pandera (ADICIONADO)
    schema = DataFrameSchema({
        "nome": Column(str),
        "valor_compra": Column(float, pa.Check.ge(VALOR_MINIMO)),
        "data_compra": Column("datetime64[ns]", nullable=True),
        "ano": Column(float, nullable=True),
        "mes": Column(float, nullable=True),
        "categoria_valor": Column("category")
    })
    df_validado = schema.validate(df_limpo)
    logger.info("Validação de schema com pandera concluída com sucesso.")
    
    exportar_dados(df_validado, OUTPUT_DIR)
    logger.info("Pipeline concluído com sucesso!")


if __name__ == "__main__":
    main()