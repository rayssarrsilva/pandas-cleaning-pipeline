import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from manipulacao import limpar_dados

def test_valor_minimo_respeitado():
    # Simula dados de entrada
    dados = pd.DataFrame({
        'nome': ['Ana', 'Bruno', 'Carlos'],
        'valor_compra': [5.0, 50.0, 150.0],
        'data_compra': ['2023-01-01', '2023-02-01', '2023-03-01']
    })
    df_limpo = limpar_dados(dados)
    assert df_limpo['valor_compra'].min() >= 10.0
    assert len(df_limpo) == 2  # Ana deve ser removida

def test_sem_nulos_em_nome():
    dados = pd.DataFrame({
        'nome': [None, 'Bruno'],
        'valor_compra': [100, 200],
        'data_compra': ['2023-01-01', '2023-02-01']
    })
    df_limpo = limpar_dados(dados)
    assert not df_limpo['nome'].isna().any()


# Executa: pytest tests/ -v OU pytest -v na raiz do projeto
