# 🎯 O que este projeto faz?
Este pipeline ingere, limpa, valida, transforma e exporta dados tabulares desestruturados, simulando um cenário real de automação de relatórios em ambientes corporativos (ex: CRM, vendas, operações).

A partir de um arquivo bruto (relatorio.csv) com problemas reais — como valores ausentes, formatos inconsistentes de data, duplicatas, valores inválidos e dados mal tipados — o código:

✅ Carrega o arquivo com tratamento de erros
✅ Analisa a qualidade dos dados (com suporte a relatório visual)
✅ Limpa os dados com regras de negócio explícitas
✅ Valida a estrutura e conteúdo com esquema declarativo (Pandera)
✅ Exporta o resultado final em 3 formatos padrão da indústria:
 - CSV (compatibilidade universal)
 - Parquet (formato otimizado para data lakes e pipelines modernos)
 - JSON (pronto para APIs ou integrações)

Tudo isso com logging estruturado, configuração externa, testes unitários e reprodutibilidade total.

---

# 🛠️ Tecnologias e Ferramentas Utilizadas
- Manipulação de dados: numpy, pandas
- Validação de schema: pandera
- Qualidade dos dados: ydata-profiling (Gera relatório visual automático) 
- Datas: pd.to_datetime(), datetime (Tratamento de múltiplos formatos de data)
- Configuração: python-dotenv (Ajuste de regras - boas práticas de DevOps)
- Logging: logging (nativo) ( depuração em produção)
- Testes: pytest (Valida as regras de negócio com testes automatizados)
- Exportação eficiente: pyarrow (via to_parquet) (formato padrão em arquiteturas de dados)
- Reprodutibilidade: requirements.txt (Garante replicabilidade do sistema) 
---
# 📦 Estrutura do Projeto
manipulacao_dados/  
├── .env                     # Configurações (ex: valor mínimo de compra)  
├── requirements.txt         # Dependências exatas (reprodutibilidade)  
├── relatorio.csv            # Dados brutos (com falhas reais)  
├── src/  
│   └── manipulacao.py       # Pipeline principal (limpeza + validação + export)  
├── tests/  
│   └── test_validacoes.py   # Testes unitários das regras de negócio  
├── output/                  # Dados limpos (CSV, Parquet, JSON)  
└── docs/                    # Relatório de qualidade (HTML, se gerado)  
---
# ▶️ Como Executar
### Crie e ative um ambiente virtual:
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

### Instale as dependências:
pip install -r requirements.txt

### Execute o pipeline:
python src/manipulacao.py

### Execute os testes:
pytest tests/ -v

#### ➡️ Os dados limpos serão salvos em output/.
---


