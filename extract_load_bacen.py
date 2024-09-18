import requests
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from time import sleep
import urllib

# Lista de URLs, descritores e descrições
series = [
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.5364/dados', 'tabela': 'nfsp_pib_setor_publico_mes'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.5507/dados', 'tabela': 'nfsp_pib_setor_publico_ano'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.4513/dados', 'tabela': 'divida_liquida_pib_setor_publico'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.21619/dados', 'tabela': 'taxa_de_cambio_euro'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.21623/dados', 'tabela': 'taxa_de_cambio_libra'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados', 'tabela': 'taxa_de_cambio_dolar'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados', 'tabela': 'selic_anualizada'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados', 'tabela': 'selic_meta'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.189/dados', 'tabela': 'igpm_mes'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.13521/dados', 'tabela': 'meta_inflacao'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.20539/dados', 'tabela': 'carteira_de_credito'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.20540/dados', 'tabela': 'carteira_de_credito_pj'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.20541/dados', 'tabela': 'carteira_de_credito_pf'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.20631/dados', 'tabela': 'concessao_de_credito'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.20632/dados', 'tabela': 'concessao_de_credito_pj'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.20633/dados', 'tabela': 'concessao_de_credito_pf'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.29037/dados', 'tabela': 'endividamento_familias'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.29038/dados', 'tabela': 'endividamento_familias_s_habitacional'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.4393/dados', 'tabela': 'confianca_consumidor'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.7341/dados', 'tabela': 'confianca_industrial'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.17660/dados', 'tabela': 'confianca_servicos'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.1402/dados', 'tabela': 'energia_comercial'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.1403/dados', 'tabela': 'energia_residencial'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.1404/dados', 'tabela': 'energia_industrial'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.1405/dados', 'tabela': 'energia_outros'},
    {'url': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.1406/dados', 'tabela': 'energia_total'}
]

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

##### Criar a conexão com o banco de dados - aqui precisa alterar os dados para entrar no novo servidor da Azure #####
params = urllib.parse.quote_plus(f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}")
conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
engine = create_engine(conn_str)

# Loop pelas séries para fazer as requisições e salvar no banco de dados
for serie in series:
    params = {'formato': 'json'}
    sucesso = False
    tentativas = 0
    max_tentativas = 5
    
    while not sucesso and tentativas < max_tentativas:
        try:
            response = requests.get(serie['url'], params=params)
            response.raise_for_status()  # Lança a exceção
            
            data = response.json()
            df = pd.DataFrame(data)
            
            # Inserir dados no banco de dados
            df.to_sql(serie['tabela'], engine, if_exists='replace', index=False, schema='bacen')
            print(f"Dados da série {serie['tabela']} salvos com sucesso no banco de dados.")

            sucesso = True
        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer a requisição para a série {serie['tabela']}: {e}")
            tentativas += 1
            sleep(10)
    
    if not sucesso:
        print(f"Falha ao obter dados da série {serie['tabela']} após {max_tentativas} tentativas.")
        
print("Fim das importações de dados do BACEN.")