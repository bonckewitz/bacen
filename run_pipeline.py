import subprocess
import os
import time
from dotenv import load_dotenv

# Função para executar scripts Python
def executar_scripts():
    try:
        # Carrega o arquivo .env
        load_dotenv()
        
        # Caminho para os arquivos
        source = os.path.join(os.getcwd())

        # Executa extração
        print("Baixando dados do BACEN")
        extract = os.path.join(source, 'extract_load_bacen.py')
        subprocess.run(['python3', extract])

          # Executa transformação
        print("Executando DBT")
        subprocess.run(['dbt', 'run'])

    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar o script: {e}")

# Chamando a função para executar os scripts
if __name__ == "__main__":

    start_time = time.time()

    # Executa os scripts de ETL
    executar_scripts()

    # Marca o fim do tempo
    end_time = time.time()

    # Calcula o tempo total de execução
    elapsed_time = (end_time - start_time) / 60

    print("Extração, carregamento, transformação e transferência dos dados finalizados.")
    print(f"Tempo total de execução: {elapsed_time:.2f} minutos")