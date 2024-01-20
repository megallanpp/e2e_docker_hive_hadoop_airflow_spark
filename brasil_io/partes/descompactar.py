import gzip
import shutil
from datetime import datetime
import os
import sys

def descompactar_arquivo_gz(arquivo_gz, arquivo_destino):
    # Verifica se o arquivo_gz existe
    if not os.path.exists(arquivo_gz):
        print(f"O arquivo {arquivo_gz} não existe.")
        sys.exit(1)

    # Verifica se o arquivo_destino já existe
    if os.path.exists(arquivo_destino):
        # Se existir, renomeia o arquivo_destino adicionando '_old_' e a data e hora
        data_hora_atual = datetime.now()
        formato_data_hora = data_hora_atual.strftime('%Y_%m_%d_%H%M%S')
        novo_nome_arquivo = f"{os.path.splitext(arquivo_destino)[0]}_old_{formato_data_hora}.csv"
        os.rename(arquivo_destino, novo_nome_arquivo)
        print(f"Arquivo existente renomeado para: {novo_nome_arquivo}")

    # Descompacta o arquivo_gz para o arquivo_destino
    with gzip.open(arquivo_gz, 'rb') as arquivo_comprimido:
        with open(arquivo_destino, 'wb') as arquivo_destino:
            arquivo_destino.write(arquivo_comprimido.read())

if __name__ == "__main__":
    arquivo_gz = 'gastos-deputados_cota_parlamentar.csv.gz'
    arquivo_destino = 'cota-parlamentar.csv'

    descompactar_arquivo_gz(arquivo_gz, arquivo_destino)

    print(f"Operação concluída. Arquivo descompactado para 'cota-parlamentar.csv'.")
