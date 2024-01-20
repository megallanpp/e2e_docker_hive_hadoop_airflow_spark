import csv
import gzip
import io
import json
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

import gzip
import shutil
from datetime import datetime
import os
import sys


class BrasilIO:

    base_url = "https://api.brasil.io/v1/"

    def __init__(self, auth_token):
        self.__auth_token = auth_token

    @property
    def headers(self):
        return {
            "User-Agent": "python-urllib/brasilio-client-0.1.0",
        }
        
    @property
    def api_headers(self):
        data = self.headers
        data.update({"Authorization": f"Token {self.__auth_token}"})
        return data

    def api_request(self, path, query_string=None):
        url = urljoin(self.base_url, path)
        if query_string:
            url += "?" + urlencode(query_string)
        request = Request(url, headers=self.api_headers)
        response = urlopen(request)
        return json.load(response)

    def data(self, dataset_slug, table_name, filters=None):
        url = f"dataset/{dataset_slug}/{table_name}/data/"
        filters = filters or {}
        filters["page"] = 1

        finished = False
        while not finished:
            response = self.request(url, filters)
            next_page = response.get("next", None)
            for row in response["results"]:
                yield row
            filters = {}
            url = next_page
            finished = next_page is None

    def download(self, dataset, table_name):
        url = f"https://data.brasil.io/dataset/{dataset}/{table_name}.csv.gz"
        request = Request(url, headers=self.headers)
        response = urlopen(request)
        return response


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

def renomear_arquivo_gz(arquivo_gz):
    # Verifica se o arquivo_gz existe
    if not os.path.exists(arquivo_gz):
        print(f"O arquivo {arquivo_gz} não existe.")
        sys.exit(1)

    # Obtém a data e hora atual
    data_hora_atual = datetime.now()
    formato_data_hora = data_hora_atual.strftime('%Y_%m_%d_%H%M%S')

    # Gera o novo nome do arquivo com base na data e hora
    novo_nome_arquivo_gz = f"processado_{formato_data_hora}_{arquivo_gz}"

    # Renomeia o arquivo_gz adicionando a palavra 'processado' e a data e hora
    os.rename(arquivo_gz, novo_nome_arquivo_gz)
    print(f"Arquivo processado e renomeado para: {novo_nome_arquivo_gz}")

    # Renomeia o arquivo_gz após a descompactação
    renomear_arquivo_gz(arquivo_gz)


def ingestao_dados():
    api = BrasilIO("b9a1249f56d1550d88fe7a9d55318a4be343ac91")
    dataset_slug = "gastos-deputados"
    table_name = "cota_parlamentar"

    # Para baixar o arquivo completo:

    # Após fazer o download, você salvá-lo no disco ou percorrer o arquivo em
    # memória. Para salvá-lo no disco:
    response = api.download(dataset_slug, table_name)
    with open(f"{dataset_slug}_{table_name}.csv.gz", mode="wb") as fobj:
        fobj.write(response.read())

    # arquivo_gz = 'gastos-deputados_cota_parlamentar.csv.gz'
    arquivo_gz = f"{dataset_slug}_{table_name}.csv.gz"
    arquivo_destino = 'cota-parlamentar.csv'

    descompactar_arquivo_gz(arquivo_gz, arquivo_destino)

    print(f"Operação concluída. Arquivo descompactado para 'cota-parlamentar.csv'.")
            

if __name__ == "__main__":

    ingestao_dados()
