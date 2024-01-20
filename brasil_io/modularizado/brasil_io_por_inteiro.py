
import class_BrasilIO as BrasilIO

if __name__ == "__main__":
    api = BrasilIO("b9a1249f56d1550d88fe7a9d55318a4be343ac91")
    dataset_slug = "gastos-deputados"
    table_name = "cota_parlamentar"

    # Para baixar o arquivo completo:

    # Após fazer o download, você salvá-lo no disco ou percorrer o arquivo em
    # memória. Para salvá-lo no disco:
    response = api.download(dataset_slug, table_name)
    with open(f"{dataset_slug}_{table_name}.csv.gz", mode="wb") as fobj:
        fobj.write(response.read())
    # TODO: o código acima pode ser melhorado de forma a não utilizar
    # `response.read()` para não colocar todo oarquivo em memória e sim fazer
    # streaming da resposta HTTP e salvar cada chunk diretamente no `fobj`.
