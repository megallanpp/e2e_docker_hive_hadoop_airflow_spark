
import class_BrasilIO as BrasilIO

if __name__ == "__main__":
    api = BrasilIO("b9a1249f56d1550d88fe7a9d55318a4be343ac91")
    dataset_slug = "gastos-deputados"
    table_name = "cota_parlamentar"

    # Caso queira percorrer o CSV em memória:
    # response = api.download(dataset_slug, table_name)
    # fobj = io.TextIOWrapper(gzip.GzipFile(fileobj=response), encoding="utf-8")
    # reader = csv.DictReader(fobj)
    # for row in reader:
    #     pass  # faça algo com `row`

    # Para navegar pela API:
    filters = {"state": "PR", "is_last": True}
    data = api.data(dataset_slug, table_name, filters)
    for row in data:
        pass  # faça algo com `row`