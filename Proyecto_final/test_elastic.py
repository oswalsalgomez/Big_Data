from Helpers.elastic import ElasticSearchClient
from app import ELASTIC_INDEX_DEFAULT

elastic = ElasticSearchClient()

query = {
    "query": {
        "match_all": {}
    }
}

print("Buscando en Elastic...")
resp = elastic.buscar(ELASTIC_INDEX_DEFAULT, query, size=5)

print(resp)