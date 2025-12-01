from Helpers.elastic import ElasticSearch
from app import ELASTIC_CLOUD_URL, ELASTIC_API_KEY, ELASTIC_INDEX_DEFAULT

# Crear cliente de ElasticSearch con los mismos datos que usa la app
elastic = ElasticSearch(
    cloud_url=ELASTIC_CLOUD_URL,
    api_key=ELASTIC_API_KEY
)

# Query sencilla: traer algunos documentos
query = {
    "query": {
        "match_all": {}
    }
}

print("Buscando en Elastic...")
resp = elastic.buscar(
    index=ELASTIC_INDEX_DEFAULT,
    query=query,
    size=5
)

print("Respuesta de Elastic:")
print(resp)