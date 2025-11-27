import os
from Helpers.elastic import indexar_json_anla

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "Data", "ANLA_json")

if __name__ == "__main__":
    print("Cargando resoluciones ANLA desde:", JSON_DIR)
    resultado = indexar_json_anla(JSON_DIR)
    print("Resultado:")
    print(resultado)