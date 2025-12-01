import os
from Helpers.elastic import indexar_json_anla, ELASTIC_INDEX_DEFAULT

if __name__ == "__main__":
    # Carpeta donde están tus JSON ANLA (ruta relativa a este archivo)
    json_dir = os.path.join("Data", "ANLA_json")

    print("Usando carpeta:", os.path.abspath(json_dir))
    print("Índice destino:", ELASTIC_INDEX_DEFAULT)

    if not os.path.isdir(json_dir):
        print("❌ La carpeta de JSON no existe. Revisa la ruta:", json_dir)
    else:
        resultado = indexar_json_anla(json_dir, ELASTIC_INDEX_DEFAULT)
        print("Resultado indexación:")
        print(resultado)