from elasticsearch import Elasticsearch
from typing import Dict, List, Optional, Any
import json
import os
from dotenv import load_dotenv

# Cargar variables de entorno (.env en local, env vars en Render)
load_dotenv()
ELASTIC_CLOUD_URL = os.getenv("ELASTIC_CLOUD_URL")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
ELASTIC_INDEX_DEFAULT = os.getenv("ELASTIC_INDEX_DEFAULT", "anla_resoluciones")


class ElasticSearch:
    def __init__(self, cloud_url: str, api_key: str):
        """
        Inicializa conexión a ElasticSearch Cloud
        
        Args:
            cloud_url: URL del cluster de Elastic Cloud
            api_key: API Key para autenticación
        """
        self.client = Elasticsearch(
            cloud_url,
            api_key=api_key,
            verify_certs=True
        )
        
    def test_connection(self) -> bool:
        """Prueba la conexión a ElasticSearch"""
        try:
            info = self.client.info()
            print(f"✅ Conectado a Elastic: {info['version']['number']}")
            return True
        except Exception as e:
            print(f"❌ Error al conectar con Elastic: {e}")
            return False
    
    def ejecutar_comando(self, comando_json: str) -> Dict:
        """
        Ejecuta un comando JSON en ElasticSearch
        
        Args:
            comando_json: Comando en formato JSON string
            
        Returns:
            Resultado de la ejecución o error
        """
        try:
            comando = json.loads(comando_json)
            
            # Extraer operación y parámetros
            operacion = comando.get('operacion')
            index = comando.get('index')
            
            if operacion == 'crear_index':
                # Crear índice
                mappings = comando.get('mappings', {})
                settings = comando.get('settings', {})
                
                response = self.client.indices.create(
                    index=index,
                    mappings=mappings,
                    settings=settings
                )
                return {'success': True, 'data': response}
                
            elif operacion == 'eliminar_index':
                # Eliminar índice
                response = self.client.indices.delete(index=index)
                return {'success': True, 'data': response}
                
            elif operacion == 'actualizar_mappings':
                # Actualizar mappings
                mappings = comando.get('mappings', {})
                response = self.client.indices.put_mapping(
                    index=index,
                    body=mappings
                )
                return {'success': True, 'data': response}
                
            elif operacion == 'info_index':
                # Obtener información del índice
                response = self.client.indices.get(index=index)
                return {'success': True, 'data': response}
                
            elif operacion == 'listar_indices':
                # Listar todos los índices
                response = self.client.cat.indices(format='json')
                return {'success': True, 'data': response}
                
            else:
                return {'success': False, 'error': f'Operación no soportada: {operacion}'}
                
        except json.JSONDecodeError as e:
            return {'success': False, 'error': f'JSON inválido: {str(e)}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def crear_index(self, nombre_index: str, mappings: Dict = None, settings: Dict = None) -> bool:
        """
        Crea un nuevo índice
        
        Args:
            nombre_index: Nombre del índice
            mappings: Definición de campos (opcional)
            settings: Configuración del índice (opcional)
        """
        try:
            body = {}
            if mappings:
                body['mappings'] = mappings
            if settings:
                body['settings'] = settings
                
            self.client.indices.create(index=nombre_index, body=body)
            return True
        except Exception as e:
            print(f"Error al crear índice: {e}")
            return False
    
    def eliminar_index(self, nombre_index: str) -> bool:
        """Elimina un índice"""
        try:
            self.client.indices.delete(index=nombre_index)
            return True
        except Exception as e:
            print(f"Error al eliminar índice: {e}")
            return False
    
    def listar_indices(self) -> List[Dict]:
        """Lista todos los índices con información detallada"""
        try:
            indices = self.client.cat.indices(format='json', h='index,docs.count,store.size,health,status')
            
            # Convertir a formato más legible
            indices_formateados = []
            for idx in indices:
                indices_formateados.append({
                    'nombre': idx.get('index', ''),
                    'total_documentos': int(idx.get('docs.count', 0)) if idx.get('docs.count', '0').isdigit() else 0,
                    'tamaño': idx.get('store.size', '0b'),
                    'salud': idx.get('health', 'unknown'),
                    'estado': idx.get('status', 'unknown')
                })
            
            return indices_formateados
        except Exception as e:
            print(f"Error al listar índices: {e}")
            return []
    
    def indexar_documento(self, index: str, documento: Dict, doc_id: str = None) -> bool:
        """
        Indexa un documento en ElasticSearch
        
        Args:
            index: Nombre del índice
            documento: Documento a indexar
            doc_id: ID del documento (opcional)
        """
        try:
            if doc_id:
                self.client.index(index=index, id=doc_id, document=documento)
            else:
                self.client.index(index=index, document=documento)
            return True
        except Exception as e:
            print(f"Error al indexar documento: {e}")
            return False
    
    def indexar_bulk(self, index: str, documentos: List[Dict]) -> Dict:
        """
        Indexa múltiples documentos de forma masiva
        
        Args:
            index: Nombre del índice
            documentos: Lista de documentos a indexar
            
        Returns:
            Diccionario con estadísticas de indexación
        """
        from elasticsearch.helpers import bulk
        
        try:
            # Preparar acciones para bulk
            acciones = []
            for doc in documentos:
                accion = {
                    '_index': index,
                    '_source': doc
                }
                acciones.append(accion)
            
            # Ejecutar bulk
            success, failed = bulk(self.client, acciones, raise_on_error=False)
            
            return {
                'success': True,
                'indexados': success,
                'fallidos': len(failed) if failed else 0,
                'errores': failed if failed else []
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def buscar(self, index: str, query: Dict, aggs=None, size: int = 10) -> Dict:
        """
        Realiza una búsqueda en ElasticSearch
        
        Args:
            index: Nombre del índice
            query: Query de búsqueda (puede ser un dict completo con 'query' o solo la query)
            aggs: Agregaciones a ejecutar (opcional)
            size: Número de resultados
        """
        try:
            # Construir el body de la búsqueda
            body = query.copy() if query else {}
            
            # Agregar las agregaciones al body si existen
            if aggs:
                body['aggs'] = aggs
            
            # Ejecutar búsqueda
            response = self.client.search(index=index, body=body, size=size)
            
            return {
                'success': True,
                'total': response['hits']['total']['value'],
                'resultados': response['hits']['hits'],
                'aggs': aggs
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def ejecutar_query(self, query_json: str) -> Dict:
        """
        Ejecuta una query en ElasticSearch
        
        Args:
            query_json: Query en formato JSON string
            
        Returns:
            Resultado de la búsqueda con hits y aggregations
        """
        try:
            import json
            query = json.loads(query_json)
            
            # Si la query tiene 'index' específico, extraerlo
            index = query.pop('index', '_all')
            
            # Ejecutar búsqueda
            response = self.client.search(index=index, body=query)
            
            return {
                'success': True,
                'total': response['hits']['total']['value'],
                'hits': response['hits']['hits'],
                'aggs': response.get('aggregations', {})
            }
        except json.JSONDecodeError as e:
            return {'success': False, 'error': f'JSON inválido: {str(e)}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def ejecutar_dml(self, comando_json: str) -> Dict:
        """
        Ejecuta un comando DML (Data Manipulation Language) en ElasticSearch
        Permite ejecutar comandos como index, update, delete directamente
        
        Args:
            comando_json: Comando DML en formato JSON string
            
        Returns:
            Resultado de la ejecución
        """
        try:
            import json
            comando = json.loads(comando_json)
            
            operacion = comando.get('operacion')
            
            if operacion == 'index' or operacion == 'create':
                # Indexar documento
                index = comando.get('index')
                documento = comando.get('documento', comando.get('body', {}))
                doc_id = comando.get('id')
                
                if doc_id:
                    response = self.client.index(index=index, id=doc_id, document=documento)
                else:
                    response = self.client.index(index=index, document=documento)
                
                return {'success': True, 'data': response}
                
            elif operacion == 'update':
                # Actualizar documento
                index = comando.get('index')
                doc_id = comando.get('id')
                doc = comando.get('doc', comando.get('documento', {}))
                
                response = self.client.update(index=index, id=doc_id, doc=doc)
                return {'success': True, 'data': response}
                
            elif operacion == 'delete':
                # Eliminar documento
                index = comando.get('index')
                doc_id = comando.get('id')
                
                response = self.client.delete(index=index, id=doc_id)
                return {'success': True, 'data': response}
                
            elif operacion == 'delete_by_query':
                # Eliminar por query
                index = comando.get('index')
                query = comando.get('query', {})
                
                response = self.client.delete_by_query(index=index, body={'query': query})
                return {'success': True, 'data': response}
                
            else:
                return {'success': False, 'error': f'Operación DML no soportada: {operacion}'}
                
        except json.JSONDecodeError as e:
            return {'success': False, 'error': f'JSON inválido: {str(e)}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def buscar_texto(self, index: str, texto: str, campos: List[str] = None, size: int = 10) -> Dict:
        """
        Búsqueda simple de texto en campos específicos
        
        Args:
            index: Nombre del índice
            texto: Texto a buscar
            campos: Lista de campos donde buscar (si es None, busca en todos)
            size: Número de resultados
        """
        try:
            if campos:
                query = {
                    "query": {
                        "multi_match": {
                            "query": texto,
                            "fields": campos,
                            "type": "best_fields"
                        }
                    }
                }
            else:
                query = {
                    "query": {
                        "query_string": {
                            "query": texto
                        }
                    }
                }
            
            return self.buscar(index, query, size)
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def obtener_documento(self, index: str, doc_id: str) -> Optional[Dict]:
        """Obtiene un documento por su ID"""
        try:
            response = self.client.get(index=index, id=doc_id)
            return response['_source']
        except Exception as e:
            print(f"Error al obtener documento: {e}")
            return None
    
    def actualizar_documento(self, index: str, doc_id: str, datos: Dict) -> bool:
        """Actualiza un documento existente"""
        try:
            self.client.update(index=index, id=doc_id, doc=datos)
            return True
        except Exception as e:
            print(f"Error al actualizar documento: {e}")
            return False
    
    def eliminar_documento(self, index: str, doc_id: str) -> bool:
        """Elimina un documento"""
        try:
            self.client.delete(index=index, id=doc_id)
            return True
        except Exception as e:
            print(f"Error al eliminar documento: {e}")
            return False
    
    def close(self):
        """Cierra la conexión"""
        self.client.close()
        
# Instancia global que usaremos en app.py
elastic = ElasticSearch(
    cloud_url=ELASTIC_CLOUD_URL,
    api_key=ELASTIC_API_KEY
)

from elasticsearch.helpers import bulk

def crear_indice_anla_si_no_existe(index_name: str = None):
    """
    Crea el índice de resoluciones ANLA con el mapping adecuado
    si aún no existe.
    """
    index_name = index_name or ELASTIC_INDEX_DEFAULT

    if elastic.client.indices.exists(index=index_name):
        return

    body = {
        "mappings": {
            "properties": {
                "fuente":            { "type": "keyword" },
                "numero_resolución": { "type": "text" },
                "fecha_resolución":  { "type": "date", "format": "yyyy-MM-dd" },
                "nombre_proyecto":   { "type": "text" },
                "ubicación":         { "type": "text" },
                "empresa": {
                    "type": "text",
                    "fields": { "keyword": { "type": "keyword" } }
                },
                "numero_expediente": { "type": "keyword" },
                "radicados":         { "type": "keyword" },
                "descripcion":       { "type": "text" },
                "tipos_infraccion":  { "type": "keyword" },
                "pdf_id":            { "type": "keyword" },
                "file_name":         { "type": "keyword" },
                "texto_completo":    { "type": "text" }
            }
        }
    }

    elastic.client.indices.create(index=index_name, body=body)

def indexar_json_anla(json_dir: str, index_name: str = None) -> Dict:
    """
    Lee todos los JSON de una carpeta y los indexa en el índice ANLA.
    """
    index_name = index_name or ELASTIC_INDEX_DEFAULT
    crear_indice_anla_si_no_existe(index_name)

    documentos = []
    for fname in os.listdir(json_dir):
        if not fname.lower().endswith(".json"):
            continue

        path = os.path.join(json_dir, fname)
        with open(path, "r", encoding="utf-8") as f:
            doc = json.load(f)

        # Usamos pdf_id como _id si existe
        doc_id = doc.get("pdf_id", os.path.splitext(fname)[0])

        documentos.append({
            "_index": index_name,
            "_id": doc_id,
            "_source": doc
        })

    if not documentos:
        return {"success": True, "indexados": 0, "fallidos": 0, "errores": []}

    success, errors = bulk(elastic.client, documentos, raise_on_error=False)

    return {
        "success": True,
        "indexados": success,
        "fallidos": len(errors) if errors else 0,
        "errores": errors or []
    }