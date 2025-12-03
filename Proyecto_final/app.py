from flask import Flask, render_template, request, redirect, url_for, jsonify, session, flash
from dotenv import load_dotenv
import os
from datetime import datetime
from werkzeug.utils import secure_filename
from Helpers import MongoDB, ElasticSearch, Funciones, WebScraping
import re
import unicodedata

# Cargar variables de entorno
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'cambiar_clave')

# ==================== CONFIGURACIÓN MONGO ====================

MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB')
MONGO_COLECCION = os.getenv('MONGO_COLECCION', 'usuario_roles')

# ==================== CONFIGURACIÓN ELASTIC ====================
# Ahora soportamos:
# - Elastic Cloud (ELASTIC_CLOUD_URL / ELASTIC_API_KEY, y opcionalmente ELASTIC_CLOUD_ID)
# - Elastic local (ELASTIC_HOST / ELASTIC_USERNAME / ELASTIC_PASSWORD)
# La clase Helpers.ElasticSearch ya está preparada para usar estas variables
# a través del helper get_es_client() que modificamos en elastic.py

# Elastic Cloud
ELASTIC_CLOUD_URL     = os.getenv('ELASTIC_CLOUD_URL')      # endpoint https://...
ELASTIC_API_KEY       = os.getenv('ELASTIC_API_KEY')
ELASTIC_CLOUD_ID      = os.getenv('ELASTIC_CLOUD_ID')       # opcional (si lo usas)

# Elastic local
ELASTIC_HOST          = os.getenv('ELASTIC_HOST')           # p.ej. https://localhost:9200
ELASTIC_USERNAME      = os.getenv('ELASTIC_USERNAME')       # p.ej. elastic
ELASTIC_PASSWORD      = os.getenv('ELASTIC_PASSWORD')       # tu password local

# Índice por defecto para resoluciones ANLA
ELASTIC_INDEX_DEFAULT = os.getenv('ELASTIC_INDEX_DEFAULT', 'anla_resoluciones')

# Versión de la aplicación
VERSION_APP = "1.3.0"
CREATOR_APP = "Oswaldo Salgado Gómez"

# Categorías de tipos de infracción (deben coincidir con clasificar_infracciones)
TIPOS_INFRACCION_CATEGORIAS = [
    "Vertimientos",
    "Emisiones atmosféricas",
    "Residuos sólidos",
    "Incumplimiento de norma",
    "Manejo de aguas",
    "Tala o afectación de bosques",
    "Incumplimiento del PMA",
    "Cierre de pozos",
    "No presentar informes"
]

# ==================== INICIALIZAR CONEXIONES ====================

mongo = MongoDB(MONGO_URI, MONGO_DB)

# IMPORTANTE:
# La clase ElasticSearch que ajustamos en elastic.py tiene esta lógica:
# - Si recibe cloud_url y api_key → conecta a Elastic Cloud con esa URL.
# - Si NO recibe parámetros (o son None) → usa get_es_client(), que a su vez
#   intenta ELASTIC_CLOUD_ID/ELASTIC_API_KEY o ELASTIC_HOST/ELASTIC_USERNAME/ELASTIC_PASSWORD.
#
# Por eso este constructor funciona tanto en local como en Render:
elastic = ElasticSearch(ELASTIC_CLOUD_URL, ELASTIC_API_KEY)

# ==================== RUTAS PÚBLICAS ====================

@app.route('/')
def landing():
    """Landing page pública"""
    return render_template('landing.html', version=VERSION_APP, creador=CREATOR_APP)

@app.route('/about')
def about():
    """Página About"""
    return render_template('about.html', version=VERSION_APP, creador=CREATOR_APP)

# ==================== FUNCIÓN NORMALIZAR PROFUNDO ====================

def normalizar_profundo(texto: str) -> str:
    """Normaliza texto para comparar empresas (mayúsculas, sin acentos, sin signos)."""
    if not texto:
        return ""
    t = texto.upper()
    t = ''.join(c for c in unicodedata.normalize('NFD', t) if unicodedata.category(c) != 'Mn')
    t = re.sub(r"[^A-Z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

# ==================== BUSCADOR ELASTIC (PÚBLICO) ====================

@app.route('/buscador') 
def buscador():
    """
    Página de búsqueda pública con filtros:
    - texto libre
    - empresa (select)
    - año de resolución
    - número de resolución
    - número de expediente
    - tipo de infracción (select)
    """

    # ---- Parámetros de la URL (GET) ----
    texto = (request.args.get('texto') or '').strip()
    empresa = (request.args.get('empresa') or '').strip()
    anio = (request.args.get('anio') or '').strip()
    num_resolucion = (request.args.get('num_resolucion') or '').strip()
    num_expediente = (request.args.get('num_expediente') or '').strip()
    tipo_infraccion = (request.args.get('tipo_infraccion') or '').strip()

    resultados = []
    total = 0
    error = None
    filtros_activos = {}

    # listas para los <select>
    empresas_opciones = []
    tipos_infraccion_opciones = []

    try:
        must_clauses = []

        # ---- TEXTO LIBRE ----
        if texto:
            filtros_activos["Texto"] = texto
            must_clauses.append({
                "multi_match": {
                    "query": texto,
                    "fields": [
                        "texto_completo",
                        "numero_resolución^3",
                        "descripcion^2",
                        "nombre_proyecto^2",
                        "empresa^2",
                        # si quieres sigues buscando también en empresa_normalizada:
                        # "empresa_normalizada^3",
                        "numero_expediente",
                        "radicados"
                    ]
                }
            })

        # ---- EMPRESA (select) - usamos empresa.keyword ----
        if empresa:
            filtros_activos["Empresa"] = empresa
            must_clauses.append({
                "term": {
                    "empresa.keyword": empresa   # 👈 mismo campo que el combo
                }
            })

        # ---- AÑO RESOLUCIÓN ----
        if anio:
            filtros_activos["Año"] = anio
            try:
                anio_int = int(anio)
                must_clauses.append({
                    "range": {
                        "fecha_resolución": {
                            "gte": f"{anio_int:04d}-01-01",
                            "lte": f"{anio_int:04d}-12-31"
                        }
                    }
                })
            except ValueError:
                pass

        # ---- NÚMERO DE RESOLUCIÓN (fragmento) ----
        if num_resolucion:
            filtros_activos["N° resolución"] = num_resolucion
            must_clauses.append({
                "wildcard": {
                    "numero_resolución": f"*{num_resolucion}*"
                }
            })

        # ---- NÚMERO DE EXPEDIENTE (fragmento) ----
        if num_expediente:
            filtros_activos["N° expediente"] = num_expediente
            must_clauses.append({
                "wildcard": {
                    "numero_expediente": f"*{num_expediente}*"
                }
            })

        # ---- TIPO DE INFRACCIÓN (select) ----
        if tipo_infraccion:
            filtros_activos["Tipo de infracción"] = tipo_infraccion
            must_clauses.append({
                "term": {
                    "tipos_infraccion": tipo_infraccion
                }
            })

        # ---- QUERY PRINCIPAL ----
        if not must_clauses:
            query_body = {"query": {"match_all": {}}}
        else:
            query_body = {"query": {"bool": {"must": must_clauses}}}

        # ---- AGREGACIONES PARA LLENAR LOS SELECTS ----
        aggs = {
            "empresas": {
                "terms": {
                    "field": "empresa.keyword",   # 👈 igual que el filtro
                    "size": 200
                }
            },
            "tipos_infraccion": {
                "terms": {
                    "field": "tipos_infraccion",
                    "size": 200
                }
            }
        }

        # Llamada a Elasticsearch mediante tu helper
        resultado = elastic.buscar(
            index=ELASTIC_INDEX_DEFAULT,
            query=query_body,
            aggs=aggs,
            size=100
        )

        if resultado.get('success'):
            resultados = resultado.get('resultados', [])
            total = resultado.get('total', 0)

            aggs_result = resultado.get('aggs', {}) or {}

            empresas_opciones = [
                b["key"] for b in aggs_result.get("empresas", {}).get("buckets", [])
            ]
            tipos_infraccion_opciones = [
                b["key"] for b in aggs_result.get("tipos_infraccion", {}).get("buckets", [])
            ]

            empresas_opciones = sorted(empresas_opciones)
            tipos_infraccion_opciones = sorted(tipos_infraccion_opciones)

            if not tipos_infraccion_opciones and TIPOS_INFRACCION_CATEGORIAS:
                tipos_infraccion_opciones = TIPOS_INFRACCION_CATEGORIAS[:]

        else:
            error = resultado.get('error', 'Error desconocido en Elastic')
            tipos_infraccion_opciones = TIPOS_INFRACCION_CATEGORIAS[:]

    except Exception as e:
        error = str(e)
        tipos_infraccion_opciones = TIPOS_INFRACCION_CATEGORIAS[:]

    # ---- DEBUG EN CONSOLA ----
    print("=== DEBUG BUSCADOR ===")
    print("Empresas AGG:", empresas_opciones)
    print("Infracciones AGG:", tipos_infraccion_opciones)
    print("Total resultados:", total)
    print("======================")

    return render_template(
        'buscador.html',
        version=VERSION_APP,
        creador=CREATOR_APP,
        texto=texto,
        empresa=empresa,
        anio=anio,
        num_resolucion=num_resolucion,
        num_expediente=num_expediente,
        tipo_infraccion=tipo_infraccion,
        empresas_opciones=empresas_opciones,
        tipos_infraccion_opciones=tipos_infraccion_opciones,
        resultados=resultados,
        total=total,
        error=error,
        filtros_activos=filtros_activos
    )
# ==================== AUTENTICACIÓN / USUARIOS (MONGO) ====================

@app.route('/login', methods=['GET', 'POST'])
def login():
    error_message = None  # Ahora sí lo usaremos correctamente

    if request.method == 'POST':
        usuario = request.form.get('usuario')
        password = request.form.get('password')

        # Validación en MongoDB
        user_data = mongo.validar_usuario(usuario, password, MONGO_COLECCION)

        if user_data:
            session['usuario'] = usuario
            session['permisos'] = user_data.get('permisos', {})
            session['logged_in'] = True

            # Si el usuario es admin_usuarios → va al gestor
            if user_data.get('permisos', {}).get('admin_usuarios', False):
                return redirect(url_for('gestor_usuarios'))

            # Si no es admin_usuarios → va al buscador
            return redirect(url_for('buscador'))

        else:
            error_message = "Usuario o contraseña incorrectos."

    return render_template('login.html', error_message=error_message)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/listar-usuarios')
def listar_usuarios():
    if not session.get('logged_in'):
        return jsonify({'error': 'No autorizado'}), 401

    permisos = session.get('permisos', {})
    if not permisos.get('admin_usuarios'):
        return jsonify({'error': 'No tiene permisos para listar usuarios'}), 403

    try:
        usuarios = mongo.listar_usuarios(MONGO_COLECCION)
        for usuario in usuarios:
            usuario['_id'] = str(usuario['_id'])
        return jsonify(usuarios)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/gestor_usuarios')
def gestor_usuarios():
    """Página de gestión de usuarios (protegida requiere login y permiso admin_usuarios)"""
    if not session.get('logged_in'):
        flash('Por favor, inicia sesión para acceder a esta página', 'warning')
        return redirect(url_for('login'))
    
    permisos = session.get('permisos', {})
    if not permisos.get('admin_usuarios'):
        flash('No tiene permisos para gestionar usuarios', 'danger')
        return redirect(url_for('admin'))
    
    return render_template(
        'gestor_usuarios.html',
        usuario=session.get('usuario'),
        permisos=permisos,
        version=VERSION_APP,
        creador=CREATOR_APP
    )

@app.route('/crear-usuario', methods=['POST'])
def crear_usuario():
    """API para crear un nuevo usuario"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_usuarios'):
            return jsonify({'success': False, 'error': 'No tiene permisos para crear usuarios'}), 403
        
        data = request.get_json() or {}
        usuario = data.get('usuario')
        password = data.get('password')
        permisos_usuario = data.get('permisos', {})
        
        if not usuario or not password:
            return jsonify({'success': False, 'error': 'Usuario y password son requeridos'}), 400
        
        # Verificar si el usuario ya existe
        usuario_existente = mongo.obtener_usuario(usuario, MONGO_COLECCION)
        if usuario_existente:
            return jsonify({'success': False, 'error': 'El usuario ya existe'}), 400
        
        # Crear usuario
        resultado = mongo.crear_usuario(usuario, password, permisos_usuario, MONGO_COLECCION)
        
        if resultado:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Error al crear usuario'}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/actualizar-usuario', methods=['POST'])
def actualizar_usuario():
    """API para actualizar un usuario existente"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_usuarios'):
            return jsonify({'success': False, 'error': 'No tiene permisos para actualizar usuarios'}), 403
        
        data = request.get_json() or {}
        usuario_original = data.get('usuario_original')
        datos_usuario = data.get('datos', {})
        
        if not usuario_original:
            return jsonify({'success': False, 'error': 'Usuario original es requerido'}), 400
        
        # Verificar si el usuario existe
        usuario_existente = mongo.obtener_usuario(usuario_original, MONGO_COLECCION)
        if not usuario_existente:
            return jsonify({'success': False, 'error': 'Usuario no encontrado'}), 404
        
        # Si el nombre de usuario cambió, verificar que no exista otro con ese nombre
        nuevo_usuario = datos_usuario.get('usuario')
        if nuevo_usuario and nuevo_usuario != usuario_original:
            usuario_duplicado = mongo.obtener_usuario(nuevo_usuario, MONGO_COLECCION)
            if usuario_duplicado:
                return jsonify({'success': False, 'error': 'Ya existe otro usuario con ese nombre'}), 400
        
        # Actualizar usuario
        resultado = mongo.actualizar_usuario(usuario_original, datos_usuario, MONGO_COLECCION)
        
        if resultado:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Error al actualizar usuario'}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/eliminar-usuario', methods=['POST'])
def eliminar_usuario():
    """API para eliminar un usuario"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_usuarios'):
            return jsonify({'success': False, 'error': 'No tiene permisos para eliminar usuarios'}), 403
        
        data = request.get_json() or {}
        usuario = data.get('usuario')
        
        if not usuario:
            return jsonify({'success': False, 'error': 'Usuario es requerido'}), 400
        
        # Verificar si el usuario existe
        usuario_existente = mongo.obtener_usuario(usuario, MONGO_COLECCION)
        if not usuario_existente:
            return jsonify({'success': False, 'error': 'Usuario no encontrado'}), 404
        
        # No permitir eliminar al usuario actual
        if usuario == session.get('usuario'):
            return jsonify({'success': False, 'error': 'No puede eliminarse a sí mismo'}), 400
        
        # Eliminar usuario
        resultado = mongo.eliminar_usuario(usuario, MONGO_COLECCION)
        
        if resultado:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Error al eliminar usuario'}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ==================== GESTIÓN ELASTIC (ADMIN ELASTIC) ====================

@app.route('/gestor_elastic')
def gestor_elastic():
    """Página de gestión de ElasticSearch (protegida requiere login y permiso admin_elastic)"""
    if not session.get('logged_in'):
        flash('Por favor, inicia sesión para acceder a esta página', 'warning')
        return redirect(url_for('login'))
    
    permisos = session.get('permisos', {})
    if not permisos.get('admin_elastic'):
        flash('No tiene permisos para gestionar ElasticSearch', 'danger')
        return redirect(url_for('admin'))
    
    return render_template(
        'gestor_elastic.html',
        usuario=session.get('usuario'),
        permisos=permisos,
        version=VERSION_APP,
        creador=CREATOR_APP
    )

@app.route('/listar-indices-elastic')
def listar_indices_elastic():
    """API para listar índices de ElasticSearch"""
    try:
        if not session.get('logged_in'):
            return jsonify({'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_elastic'):
            return jsonify({'error': 'No tiene permisos para gestionar ElasticSearch'}), 403
        
        indices = elastic.listar_indices()
        # Se asume que devuelve lista de dicts con:
        # nombre, total_documentos, tamaño, salud, estado
        return jsonify(indices)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/ejecutar-query-elastic', methods=['POST'])
def ejecutar_query_elastic():
    """API para ejecutar una query en ElasticSearch"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_elastic'):
            return jsonify({'success': False, 'error': 'No tiene permisos para gestionar ElasticSearch'}), 403
        
        data = request.get_json() or {}
        query_json = data.get('query')
        
        if not query_json:
            return jsonify({'success': False, 'error': 'Query es requerida'}), 400
        
        resultado = elastic.ejecutar_query(query_json)
        # Se asume que resultado ya tiene success, hits, aggs, total, etc.
        return jsonify(resultado)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/ejecutar-dml-elastic', methods=['POST'])
def ejecutar_dml_elastic():
    """
    API para ejecutar un comando DML genérico en ElasticSearch.
    Por ahora, reutiliza elastic.ejecutar_query(comando_json) y devuelve el resultado en 'data'.
    """
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401

        permisos = session.get('permisos', {})
        if not permisos.get('admin_elastic'):
            return jsonify({'success': False, 'error': 'No tiene permisos para gestionar ElasticSearch'}), 403

        data = request.get_json() or {}
        comando_json = data.get('comando')

        if not comando_json:
            return jsonify({'success': False, 'error': 'Comando es requerido'}), 400

        # Aquí podrías implementar lógica específica para DML (index, update, delete, etc.).
        # Por ahora, se delega al helper como si fuera una operación genérica.
        resultado = elastic.ejecutar_query(comando_json)

        return jsonify({
            'success': True,
            'data': resultado
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ==================== CARGA DE DOCUMENTOS A ELASTIC (ADMIN DATA) ====================

@app.route('/cargar_doc_elastic')
def cargar_doc_elastic():
    """Página de carga de documentos a ElasticSearch (protegida requiere login y permiso admin_data_elastic)"""
    if not session.get('logged_in'):
        flash('Por favor, inicia sesión para acceder a esta página', 'warning')
        return redirect(url_for('login'))
    
    permisos = session.get('permisos', {})
    if not permisos.get('admin_data_elastic'):
        flash('No tiene permisos para cargar datos a ElasticSearch', 'danger')
        return redirect(url_for('admin'))
    
    return render_template(
        'documentos_elastic.html',
        usuario=session.get('usuario'),
        permisos=permisos,
        version=VERSION_APP,
        creador=CREATOR_APP
    )

@app.route('/procesar-webscraping-elastic', methods=['POST'])
def procesar_webscraping_elastic():
    """API para procesar Web Scraping y preparar archivos para carga a Elastic"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_data_elastic'):
            return jsonify({'success': False, 'error': 'No tiene permisos para cargar datos'}), 403
        
        data = request.get_json() or {}
        url = data.get('url')
        extensiones_navegar = data.get('extensiones_navegar', 'aspx')
        tipos_archivos = data.get('tipos_archivos', 'pdf')
        index = data.get('index')
        
        if not url or not index:
            return jsonify({'success': False, 'error': 'URL e índice son requeridos'}), 400
        
        # Procesar listas de extensiones
        lista_ext_navegar = [ext.strip() for ext in extensiones_navegar.split(',')]
        lista_tipos_archivos = [ext.strip() for ext in tipos_archivos.split(',')]
        
        # Combinar ambas listas para extraer todos los enlaces
        todas_extensiones = lista_ext_navegar + lista_tipos_archivos
        
        # Inicializar WebScraping
        scraper = WebScraping(dominio_base=url.rsplit('/', 1)[0] + '/')
        
        # Limpiar carpeta de uploads
        carpeta_upload = 'static/uploads'
        Funciones.crear_carpeta(carpeta_upload)
        Funciones.borrar_contenido_carpeta(carpeta_upload)
        
        # Extraer todos los enlaces
        json_path = os.path.join(carpeta_upload, 'links.json')
        resultado = scraper.extraer_todos_los_links(
            url_inicial=url,
            json_file_path=json_path,
            listado_extensiones=todas_extensiones,
            max_iteraciones=50
        )
        
        if not resultado['success']:
            return jsonify({'success': False, 'error': 'Error al extraer enlaces'}), 500
        
        # Descargar archivos PDF (o los tipos especificados)
        resultado_descarga = scraper.descargar_pdfs(json_path, carpeta_upload)
        
        scraper.close()
        
        # Listar archivos descargados
        archivos = Funciones.listar_archivos_carpeta(carpeta_upload, lista_tipos_archivos)
        
        return jsonify({
            'success': True,
            'archivos': archivos,
            'mensaje': f'Se descargaron {len(archivos)} archivos',
            'stats': {
                'total_enlaces': resultado.get('total_links', 0),
                'descargados': resultado_descarga.get('descargados', 0),
                'errores': resultado_descarga.get('errores', 0)
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/procesar-zip-elastic', methods=['POST'])
def procesar_zip_elastic():
    """API para procesar archivo ZIP con archivos JSON y listarlos para carga"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_data_elastic'):
            return jsonify({'success': False, 'error': 'No tiene permisos para cargar datos'}), 403
        
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No se envió ningún archivo'}), 400
        
        file = request.files['file']
        index = request.form.get('index')
        
        if not file.filename:
            return jsonify({'success': False, 'error': 'Archivo no válido'}), 400
        
        if not index:
            return jsonify({'success': False, 'error': 'Índice no especificado'}), 400
        
        # Guardar archivo ZIP temporalmente
        filename = secure_filename(file.filename)
        carpeta_upload = 'static/uploads'
        Funciones.crear_carpeta(carpeta_upload)
        Funciones.borrar_contenido_carpeta(carpeta_upload)
        
        zip_path = os.path.join(carpeta_upload, filename)
        file.save(zip_path)
        print(f"Archivo ZIP guardado en: {zip_path}")
        
        # Descomprimir ZIP
        Funciones.descomprimir_zip_local(zip_path, carpeta_upload)
        
        # Eliminar archivo ZIP
        os.remove(zip_path)
        
        # Listar archivos JSON
        archivos_json = Funciones.listar_archivos_json(carpeta_upload)
        
        return jsonify({
            'success': True,
            'archivos': archivos_json,
            'mensaje': f'Se encontraron {len(archivos_json)} archivos JSON'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
@app.route('/cargar-documentos-elastic', methods=['POST'])
def cargar_documentos_elastic():
    """API para cargar documentos a ElasticSearch desde archivos procesados"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_data_elastic'):
            return jsonify({'success': False, 'error': 'No tiene permisos para cargar datos'}), 403
        
        data = request.get_json() or {}
        archivos = data.get('archivos', [])
        index = data.get('index')
        metodo = data.get('metodo', 'zip')
        
        if not archivos or not index:
            return jsonify({'success': False, 'error': 'Archivos e índice son requeridos'}), 400
        
        documentos = []
        
        if metodo == 'zip':
            # Cargar archivos JSON directamente
            for archivo in archivos:
                ruta = archivo.get('ruta')
                print(f"Procesando archivo JSON: {ruta}")
                if ruta and os.path.exists(ruta):
                    doc = Funciones.leer_json(ruta)
                    if doc:
                        documentos.append(doc)
        
        elif metodo == 'webscraping':
            # Procesar archivos con PLN (aquí está simulado)
            for archivo in archivos:
                ruta = archivo.get('ruta')
                if not ruta or not os.path.exists(ruta):
                    continue
                
                extension = archivo.get('extension', '').lower()
                
                # Extraer texto según tipo de archivo
                texto = ""
                if extension == 'pdf':
                    # Intentar extracción normal
                    texto = Funciones.extraer_texto_pdf(ruta)
                    
                    # Si no se extrajo texto, intentar con OCR
                    if not texto or len(texto.strip()) < 100:
                        try:
                            texto = Funciones.extraer_texto_pdf_ocr(ruta)
                        except:
                            pass
                
                elif extension == 'txt':
                    try:
                        with open(ruta, 'r', encoding='utf-8') as f:
                            texto = f.read()
                    except:
                        try:
                            with open(ruta, 'r', encoding='latin-1') as f:
                                texto = f.read()
                        except:
                            pass
                
                if not texto or len(texto.strip()) < 50:
                    continue
                
                # Procesar con PLN (simulado, sin modelos reales)
                try:
                    resumen = ""
                    entidades = ""
                    temas = []  # lista de (palabra, relevancia)
                    
                    # Crear documento
                    documento = {
                        'texto': texto,
                        'fecha': datetime.now().isoformat(),
                        'ruta': ruta,
                        'nombre_archivo': archivo.get('nombre', ''),
                        'resumen': resumen,
                        'entidades': entidades,
                        'temas': [{'palabra': palabra, 'relevancia': relevancia} for palabra, relevancia in temas]
                    }
                    
                    documentos.append(documento)
                
                except Exception as e:
                    print(f"Error al procesar {archivo.get('nombre')}: {e}")
                    continue
        
        if not documentos:
            return jsonify({'success': False, 'error': 'No se pudieron procesar documentos'}), 400
        
        # Indexar documentos en Elastic
        resultado = elastic.indexar_bulk(index, documentos)
        
        return jsonify({
            'success': resultado.get('success', True),
            'indexados': resultado.get('indexados', 0),
            'errores': resultado.get('fallidos', 0)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ==================== ADMIN (PANEL PRINCIPAL) ====================

@app.route('/admin')
def admin():
    """Página de administración (protegida requiere login)"""
    if not session.get('logged_in'):
        flash('Por favor, inicia sesión para acceder al área de administración', 'warning')
        return redirect(url_for('login'))
    
    return render_template(
        'admin.html',
        usuario=session.get('usuario'),
        permisos=session.get('permisos'),
        version=VERSION_APP,
        creador=CREATOR_APP
    )

# ==================== MAIN ====================

if __name__ == '__main__':
    # Crear carpetas necesarias
    Funciones.crear_carpeta('static/uploads')
    
    # Verificar conexiones
    print("\n" + "="*50)
    print("VERIFICANDO CONEXIONES")

    if mongo.test_connection():
        print("✅ MongoDB Atlas: Conectado")
    else:
        print("❌ MongoDB Atlas: Error de conexión")
    
    if elastic.test_connection():
        print("✅ ElasticSearch: Conectado")
    else:
        print("❌ ElasticSearch: Error de conexión")

    # Ejecutar la aplicación (localmente para pruebas)
    app.run(debug=True, host='0.0.0.0', port=5000)