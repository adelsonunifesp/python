# app_django.py

import sys
import os
import json
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

from django.conf import settings
from django.urls import path
from django.http import JsonResponse, HttpRequest, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
import traceback

# Configuração para que o Django possa ser executado como um script standalone
# É crucial configurar settings ANTES de importar qualquer coisa do Django que precise delas.
if not settings.configured:
    settings.configure(
        DEBUG=True,
        SECRET_KEY='sua-chave-secreta-aqui-para-desenvolvimento', # Altere para uma chave forte em produção
        ROOT_URLCONF=__name__, # Define este arquivo como o módulo de URLs raiz
        # Adicione outros settings necessários, como TEMPLATES, INSTALLED_APPS se usar mais funcionalidades
        # INSTALLED_APPS=[
        #     'django.contrib.auth',
        #     'django.contrib.contenttypes',
        #     # Outros apps que você possa precisar
        # ],
    )

# Adiciona o diretório 'common' ao sys.path para importar o connectorDB e crud
current_dir = Path(__file__).parent
common_dir_src = current_dir / "../../common/src/"
common_dir = current_dir / "../../common/"

if str(common_dir_src) not in sys.path:
    sys.path.insert(0, str(common_dir_src))
if str(common_dir) not in sys.path:
    sys.path.insert(0, str(common_dir))

# Importa as classes do seu módulo refatorado connectorDB e crud
from connectorDB import DBConnectionManager, DatabaseError, ConnectionError, ConfigError, SecurityError
from crud import CRUD # type: ignore

# Configuração de logger para a aplicação Django
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuração do Banco de Dados ---
# Usaremos a configuração do PostgreSQL para a API de exemplo.
# Certifique-se de que o arquivo config_postgres.json exista e esteja correto.
CONFIG_FILE = str(common_dir / "config_postgres.json")
TABLE_NAME = "cliente" # Nome da tabela para as operações CRUD

# Função para obter uma instância de CRUD
def get_crud_instance():
    """
    Retorna uma tupla contendo a instância do conector e a instância CRUD.
    A conexão será gerenciada pelo contexto 'with'.
    """
    try:
        manager = DBConnectionManager(CONFIG_FILE)
        # O conector retornado por manager.get_connector() é um gerenciador de contexto.
        # Ele abrirá a conexão em __enter__ e a fechará em __exit__.
        # Retornamos o conector e o CRUD para que o ponto de chamada possa usar 'with'.
        connector = manager.get_connector()
        return connector, CRUD(connector)
    except (ConnectionError, ConfigError, SecurityError, DatabaseError) as e:
        logger.error(f"Erro ao inicializar o conector do banco de dados: {e}")
        return None, None # Sinaliza falha

@csrf_exempt # Desabilita a proteção CSRF para esta view (apenas para API REST sem forms de navegador)
@require_http_methods(["POST"])
def create_cliente(request: HttpRequest):
    """Cria um novo cliente."""
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Dados JSON inválidos"}, status=400)

    required_fields = ["id_cliente", "nome", "email"]
    if not all(field in data for field in required_fields):
        return JsonResponse({"error": f"Campos obrigatórios ausentes: {', '.join(required_fields)}"}, status=400)

    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
            return JsonResponse({"error": "Falha na conexão com o banco de dados."}, status=500)

        with connector as conn: # Garante que a conexão seja aberta e fechada
            rows_affected = crud.create(TABLE_NAME, data)
            if rows_affected > 0:
                logger.info(f"Cliente {data.get('id_cliente')} criado com sucesso.")
                return JsonResponse({"message": "Cliente criado com sucesso", "rows_affected": rows_affected}, status=201)
            else:
                logger.warning(f"Nenhum cliente criado para os dados: {data}")
                return JsonResponse({"message": "Nenhum cliente criado. Verifique os dados."}, status=400)
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao criar cliente: {e}")
        return JsonResponse({"error": f"Erro no banco de dados: {str(e)}"}, status=500)
    except Exception as e:
        logger.error(f"Erro inesperado ao criar cliente: {e}")
        logger.error(traceback.format_exc()) # Imprime o stack trace completo
        return JsonResponse({"error": f"Erro interno do servidor: {str(e)}"}, status=500)

@csrf_exempt # Desabilita a proteção CSRF
@require_http_methods(["GET"])
def get_clientes(request: HttpRequest):
    """Lê clientes com base em query parameters (condições) ou todos."""
    conditions = dict(request.GET) # Obtém query parameters como dicionário

    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
            return JsonResponse({"error": "Falha na conexão com o banco de dados."}, status=500)

        with connector as conn:
            clientes = crud.read(TABLE_NAME, conditions)
            # JsonResponse do Django lida bem com listas de dicionários ou listas de tuplas.
            return JsonResponse(clientes, safe=False, status=200) # safe=False para permitir serialização de listas
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao ler clientes: {e}")
        return JsonResponse({"error": f"Erro no banco de dados: {str(e)}"}, status=500)
    except Exception as e:
        logger.error(f"Erro inesperado ao ler clientes: {e}")
        logger.error(traceback.format_exc())
        return JsonResponse({"error": f"Erro interno do servidor: {str(e)}"}, status=500)

@csrf_exempt # Desabilita a proteção CSRF
@require_http_methods(["GET"])
def get_cliente_by_id(request: HttpRequest, id_cliente: str):
    """Lê um cliente específico pelo id_cliente."""
    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
            return JsonResponse({"error": "Falha na conexão com o banco de dados."}, status=500)

        with connector as conn:
            cliente = crud.read(TABLE_NAME, {"id_cliente": id_cliente})
            if cliente:
                return JsonResponse(cliente[0], status=200)
            else:
                return JsonResponse({"message": "Cliente não encontrado"}, status=404)
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao ler cliente por ID: {e}")
        return JsonResponse({"error": f"Erro no banco de dados: {str(e)}"}, status=500)
    except Exception as e:
        logger.error(f"Erro inesperado ao ler cliente por ID: {e}")
        logger.error(traceback.format_exc())
        return JsonResponse({"error": f"Erro interno do servidor: {str(e)}"}, status=500)

@csrf_exempt # Desabilita a proteção CSRF
@require_http_methods(["PUT"])
def update_cliente(request: HttpRequest, id_cliente: str):
    """Atualiza um cliente existente pelo id_cliente."""
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Dados JSON inválidos"}, status=400)

    if not data:
        return JsonResponse({"error": "Nenhum dado para atualização fornecido."}, status=400)

    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
            return JsonResponse({"error": "Falha na conexão com o banco de dados."}, status=500)

        with connector as conn:
            conditions = {"id_cliente": id_cliente}
            rows_affected = crud.update(TABLE_NAME, data, conditions)
            if rows_affected > 0:
                logger.info(f"Cliente {id_cliente} atualizado com sucesso.")
                return JsonResponse({"message": "Cliente atualizado com sucesso", "rows_affected": rows_affected}, status=200)
            else:
                logger.warning(f"Nenhum cliente encontrado ou atualizado para ID: {id_cliente}")
                return JsonResponse({"message": "Cliente não encontrado ou nenhum dado para atualização"}, status=404)
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao atualizar cliente: {e}")
        return JsonResponse({"error": f"Erro no banco de dados: {str(e)}"}, status=500)
    except ValueError as e: # Captura a validação de condições vazias do CRUD
        logger.error(f"Erro de validação na atualização: {e}")
        return JsonResponse({"error": str(e)}, status=400)
    except Exception as e:
        logger.error(f"Erro inesperado ao atualizar cliente: {e}")
        logger.error(traceback.format_exc())
        return JsonResponse({"error": f"Erro interno do servidor: {str(e)}"}, status=500)

@csrf_exempt # Desabilita a proteção CSRF
@require_http_methods(["DELETE"])
def delete_cliente(request: HttpRequest, id_cliente: str):
    """Deleta um cliente existente pelo id_cliente."""
    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
            return JsonResponse({"error": "Falha na conexão com o banco de dados."}, status=500)

        with connector as conn:
            conditions = {"id_cliente": id_cliente}
            rows_affected = crud.delete(TABLE_NAME, conditions)
            if rows_affected > 0:
                logger.info(f"Cliente {id_cliente} deletado com sucesso.")
                return JsonResponse({"message": "Cliente deletado com sucesso", "rows_affected": rows_affected}, status=200)
            else:
                logger.warning(f"Nenhum cliente encontrado para deleção com ID: {id_cliente}")
                return JsonResponse({"message": "Cliente não encontrado"}, status=404)
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao deletar cliente: {e}")
        return JsonResponse({"error": f"Erro no banco de dados: {str(e)}"}, status=500)
    except ValueError as e: # Captura a validação de condições vazias do CRUD
        logger.error(f"Erro de validação na deleção: {e}")
        return JsonResponse({"error": str(e)}, status=400)
    except Exception as e:
        logger.error(f"Erro inesperado ao deletar cliente: {e}")
        logger.error(traceback.format_exc())
        return JsonResponse({"error": f"Erro interno do servidor: {str(e)}"}, status=500)

# Definir as URL patterns para o aplicativo Django
urlpatterns = [
    path('clientes', get_clientes),
    path('clientes/<str:id_cliente>', get_cliente_by_id),
    path('clientes', create_cliente), # POST para /clientes
    path('clientes/<str:id_cliente>', update_cliente), # PUT para /clientes/{id}
    path('clientes/<str:id_cliente>', delete_cliente), # DELETE para /clientes/{id}
]

# Configuração inicial para criar o arquivo config_postgres.json se não existir
def setup_config_file():
    """Cria o diretório common e um arquivo de configuração de exemplo se não existirem."""
    Path(common_dir).mkdir(parents=True, exist_ok=True)
    
    if not Path(CONFIG_FILE).exists():
        logger.warning(f"Arquivo de configuração '{CONFIG_FILE}' não encontrado. Criando um exemplo.")
        example_config = {
            "db_type": "postgresql",
            "host": "localhost", # Altere para o seu host do DB
            "database": "s4laldeveloper", # Altere para o seu DB
            "user": "postgres", # Altere para seu usuário
            "password": "ENC:SUA_SENHA_CRIPTOGRAFADA_AQUI", # SUBSTITUA PELA SUA SENHA CRIPTOGRAFADA REAL
            "port": 5432,
            "schema": "public"
        }
        with open(CONFIG_FILE, "w", encoding='utf-8') as f:
            json.dump(example_config, f, indent=2)
        print(f"Arquivo '{CONFIG_FILE}' criado com um exemplo. Por favor, edite-o com suas credenciais reais e, se necessário, criptografe a senha.")

if __name__ == '__main__':
    setup_config_file() # Garante que o arquivo de configuração exista
    
    from django.core.wsgi import get_wsgi_application
    from werkzeug.serving import run_simple

    # Obtenha a aplicação WSGI do Django
    application = get_wsgi_application()

    print("Para rodar com Werkzeug (apenas para desenvolvimento):")
    print("Acesse: http://localhost:8000/clientes")
    
    # Execute a aplicação WSGI usando run_simple do Werkzeug
    # Isso é apenas para fins de desenvolvimento e demonstração.
    # Em produção, você usaria um servidor WSGI como Gunicorn, uWSGI, Nginx + uWSGI, etc.
    run_simple('localhost', 8000, application, use_reloader=True, use_debugger=True)
