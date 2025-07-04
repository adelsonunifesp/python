# app.py

import sys
import os
import json
from pathlib import Path
from flask import Flask, request, jsonify
import logging

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
from crud import CRUD

app = Flask(__name__)

# Configuração de logger para a aplicação Flask
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuração do Banco de Dados ---
# Usaremos a configuração do PostgreSQL para a API de exemplo.
# Certifique-se de que o arquivo config_postgres.json exista e esteja correto.
CONFIG_FILE = str(common_dir / "config_postgres.json")
TABLE_NAME = "cliente" # Nome da tabela para as operações CRUD

# Função para obter uma instância de CRUD (com nova conexão por requisição ou pool, se aplicável)
# Neste exemplo simples, criaremos uma nova conexão para cada requisição para demonstrar o 'with'.
# Em uma aplicação de produção, você pode considerar um pool de conexões.
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

@app.route('/clientes', methods=['POST'])
def create_cliente():
    """Cria um novo cliente."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "Dados JSON não fornecidos"}), 400

    required_fields = ["id_cliente", "nome", "email"]
    if not all(field in data for field in required_fields):
        return jsonify({"error": f"Campos obrigatórios ausentes: {', '.join(required_fields)}"}), 400

    try:
        # Usamos o gerenciador de contexto do conector
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
             return jsonify({"error": "Falha na conexão com o banco de dados."}), 500

        with connector as conn: # Garante que a conexão seja aberta e fechada
            rows_affected = crud.create(TABLE_NAME, data)
            if rows_affected > 0:
                logger.info(f"Cliente {data.get('id_cliente')} criado com sucesso.")
                return jsonify({"message": "Cliente criado com sucesso", "rows_affected": rows_affected}), 201
            else:
                logger.warning(f"Nenhum cliente criado para os dados: {data}")
                return jsonify({"message": "Nenhum cliente criado. Verifique os dados."}), 400
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao criar cliente: {e}")
        return jsonify({"error": f"Erro no banco de dados: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"Erro inesperado ao criar cliente: {e}")
        return jsonify({"error": f"Erro interno do servidor: {str(e)}"}), 500

@app.route('/clientes', methods=['GET'])
def get_clientes():
    """Lê clientes com base em query parameters (condições) ou todos."""
    conditions = request.args.to_dict() # Obtém query parameters como dicionário

    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
             return jsonify({"error": "Falha na conexão com o banco de dados."}), 500

        with connector as conn:
            clientes = crud.read(TABLE_NAME, conditions)
            # Se o cursor do MySQL ou PostgreSQL retorna dicionários (como configurado),
            # então jsonify pode serializá-los diretamente.
            # Se retornar tuplas (Firebird, ou se o cursor do MySQL/Postgres não for dictionary/DictCursor),
            # talvez seja necessário converter para lista de dicionários.
            # No nosso setup, MySQL e Postgres já retornam dicts. Firebird retorna tuples.
            # Se for Firebird e você precisar de dicionários, o conn.execute_query precisaria mapear.
            return jsonify(clientes), 200
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao ler clientes: {e}")
        return jsonify({"error": f"Erro no banco de dados: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"Erro inesperado ao ler clientes: {e}")
        return jsonify({"error": f"Erro interno do servidor: {str(e)}"}), 500

@app.route('/clientes/<string:id_cliente>', methods=['GET'])
def get_cliente_by_id(id_cliente):
    """Lê um cliente específico pelo id_cliente."""
    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
             return jsonify({"error": "Falha na conexão com o banco de dados."}), 500

        with connector as conn:
            # Assumimos que id_cliente é a chave primária ou um campo único para busca
            cliente = crud.read(TABLE_NAME, {"id_cliente": id_cliente})
            if cliente:
                return jsonify(cliente[0]), 200 # Retorna o primeiro (e esperado único) resultado
            else:
                return jsonify({"message": "Cliente não encontrado"}), 404
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao ler cliente por ID: {e}")
        return jsonify({"error": f"Erro no banco de dados: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"Erro inesperado ao ler cliente por ID: {e}")
        return jsonify({"error": f"Erro interno do servidor: {str(e)}"}), 500


@app.route('/clientes/<string:id_cliente>', methods=['PUT'])
def update_cliente(id_cliente):
    """Atualiza um cliente existente pelo id_cliente."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "Dados JSON não fornecidos"}), 400

    if not data.keys():
        return jsonify({"error": "Nenhum dado para atualização fornecido."}), 400

    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
             return jsonify({"error": "Falha na conexão com o banco de dados."}), 500

        with connector as conn:
            conditions = {"id_cliente": id_cliente}
            rows_affected = crud.update(TABLE_NAME, data, conditions)
            if rows_affected > 0:
                logger.info(f"Cliente {id_cliente} atualizado com sucesso.")
                return jsonify({"message": "Cliente atualizado com sucesso", "rows_affected": rows_affected}), 200
            else:
                logger.warning(f"Nenhum cliente encontrado ou atualizado para ID: {id_cliente}")
                return jsonify({"message": "Cliente não encontrado ou nenhum dado para atualização"}), 404
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao atualizar cliente: {e}")
        return jsonify({"error": f"Erro no banco de dados: {str(e)}"}), 500
    except ValueError as e: # Captura a validação de condições vazias do CRUD
        logger.error(f"Erro de validação na atualização: {e}")
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Erro inesperado ao atualizar cliente: {e}")
        return jsonify({"error": f"Erro interno do servidor: {str(e)}"}), 500

@app.route('/clientes/<string:id_cliente>', methods=['DELETE'])
def delete_cliente(id_cliente):
    """Deleta um cliente existente pelo id_cliente."""
    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
             return jsonify({"error": "Falha na conexão com o banco de dados."}), 500

        with connector as conn:
            conditions = {"id_cliente": id_cliente}
            rows_affected = crud.delete(TABLE_NAME, conditions)
            if rows_affected > 0:
                logger.info(f"Cliente {id_cliente} deletado com sucesso.")
                return jsonify({"message": "Cliente deletado com sucesso", "rows_affected": rows_affected}), 200
            else:
                logger.warning(f"Nenhum cliente encontrado para deleção com ID: {id_cliente}")
                return jsonify({"message": "Cliente não encontrado"}), 404
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao deletar cliente: {e}")
        return jsonify({"error": f"Erro no banco de dados: {str(e)}"}), 500
    except ValueError as e: # Captura a validação de condições vazias do CRUD
        logger.error(f"Erro de validação na deleção: {e}")
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Erro inesperado ao deletar cliente: {e}")
        return jsonify({"error": f"Erro interno do servidor: {str(e)}"}), 500

if __name__ == '__main__':
    # Cria o diretório common se não existir, para garantir que os arquivos de configuração possam ser colocados
    Path(common_dir).mkdir(parents=True, exist_ok=True)
    
    # Exemplo de como você pode criar um config_postgres.json se ele não existir
    # Em um ambiente real, você faria isso separadamente ou usaria variáveis de ambiente/Dockerfile
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

    app.run(debug=True, port=5000) # Rode em modo de depuração para desenvolvimento