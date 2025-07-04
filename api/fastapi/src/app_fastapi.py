# app_fastapi.py

import sys
import os
import json
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

# Adiciona o diretório 'common' ao sys.path para importar o connectorDB.py
# Isso permite que scripts fora de 'common' importem módulos de 'common'.
current_dir = Path(__file__).parent
common_dir = current_dir / "common"
if str(common_dir) not in sys.path:
    sys.path.insert(0, str(common_dir))
common_dir_src = current_dir / "common/src/"
if str(common_dir_src) not in sys.path:
    sys.path.insert(0, str(common_dir_src))

from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

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

app = FastAPI()

# Configuração de logger para a aplicação FastAPI
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuração do Banco de Dados ---
# Certifique-se de que o arquivo nome_arquivo.json exista e esteja correto.
CONFIG_FILE = str(common_dir / "config_firebird.json")
# Nome da tabela para as operações CRUD
TABLE_NAME = "cliente"

# Definição do Pydantic Model para Cliente
class Cliente(BaseModel):
    id_cliente: str
    nome: str
    email: str
    # Adicionar outros campos conforme a estrutura da sua tabela cliente

# Modelo para atualização, onde todos os campos são opcionais
class ClienteUpdate(BaseModel):
    id_cliente: Optional[str] = None
    nome: Optional[str] = None
    email: Optional[str] = None

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
        # Retorna o conector e o CRUD para que o ponto de chamada possa usar 'with'.
        connector = manager.get_connector()
        return connector, CRUD(connector)
    except (ConnectionError, ConfigError, SecurityError, DatabaseError) as e:
        logger.error(f"Erro ao inicializar o conector do banco de dados: {e}")
        return None, None # Sinaliza falha

@app.post("/clientes", response_model=Dict[str, Any], status_code=status.HTTP_201_CREATED)
async def create_cliente(cliente: Cliente):
    """Cria um novo cliente."""
    data = cliente.model_dump() # Usa model_dump() para obter um dicionário

    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falha na conexão com o banco de dados."
            )

        with connector as conn: # Garante que a conexão seja aberta e fechada
            rows_affected = crud.create(TABLE_NAME, data)
            if rows_affected > 0:
                logger.info(f"Cliente {data.get('id_cliente')} criado com sucesso.")
                return JSONResponse(
                    content={"message": "Cliente criado com sucesso", "rows_affected": rows_affected},
                    status_code=status.HTTP_201_CREATED
                )
            else:
                logger.warning(f"Nenhum cliente criado para os dados: {data}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Nenhum cliente criado. Verifique os dados."
                )
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao criar cliente: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro no banco de dados: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Erro inesperado ao criar cliente: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno do servidor: {str(e)}"
        )

@app.get("/clientes", response_model=List[Dict[str, Any]])
async def get_clientes(request: Request):
    """Lê clientes com base em query parameters (condições) ou todos."""
    conditions = dict(request.query_params) # Obtém query parameters como dicionário

    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falha na conexão com o banco de dados."
            )

        with connector as conn:
            clientes = crud.read(TABLE_NAME, conditions)
            # Como os conectores MySQL e PostgreSQL retornam dicionários e Firebird tuplas,
            # asseguramos que o retorno seja sempre uma lista de dicionários para JSONResponse.
            if clientes and isinstance(clientes[0], tuple):
                # Se for Firebird, o `read` retorna tuplas. Precisamos converter para dicts.
                # Isso exigiria obter os nomes das colunas, que não são facilmente acessíveis
                # na interface IDBConnector para um resultado genérico de `read`.
                # Para manter a compatibilidade total com o Flask original que serializa tuplas,
                # aqui assumimos que os resultados são dicionários (como em MySQL/Postgres)
                # ou que a serialização de tuplas pelo JSONResponse do FastAPI é aceitável.
                # Para uma solução robusta com Firebird, o `read` do CRUD ou o conector
                # precisaria retornar os nomes das colunas ou um DictCursor.
                # Por ora, FastAPI vai serializar a lista de tuplas como lista de listas JSON.
                pass # Deixa como está, FastAPI vai converter tuplas em listas JSON
            return JSONResponse(content=clientes, status_code=status.HTTP_200_OK)
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao ler clientes: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro no banco de dados: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Erro inesperado ao ler clientes: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno do servidor: {str(e)}"
        )

@app.get("/clientes/{id_cliente}", response_model=Dict[str, Any])
async def get_cliente_by_id(id_cliente: str):
    """Lê um cliente específico pelo id_cliente."""
    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falha na conexão com o banco de dados."
            )

        with connector as conn:
            # Assumimos que id_cliente é a chave primária ou um campo único para busca
            cliente = crud.read(TABLE_NAME, {"id_cliente": id_cliente})
            if cliente:
                return JSONResponse(content=cliente[0], status_code=status.HTTP_200_OK)
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Cliente não encontrado"
                )
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao ler cliente por ID: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro no banco de dados: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Erro inesperado ao ler cliente por ID: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno do servidor: {str(e)}"
        )

@app.put("/clientes/{id_cliente}", response_model=Dict[str, Any])
async def update_cliente(id_cliente: str, cliente_update: ClienteUpdate):
    """Atualiza um cliente existente pelo id_cliente."""
    data = cliente_update.model_dump(exclude_unset=True) # Exclui campos que não foram definidos

    if not data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Nenhum dado para atualização fornecido."
        )

    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falha na conexão com o banco de dados."
            )

        with connector as conn:
            conditions = {"id_cliente": id_cliente}
            rows_affected = crud.update(TABLE_NAME, data, conditions)
            if rows_affected > 0:
                logger.info(f"Cliente {id_cliente} atualizado com sucesso.")
                return JSONResponse(
                    content={"message": "Cliente atualizado com sucesso", "rows_affected": rows_affected},
                    status_code=status.HTTP_200_OK
                )
            else:
                logger.warning(f"Nenhum cliente encontrado ou atualizado para ID: {id_cliente}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Cliente não encontrado ou nenhum dado para atualização"
                )
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao atualizar cliente: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro no banco de dados: {str(e)}"
        )
    except ValueError as e: # Captura a validação de condições vazias do CRUD
        logger.error(f"Erro de validação na atualização: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Erro inesperado ao atualizar cliente: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno do servidor: {str(e)}"
        )

@app.delete("/clientes/{id_cliente}", response_model=Dict[str, Any])
async def delete_cliente(id_cliente: str):
    """Deleta um cliente existente pelo id_cliente."""
    try:
        connector, crud = get_crud_instance()
        if connector is None or crud is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falha na conexão com o banco de dados."
            )

        with connector as conn:
            conditions = {"id_cliente": id_cliente}
            rows_affected = crud.delete(TABLE_NAME, conditions)
            if rows_affected > 0:
                logger.info(f"Cliente {id_cliente} deletado com sucesso.")
                return JSONResponse(
                    content={"message": "Cliente deletado com sucesso", "rows_affected": rows_affected},
                    status_code=status.HTTP_200_OK
                )
            else:
                logger.warning(f"Nenhum cliente encontrado para deleção com ID: {id_cliente}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Cliente não encontrado"
                )
    except DatabaseError as e:
        logger.error(f"Erro no banco de dados ao deletar cliente: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro no banco de dados: {str(e)}"
        )
    except ValueError as e: # Captura a validação de condições vazias do CRUD
        logger.error(f"Erro de validação na deleção: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Erro inesperado ao deletar cliente: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno do servidor: {str(e)}"
        )

# Configuração inicial para criar o arquivo config_postgres.json se não existir
@app.on_event("startup")
async def startup_event():
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

# Para rodar com Uvicorn: uvicorn app_fastapi:app --reload --port 8000
