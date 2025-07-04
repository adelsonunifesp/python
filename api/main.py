# main.py

import time
import os
import sys
import json
import stat
from pathlib import Path
from typing import Optional, Any, Dict, List, Union, Tuple

# Adiciona o diretório 'common' ao sys.path para importar o connectorDB.py
# Isso permite que scripts fora de 'common' importem módulos de 'common'.
current_dir = Path(__file__).parent
common_dir = current_dir / "common"
if str(common_dir) not in sys.path:
    sys.path.insert(0, str(common_dir))
common_dir_src = current_dir / "common/src/"
if str(common_dir_src) not in sys.path:
    sys.path.insert(0, str(common_dir_src))

# Importa as classes do seu módulo refatorado connectorDB
from connectorDB import DBConnectionManager, IDBConnector, TableMetadata
from connectorDB import DatabaseError, ConnectionError, ConfigError, SecurityError

# Importa a classe CRUD
from crud import CRUD 

import logging

# Configuração de logger para este script de teste
# Mude para logging.DEBUG se quiser ver logs mais detalhados do conector e do CRUD
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def testar_conexao_e_metadados(config_file_path: str, table_name: str):
    """
    Testa a conexão e a recuperação de metadados de uma tabela,
    utilizando o DBConnectionManager e o gerenciador de contexto.
    """
    try:
        print(f"\n--- Iniciando teste de conexão para '{config_file_path}' ---")

        # Verifica arquivo de configuração e permissões (reaproveitando lógica existente)
        config_path = Path(config_file_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Arquivo de configuração '{config_file_path}' não encontrado.")
        if os.name != 'nt':  # Só verifica permissões em sistemas Unix
            mode = config_path.stat().st_mode
            if mode & (stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP):
                logger.warning(f"⚠️ Aviso: Permissões amplas detectadas para '{config_file_path}'. Recomenda-se 0o600.")

        # Criar DBConnectionManager para carregar a configuração
        manager = DBConnectionManager(config_file_path)

        # Usar o gerenciador de contexto com o conector obtido
        with manager.get_connector() as conn: # Agora o conector (conn) suporta 'with'
            print("✅ Conexão estabelecida com sucesso usando DBConnectionManager!")

            print(f"\n--- Obtendo metadados da tabela '{table_name}' ---")
            try:
                metadata: TableMetadata = conn.get_table_metadata(table_name)

                print("\n🔍 METADADOS COMPLETOS:")

                print("\n📋 INFORMAÇÕES DA TABELA:")
                print(f"  - Nome: {metadata.name}")
                if metadata.size_bytes is not None:
                    size_gb = metadata.size_bytes / (1024 ** 3)
                    if size_gb >= 1:
                        print(f"  - Tamanho: {size_gb:.2f} GB")
                    else:
                        size_mb = metadata.size_bytes / (1024 ** 2)
                        if size_mb >= 1:
                            print(f"  - Tamanho: {size_mb:.2f} MB")
                        else:
                            size_kb = metadata.size_bytes / 1024
                            print(f"  - Tamanho: {size_kb:.2f} KB")

                if metadata.row_count is not None:
                    print(f"  - Total de registros: {metadata.row_count}")
                if metadata.comment:
                    print(f"  - Descrição da tabela: {metadata.comment}")
                else:
                    print("  - Descrição da tabela: Nenhuma descrição disponível")

                print(f"\n📝 COLUNAS ({len(metadata.columns)}):")
                for col in metadata.columns:
                    col_info = f"  → {col.name}: {col.type}"
                    col_info += f" ({'NULL' if col.is_nullable else 'NOT NULL'})"
                    if col.default_value is not None:
                        col_info += f" DEFAULT '{col.default_value}'"

                    details = []
                    if col.max_length is not None:
                        details.append(f"Comp. Máx: {col.max_length}")
                    if col.numeric_precision is not None:
                        details.append(f"Precisão: {col.numeric_precision}")
                    if col.numeric_scale is not None:
                        details.append(f"Escala: {col.numeric_scale}")
                    if col.is_primary_key:
                        details.append("PK")
                    if col.is_unique:
                        details.append("Único")

                    if details:
                        col_info += " [" + ", ".join(details) + "]"

                    if col.comment:
                        col_info += f"\n    Descrição: {col.comment}"
                    else:
                        col_info += "\n    Descrição: Nenhuma descrição disponível"

                    print(col_info)

                if metadata.primary_keys:
                    print(f"\n🔑 CHAVE PRIMÁRIA: {', '.join(metadata.primary_keys)}")
                else:
                    print("\n🔑 CHAVE PRIMÁRIA: Nenhuma chave primária definida")

                if metadata.foreign_keys:
                    print("\n🔗 CHAVES ESTRANGEIRAS:")
                    for fk in metadata.foreign_keys:
                        print(f"  → Nome da Constraint: {fk.name}")
                        print(f"    Coluna Local: '{fk.column_name}'")
                        print(f"    Referencia: '{fk.referenced_table_name}'.'{fk.referenced_column_name}'")
                        print(f"    Ações: ON UPDATE '{fk.on_update}', ON DELETE '{fk.on_delete}'")
                else:
                    print("\n🔗 CHAVES ESTRANGEIRAS: Nenhuma chave estrangeira definida")

                if metadata.indexes:
                    print("\n📊 ÍNDICES:")
                    for idx in metadata.indexes:
                        idx_type_info = f" ({idx.type})" if idx.type else ""
                        is_unique_info = " (Único)" if idx.is_unique else ""
                        is_primary_info = " (PK Index)" if idx.is_primary else ""
                        print(f"  → Nome: {idx.name}{idx_type_info}{is_unique_info}{is_primary_info}, Colunas: {', '.join(idx.columns)}")
                else:
                    print("\n📊 ÍNDICES: Nenhum índice adicional definido")

                print("\n🧪 TESTE DE CONSULTA:")
                try:
                    # Adaptação para o nome da tabela no PostgreSQL e Firebird
                    db_type = conn.db_type
                    if db_type == 'mysql':
                        query_test = f"SELECT COUNT(*) FROM `{table_name}`"
                    elif db_type == 'firebird':
                        query_test = f"SELECT COUNT(*) FROM \"{table_name.upper()}\""
                    else:  # PostgreSQL
                        query_test = f"SELECT COUNT(*) FROM \"{table_name}\""

                    resultado = conn.execute_query(query_test)
                    count_value = None

                    if resultado:
                        if isinstance(resultado[0], dict):
                            count_value = next(iter(resultado[0].values()), None) # Extrai o valor do dicionário
                        elif isinstance(resultado[0], tuple):
                            count_value = resultado[0][0] # Extrai o primeiro elemento da tupla
                        else: # Fallback para outros tipos de retorno
                            count_value = resultado[0]

                    if count_value is not None:
                        print(f"Total de registros na tabela '{table_name}': {count_value}")
                    else:
                        print(f"Não foi possível obter o total de registros para a tabela '{table_name}'.")

                except Exception as query_error:
                    print(f"⚠️ Erro ao executar consulta de teste: {query_error}")

            except Exception as meta_error:
                print(f"⚠️ Erro ao obter metadados da tabela '{table_name}': {meta_error}")

    except FileNotFoundError as e:
        print(f"❌ Falha no teste: {e}")
    except ValueError as e:
        print(f"❌ Falha de configuração: {e}")
    except ConnectionError as e:
        print(f"❌ Falha de conexão com o banco de dados: {e}")
    except ConfigError as e:
        print(f"❌ Erro de configuração do sistema: {e}")
    except SecurityError as e:
        print(f"❌ Erro de segurança (criptografia): {e}")
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado durante o teste: {e}")
    finally:
        print(f"\n--- Teste para '{config_file_path}' finalizado ---")

def criar_arquivo_config(config_file_path: str, content: dict):
    """Cria arquivo de configuração com permissões seguras."""
    config_path = Path(config_file_path)
    # Garante que o diretório exista
    config_path.parent.mkdir(parents=True, exist_ok=True) 

    if not config_path.exists():
        try:
            with open(config_path, "w", encoding='utf-8') as f:
                json.dump(content, f, indent=2)
            if os.name != 'nt': # Aplica permissões em sistemas não-Windows
                os.chmod(config_path, 0o600)
            print(f"Arquivo '{config_file_path}' criado com sucesso.")
        except Exception as e:
            print(f"Erro ao criar arquivo '{config_file_path}': {e}")
    else:
        print(f"Arquivo '{config_file_path}' já existe. Ignorando criação.")

if __name__ == "__main__":
    # --- Configurações de Teste ---
    # As senhas e/ou usuários DEVERIAM estar criptografados no JSON
    # e a chave em common/secret.key.
    # Para este exemplo, deixei em texto simples para facilidade de visualização.
    # No entanto, em um cenário real, use o script setup_db_creds.py para criptografar.

    # Certifique-se de que esses valores são válidos para seus ambientes de DB!
    # A tabela 'cliente' é um exemplo para PostgreSQL e MySQL.
    # A tabela 'ESTADO' é um exemplo para Firebird (geralmente nomes de tabelas em maiúsculas).
    test_configs = [
        {
            "config_path": str(common_dir / "config_firebird.json"),
            "table_name": "ESTADO",
            "content": {
                "db_type": "firebird",
                "host": "172.20.96.56",
                "database": "/opt/firebird/data/sua_base_firebird.fdb", # ALtere para seu caminho!
                "user": "sysdba",
                "password": "ENC:gAAAAABoTAK-XO3CnCePFZxk5yniTMHinJIBqeNUIw8hvt34os0fxYcMJbPds4KH_6E78faXR9k5-0JsOXXwM-3wDEmvi_NNPA==",
                "port": 3050,
                "charset": "UTF8"
            }
        },
        {
            "config_path": str(common_dir / "config_mysql.json"),
            "table_name": "cliente",
            "content": {
                "db_type": "mysql",
                "host": "172.20.96.56",
                "database": "s4laldeveloper",
                "user": "root",
                "password": "ENC:gAAAAABoTAK-Y8TvZPT3pHSGHFM5N5lGrIfq-Xas-gE9q35b6V5Py-8xDx9DyxnnwA8x8-LuUW4PKI9OiAwubzI-5wXyrlLMSw==",
                "port": 3306
            }
        },
        {
            "config_path": str(common_dir / "config_postgres.json"),
            "table_name": "cliente", 
            "content": {
                "db_type": "postgresql",
                "host": "172.20.96.56",
                "database": "s4laldeveloper",
                "user": "postgres",
                "password": "ENC:gAAAAABoTAK-X0iyr4ddpu2v0Fhz95ugIZsbe_YzCBN4SY7-9xXkHI7TEYFt_QAPIo3i6RB8JVuQnTkzAgnXVqmY6bHDw3JWdw==",
                "port": 5432,
                "schema": "public"
            }
        }
    ]

    # Criar arquivos de configuração e testar cada SGBD
    for test_data in test_configs:
        criar_arquivo_config(test_data["config_path"], test_data["content"])
        testar_conexao_e_metadados(test_data["config_path"], test_data["table_name"])

    # --- Bloco de testes CRUD (Exemplo com PostgreSQL) ---
    print("\n" + "="*55)
    print("--- INICIANDO TESTES CRUD (PostgreSQL como exemplo) ---")
    print("="*55 + "\n")

    postgres_config_path = str(common_dir / "config_postgres.json")
    crud_table_name = "cliente" # Assegure-se de que esta tabela exista e seja adequada para CRUD

    try:
        # Cria o manager para o PostgreSQL
        manager_crud = DBConnectionManager(postgres_config_path)

        # Obtém o conector e o passa para a classe CRUD
        # A conexão será aberta e fechada automaticamente pelo 'with' do conector
        with manager_crud.get_connector() as conn_for_crud:
            crud = CRUD(conn_for_crud) # Passa a instância do conector aberta

            temp_prefix = "tpt"
            current_timestamp = int(time.time())

            # --- Limpeza de dados de teste (garante que IDs e emails não se acumulem) ---
            print("\n--- Iniciando limpeza de dados de teste para CRUD ---")
            # Adiciona IDs e e-mails baseados no prefixo temporário e timestamps recentes
            ids_and_emails_to_clean = [
                f"{temp_prefix}a{current_timestamp}", f"{temp_prefix}a{current_timestamp - 1}",
                f"{temp_prefix}b{current_timestamp}", f"{temp_prefix}b{current_timestamp - 1}",
                f"{temp_prefix}t{current_timestamp}", f"{temp_prefix}t{current_timestamp - 1}",
                f"{temp_prefix}a{current_timestamp}@example.com", f"{temp_prefix}a{current_timestamp - 1}@example.com",
                f"{temp_prefix}b{current_timestamp}@example.com", f"{temp_prefix}b{current_timestamp - 1}@example.com",
                f"{temp_prefix}t{current_timestamp}@example.com", f"{temp_prefix}t{current_timestamp - 1}@example.com",
            ]
            
            # Limpa registros antigos ou de execuções anteriores com IDs/emails "fixos"
            fixed_test_ids_emails = ["alice_test_id", "bruno_test_id", "transaction_test_id",
                                      "alice.test@example.com", "bruno.test@example.com", "transaction.test@example.com"]
            ids_and_emails_to_clean.extend(fixed_test_ids_emails)


            # Realiza a limpeza
            for unique_val in set(ids_and_emails_to_clean):
                try:
                    rows_deleted_id = crud.delete(crud_table_name, {"id_cliente": unique_val})
                    if rows_deleted_id > 0:
                        print(f"Cliente com ID '{unique_val}' deletado na limpeza.")
                except DatabaseError as e:
                    # Ignora erros se o registro não existe, mas loga outros problemas
                    if "no rows affected" not in str(e).lower() and "does not exist" not in str(e).lower():
                        logger.warning(f"⚠️ Erro inesperado durante limpeza por ID '{unique_val}': {e}")
                except Exception as e:
                    logger.warning(f"⚠️ Erro genérico durante limpeza por ID '{unique_val}': {e}")
                
                try:
                    rows_deleted_email = crud.delete(crud_table_name, {"email": unique_val})
                    if rows_deleted_email > 0:
                        print(f"Cliente com email '{unique_val}' deletado na limpeza.")
                except DatabaseError as e:
                    if "no rows affected" not in str(e).lower() and "does not exist" not in str(e).lower():
                        logger.warning(f"⚠️ Erro inesperado durante limpeza por email '{unique_val}': {e}")
                except Exception as e:
                    logger.warning(f"⚠️ Erro genérico durante limpeza por email '{unique_val}': {e}")
            print("--- Limpeza de dados de teste para CRUD finalizada ---")

            # --- Testes CRUD ---
            new_cliente_id_1 = f"{temp_prefix}a{current_timestamp}"
            new_cliente_email_1 = f"{temp_prefix}a{current_timestamp}@example.com"

            new_cliente_id_2 = f"{temp_prefix}b{current_timestamp + 1}"
            new_cliente_email_2 = f"{temp_prefix}b{current_timestamp + 1}@example.com"

            new_cliente_id_3_transaction = f"{temp_prefix}t{current_timestamp + 2}"
            new_cliente_email_3_transaction = f"{temp_prefix}t{current_timestamp + 2}@example.com"

            # Criar (CREATE)
            print("\n--- Criando novo cliente ---")
            cliente_alice_data = {"nome": "Alice Silva", "email": new_cliente_email_1, "id_cliente": new_cliente_id_1}
            rows_affected_create_1 = crud.create(crud_table_name, cliente_alice_data)
            print(f"Linhas afetadas (CREATE Alice): {rows_affected_create_1}")

            cliente_bruno_data = {"nome": "Bruno Souza", "email": new_cliente_email_2, "id_cliente": new_cliente_id_2}
            rows_affected_create_2 = crud.create(crud_table_name, cliente_bruno_data)
            print(f"Linhas afetadas (CREATE Bruno): {rows_affected_create_2}")

            # Ler (READ) - Todos os clientes
            print("\n--- Lendo todos os clientes ---")
            all_clientes = crud.read(crud_table_name)
            for cliente in all_clientes:
                if isinstance(cliente, dict) and (new_cliente_id_1 in str(cliente.get("id_cliente")) or new_cliente_id_2 in str(cliente.get("id_cliente"))):
                    print(cliente)

            # Ler (READ) - Cliente específico
            print(f"\n--- Lendo cliente com email '{new_cliente_email_1}' ---")
            alice = crud.read(crud_table_name, {"email": new_cliente_email_1})
            print(alice)

            # Atualizar (UPDATE)
            print("\n--- Atualizando cliente Alice ---")
            update_data = {"nome": "Alice G. Silva", "email": new_cliente_email_1} # Incluir email para PostgreSQL update
            conditions_update = {"id_cliente": new_cliente_id_1} # Usar ID para a condição
            rows_affected_update = crud.update(crud_table_name, update_data, conditions_update)
            print(f"Linhas afetadas (UPDATE): {rows_affected_update}")

            # Ler (READ) novamente para verificar a atualização
            print(f"\n--- Lendo cliente Alice após atualização ---")
            alice_updated = crud.read(crud_table_name, {"id_cliente": new_cliente_id_1})
            print(alice_updated)

            # Deletar (DELETE)
            print("\n--- Deletando cliente Bruno ---")
            conditions_delete = {"id_cliente": new_cliente_id_2}
            rows_affected_delete = crud.delete(crud_table_name, conditions_delete)
            print(f"Linhas afetadas (DELETE): {rows_affected_delete}")

            # Ler (READ) para verificar a exclusão
            print("\n--- Lendo todos os clientes após exclusão ---")
            remaining_clientes = crud.read(crud_table_name)
            found_bruno = False
            for cliente in remaining_clientes:
                if isinstance(cliente, dict) and new_cliente_id_2 in str(cliente.get("id_cliente")):
                    found_bruno = True
                print(cliente)
            if not found_bruno:
                print("Cliente Bruno não encontrado (esperado).")
            else:
                print("Cliente Bruno AINDA encontrado (INESPERADO!).")

            # Exemplo de transação
            print("\n--- Exemplo de Transação ---")
            try:
                crud.begin_transaction()
                print("Transação iniciada.")

                cliente_in_transaction = {"nome": "Teste Transação", "email": new_cliente_email_3_transaction, "id_cliente": new_cliente_id_3_transaction}
                crud.create(crud_table_name, cliente_in_transaction)
                print("Registro criado na transação.")

                # Descomente a linha abaixo para simular um erro e testar o rollback
                # raise ValueError("Simulando um erro para rollback!")

                crud.commit()
                print("Transação confirmada.")
            except Exception as e:
                print(f"Erro na transação: {e}. Realizando rollback.")
                crud.rollback()
                print("Transação desfeita (rollback).")
            
            # Tentar ler para ver se o registro foi desfeito após rollback
            trans_cliente = crud.read(crud_table_name, {"id_cliente": new_cliente_id_3_transaction})
            if not trans_cliente:
                print("Registro de transação não encontrado após rollback (esperado).")
            else:
                print(f"Registro de transação encontrado após rollback (INESPERADO!): {trans_cliente}")

    except ConnectionError as e:
        print(f"❌ Erro de conexão durante testes CRUD: {e}")
    except DatabaseError as e:
        print(f"❌ Erro de banco de dados durante testes CRUD: {e}")
    except ConfigError as e:
        print(f"❌ Erro de configuração durante testes CRUD: {e}")
    except SecurityError as e:
        print(f"❌ Erro de segurança durante testes CRUD: {e}")
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado durante os testes CRUD: {e}")
    finally:
        print("\n--- Testes CRUD finalizados. ---")
