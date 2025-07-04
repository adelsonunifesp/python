import os
import json
import stat
import logging
from cryptography.fernet import Fernet, InvalidToken
import copy

# Importações dos drivers de banco de dados
# As importações são feitas diretamente aqui. As checagens de None
# serão feitas ao instanciar os conectores e ao definir os tipos globais.

fdb_mod = None
fdb_available = False
try:
    import firebird.driver as fdb_mod
    fdb_available = True
except ImportError:
    logging.warning("Firebird driver (firebird.driver) não encontrado. Conexão Firebird não estará disponível.")

mysql_connector_mod = None
mysql_connector_available = False
try:
    import mysql.connector as mysql_connector_mod
    mysql_connector_available = True
except ImportError:
    logging.warning("MySQL driver (mysql-connector-python) não encontrado. Conexão MySQL não estará disponível.")

psycopg2_mod = None
psycopg2_extras_mod = None
psycopg2_extensions_mod = None
psycopg2_available = False
psycopg2_extras_available = False
psycopg2_extensions_available = False
try:
    import psycopg2 as psycopg2_mod
    import psycopg2.extras as psycopg2_extras_mod
    import psycopg2.extensions as psycopg2_extensions_mod # Importa extensions
    psycopg2_available = True
    psycopg2_extras_available = True
    psycopg2_extensions_available = True
except ImportError:
    logging.warning("PostgreSQL driver (psycopg2-binary) não encontrado. Conexão PostgreSQL não estará disponível.")

# Importações do módulo typing
from typing import Dict, Any, List, Optional, Union, Tuple, cast
from pathlib import Path
from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod

# --- Configuração do Logger ---
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# Opcional: Para debugar queries e params, mudar para logging.DEBUG
# logger.setLevel(logging.DEBUG)


# --- Dataclasses para Metadados ---

@dataclass
class ColumnMetadata:
    """Metadados de uma coluna de tabela."""
    name: str
    type: str
    is_nullable: bool = True
    default_value: Optional[str] = None
    max_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    is_primary_key: bool = False
    is_unique: bool = False
    comment: Optional[str] = None


@dataclass
class ForeignKeyMetadata:
    """Metadados de uma chave estrangeira."""
    name: str
    column_name: str
    referenced_table_name: str
    referenced_column_name: str
    on_update: Optional[str] = None
    on_delete: Optional[str] = None


@dataclass
class IndexMetadata:
    """Metadados de um índice."""
    name: str
    columns: List[str] = field(default_factory=list)
    is_unique: bool = False
    is_primary: bool = False
    type: Optional[str] = None


@dataclass
class TableMetadata:
    """Metadados completos de uma tabela."""
    name: str
    columns: List[ColumnMetadata] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[ForeignKeyMetadata] = field(default_factory=list)
    indexes: List[IndexMetadata] = field(default_factory=list)
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    comment: Optional[str] = None


# --- Classes de Exceção Personalizadas ---

class ConfigError(Exception):
    """Exceção para erros de configuração."""
    pass


class SecurityError(Exception):
    """Exceção para erros de segurança (ex: criptografia)."""
    pass


class DatabaseError(Exception):
    """Exceção base para erros de banco de dados."""
    pass


class ConnectionError(DatabaseError):
    """Exceção para erros de conexão com o banco de dados."""
    pass


# --- Função auxiliar para conversão segura para int ---
def _safe_int_conversion(value: Any) -> Optional[int]:
    """
    Tenta converter um valor para int. Retorna None se o valor é None,
    string vazia/espaços, ou não pode ser convertido para int.
    """
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
        return int(value)
    except (ValueError, TypeError):
        return None


# --- Interface e Base para Conectores de DB Específicos ---

# Definido tipos para as conexões específicas
FirebirdConnection: Any = Any
MySQLConnection: Any = Any
PostgreSQLConnection: Any = Any


class IDBConnector(ABC):
    """
    Interface para conectores de banco de dados específicos.
    Define o contrato para todos os conectores de DB.
    Também define a interface para um gerenciador de contexto (com o 'with' statement).
    """
    connection: Any # Mantido como Any para máxima compatibilidade com linters aqui
    config: Dict[str, Any]
    db_type: str

    def __init__(self, connection_config: Dict[str, Any]):
        self.config = connection_config
        self.connection = None
        self.db_type = self.config.get('db_type', 'unknown').lower()
    
    @abstractmethod
    def start_transaction(self):
        """Inicia uma transação explícita. Pode não ser necessário se o driver gerencia implicitamente com autocommit=False."""
        pass

    @abstractmethod
    def commit_transaction(self):
        """Confirma a transação atual."""
        pass

    @abstractmethod
    def rollback_transaction(self):
        """Desfaz a transação atual."""
        pass

    @abstractmethod
    def get_placeholder(self) -> str:
        """Retorna o placeholder de parâmetro específico do SGBD (ex: %s, ?)."""
        pass

    @abstractmethod
    def connect(self) -> Any:
        """Estabelece conexão com o banco de dados."""
        pass

    @abstractmethod
    def disconnect(self):
        """Fecha a conexão com o banco de dados."""
        pass

    @abstractmethod
    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """Obtém metadados de uma tabela."""
        pass

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Union[Tuple[Any, ...], Dict[str, Any]]]:
        """Executa uma consulta SQL e retorna os resultados. Pode ser lista de tuplas ou dicionários."""
        pass

    @abstractmethod
    def execute_update(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> int:
        """Executa uma atualização SQL."""
        pass

    @abstractmethod
    def _get_cursor(self) -> Any:
        """Método abstrato para retornar um cursor, a ser implementado por cada conector específico."""
        pass

    @abstractmethod
    def __enter__(self) -> 'IDBConnector':
        """
        Método de entrada do gerenciador de contexto.
        DEVE ser implementado por classes concretas para estabelecer a conexão.
        """
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Método de saída do gerenciador de contexto.
        DEVE ser implementado por classes concretas para fechar a conexão
        e lidar com commit/rollback.
        """
        pass


class BaseDBConnector(IDBConnector):
    """
    Classe base abstrata para implementação de conectores de SGBD.
    Fornece implementações padrão para os métodos do gerenciador de contexto
    e operações de consulta/atualização.
    """
    def start_transaction(self):
        """
        Inicia uma transação explícita.
        Para drivers com autocommit=False (MySQL, PostgreSQL), um novo bloco de transação
        geralmente é iniciado automaticamente após um commit/rollback.
        Para Firebird, pode ser necessário um `connection.begin()`.
        Este método pode ser uma no-op para alguns drivers, mas é mantido para consistência da interface.
        """
        logger.debug(f"start_transaction chamado para {self.db_type}. Gerenciamento via autocommit=False ou driver.")
        if self.connection and hasattr(self.connection, 'begin'): # Firebird
            try:
                self.connection.begin() # type: ignore [attr-defined]
                logger.info("Transação Firebird iniciada explicitamente.")
            except Exception as e:
                logger.error(f"Erro ao iniciar transação Firebird: {e}")
                raise DatabaseError(f"Erro ao iniciar transação Firebird: {e}") from e
        # Para MySQL/PostgreSQL com autocommit=False, um 'BEGIN' é implícito.
        # Nenhuma ação explícita é necessária aqui.
        pass

    def commit_transaction(self):
        if self.connection is None:
            raise ConnectionError(f"Conexão com o banco de dados ({self.db_type}) não estabelecida para commit.")
        try:
            self.connection.commit()
            logger.info(f"Transação {self.db_type} confirmada.")
        except Exception as e:
            logger.error(f"Erro ao confirmar transação {self.db_type}: {e}")
            raise DatabaseError(f"Erro ao confirmar transação {self.db_type}: {e}") from e

    def rollback_transaction(self):
        if self.connection is None:
            raise ConnectionError(f"Conexão com o banco de dados ({self.db_type}) não estabelecida para rollback.")
        try:
            self.connection.rollback()
            logger.warning(f"Transação {self.db_type} desfeita (rollback).")
        except Exception as e:
            logger.error(f"Erro ao desfazer transação {self.db_type}: {e}")
            raise DatabaseError(f"Erro ao desfazer transação {self.db_type}: {e}") from e
        
    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Union[Tuple[Any, ...], Dict[str, Any]]]:
        """Executa uma consulta e retorna os resultados. Implementação concreta do IDBConnector."""
        if self.connection is None:
            raise ConnectionError(f"Conexão com o banco de dados ({self.db_type}) não estabelecida.")

        try:
            with self._get_cursor() as cursor:
                logger.debug(f"Executando consulta ({self.db_type}): {query} com parâmetros: {params}")
                cursor.execute(query, params or ())
                return cursor.fetchall()
        except Exception as db_err:
            if self.connection and hasattr(self.connection, 'rollback'):
                try:
                    self.connection.rollback() # type: ignore [attr-defined]
                except Exception as rb_err:
                    logger.error(f"Erro ao tentar rollback durante erro de consulta: {rb_err}")
            logger.error(f"Erro no banco de dados ({self.db_type}) ao executar consulta: {query}\n - {db_err}")
            raise DatabaseError(f"Erro ao executar consulta: {query}\n - {str(db_err)}")

    def execute_update(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> int:
        """Executa uma atualização e retorna o número de linhas afetadas. Implementação concreta do IDBConnector."""
        if self.connection is None:
            raise ConnectionError(f"Conexão com o banco de dados ({self.db_type}) não estabelecida.")

        try:
            with self._get_cursor() as cursor:
                logger.debug(f"Executando atualização ({self.db_type}): {query} com parâmetros: {params}")
                cursor.execute(query, params or ())
                return cursor.rowcount
        except Exception as db_err:
            if self.connection and hasattr(self.connection, 'rollback'):
                try:
                    self.connection.rollback() # type: ignore [attr-defined]
                except Exception as rb_err:
                    logger.error(f"Erro ao tentar rollback durante erro de atualização: {rb_err}")
            logger.error(f"Erro no banco de dados ({self.db_type}) ao executar atualização: {query}\n - {db_err}")
            raise DatabaseError(f"Erro ao executar atualização: {query}\n - {str(db_err)}")

    @abstractmethod
    def _get_cursor(self) -> Any:
        pass

    @abstractmethod
    def connect(self) -> Any:
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def get_table_metadata(self, table_name: str) -> TableMetadata:
        pass

    def __enter__(self) -> 'BaseDBConnector': # Retorna a própria instância
        """
        Entra no contexto do gerenciador de conexão, estabelecendo a conexão.
        Retorna a própria instância do conector.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Sai do contexto do gerenciador de conexão, fechando a conexão.
        Realiza rollback em caso de exceção, ou commit se não houver.
        """
        if self.connection:
            if exc_type:
                # Ocorreu um erro, tenta fazer rollback
                if hasattr(self.connection, 'rollback'):
                    try:
                        self.connection.rollback()
                        logger.error(f"Rollback realizado devido a um erro no bloco 'with': {exc_val}")
                    except Exception as rb_err:
                        logger.error(f"Erro ao tentar rollback durante exceção no bloco 'with': {rb_err}")
            else:
                # Nenhum erro, tenta fazer commit
                if hasattr(self.connection, 'commit'):
                    try:
                        self.connection.commit()
                        logger.info("Transação confirmada no final do bloco 'with'.")
                    except Exception as cm_err:
                        logger.error(f"Erro ao tentar commit no final do bloco 'with': {cm_err}")
            self.disconnect()


# --- Implementações Específicas de Conectores ---

class FirebirdConnector(BaseDBConnector):
    """Conector para Firebird Database."""
    
    def get_placeholder(self) -> str:
        return "?"

    def connect(self) -> Any:
        # Verifica se já está conectado de forma robusta para firebird-driver
        # if self.connection and self.connection.is_connected():
        #     logger.info("Conexão Firebird já estabelecida.")
        #     return self.connection

        # Use fdb_mod que foi importado como firebird.driver
        if fdb_mod is None or not fdb_available:
            raise ImportError("Firebird driver não está instalado ou não pôde ser carregado.")

        # Acessa os dados de conexão via self.config, que é populado pelo __init__ da classe base
        host = self.config.get('host')
        port = self.config.get('port', 3050)
        database_path = self.config.get('database')
        user_val = self.config.get('user')
        password_val = self.config.get('password')
        charset = str(self.config.get('charset', 'UTF8'))

        if not host:
            raise ConnectionError("Host Firebird não fornecido na configuração.")
        if not database_path: # Usar database_path
            raise ConnectionError("Caminho do banco de dados Firebird não fornecido na configuração.")
        if not user_val or not str(user_val).strip():
            raise ConnectionError("Nome de usuário Firebird não fornecido ou vazio após processamento da configuração.")
        if not password_val or not str(password_val).strip():
            raise ConnectionError("Senha Firebird não fornecida ou vazia após processamento da configuração. Verifique a criptografia.")
        
        final_user = str(user_val).strip()
        final_password = str(password_val).strip()

        # Construa a string completa de conexão esperada pelo Firebird (DSN implícito no parâmetro 'database')
        # Formato: "host/port:path/to/database.fdb" ou "host/port:alias_do_banco"
        firebird_dsn = f"{host}/{port}:{database_path}"

        try:
            # Chame fdb_mod.connect() e passe o DSN completo no parâmetro 'database' (ou 'dsn' se sua versão suportar)
            # A documentação do firebird-driver (versão 0.9.x e anteriores) geralmente usa 'database'
            self.connection = fdb_mod.connect(
                database=firebird_dsn, 
                user=final_user,
                password=final_password,
                charset=charset,
                **{k: v for k, v in self.config.items() if k not in ['host', 'port', 'database', 'user', 'password', 'charset', 'db_type']}
            )
            logger.info("Conectado ao banco de dados Firebird.")
            return self.connection
        except Exception as e:
            logger.error(f"Erro de conexão Firebird: {e}")
            raise ConnectionError(f"Erro ao conectar ao Firebird: {str(e)}")

    def disconnect(self):
        if self.connection is not None:
            try:
                self.connection.close() # type: ignore [attr-defined]
                self.connection = None
                logger.info("Desconectado do banco de dados Firebird.")
            except Exception as e:
                logger.error(f"Erro ao desconectar do Firebird: {e}")

    def _get_cursor(self) -> Any:
        """Retorna um cursor para Firebird."""
        if self.connection is None:
            raise ConnectionError("Nenhuma conexão Firebird para obter cursor.")
        # firebird-driver retorna tuplas por padrão. Se precisar de DictCursor, faça:
        # return self.connection.cursor(cursor_factory=fdb_mod.DictCursor) # type: ignore [attr-defined]
        return self.connection.cursor() # type: ignore [attr-defined]

    def _map_firebird_type(self, field_type: int, field_sub_type: int) -> str:
        """Mapeia tipos de dados Firebird para string genérica."""
        type_map = {
            7: 'SMALLINT', 8: 'INTEGER', 10: 'FLOAT', 12: 'DATE', 13: 'TIME',
            14: 'CHAR', 16: 'BIGINT', 27: 'DOUBLE PRECISION', 35: 'TIMESTAMP',
            37: 'VARCHAR', 261: 'BLOB'
        }
        if field_type == 23:
            return 'BOOLEAN'
        if field_type == 7 and field_sub_type == 1:
            return 'BOOLEAN' # Específico para SMALLINT com SUB_TYPE 1 para BOOLEAN em algumas configs
        return type_map.get(field_type, 'UNKNOWN')

    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """Implementação específica para Firebird."""
        columns: List[ColumnMetadata] = []
        primary_keys: List[str] = []
        indexes: Dict[str, IndexMetadata] = {}

        query_columns = """
            SELECT rf.RDB$FIELD_NAME,
                    f.RDB$FIELD_TYPE,
                    f.RDB$FIELD_LENGTH,
                    f.RDB$FIELD_PRECISION,
                    f.RDB$FIELD_SCALE,
                    rf.RDB$NULL_FLAG,
                    rf.RDB$DEFAULT_SOURCE,
                    rf.RDB$DESCRIPTION,
                    f.RDB$FIELD_SUB_TYPE
            FROM RDB$RELATION_FIELDS rf
                JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
            WHERE rf.RDB$RELATION_NAME = ?
            ORDER BY rf.RDB$FIELD_POSITION;
            """
        results_columns = self.execute_query(query_columns, (table_name.upper(),))

        if not results_columns:
            raise ValueError(f"Tabela '{table_name.upper()}' não encontrada ou sem colunas acessíveis no Firebird.")

        for row in results_columns:
            row_tuple = cast(Tuple[Any, ...], row)
            
            col_name = str(row_tuple[0]).strip()
            field_type = int(row_tuple[1])
            field_length = _safe_int_conversion(row_tuple[2])
            field_precision = _safe_int_conversion(row_tuple[3])
            field_scale = _safe_int_conversion(row_tuple[4])
            is_nullable = bool(row_tuple[5] == 0)
            default_value = str(row_tuple[6]).strip() if row_tuple[6] else None
            comment = str(row_tuple[7]).strip() if row_tuple[7] else None
            field_sub_type = int(row_tuple[8]) if row_tuple[8] is not None else 0

            col_type_str = self._map_firebird_type(field_type, field_sub_type)

            num_precision = field_precision
            num_scale = field_scale
            if num_scale is not None:
                num_scale = -num_scale # Firebird stores negative scale for decimal places

            columns.append(ColumnMetadata(
                name=col_name,
                type=col_type_str,
                is_nullable=is_nullable,
                default_value=default_value,
                max_length=field_length,
                numeric_precision=num_precision,
                numeric_scale=num_scale,
                comment=comment
            ))

        query_pk = """
            SELECT s.RDB$FIELD_NAME
            FROM RDB$RELATION_CONSTRAINTS rc
                JOIN RDB$INDEX_SEGMENTS s ON rc.RDB$INDEX_NAME = s.RDB$INDEX_NAME
            WHERE rc.RDB$RELATION_NAME = ?
              AND rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY';
            """
        results_pk = self.execute_query(query_pk, (table_name.upper(),))
        primary_keys = [str(row[0]).strip() for row in results_pk if isinstance(row, tuple)]

        query_fk = """
            SELECT rc.RDB$CONSTRAINT_NAME         AS name,
                   TRIM(s.RDB$FIELD_NAME)         AS column_name,
                   TRIM(refc.RDB$RELATION_NAME) AS referenced_table_name,
                   TRIM(refs.RDB$FIELD_NAME)    AS referenced_column_name,
                   ref.RDB$UPDATE_RULE          AS on_update_raw,
                   ref.RDB$DELETE_RULE          AS on_delete_raw
            FROM RDB$RELATION_CONSTRAINTS rc
                JOIN RDB$INDICES i ON rc.RDB$INDEX_NAME = i.RDB$INDEX_NAME
                JOIN RDB$INDEX_SEGMENTS s ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME
                LEFT JOIN RDB$REF_CONSTRAINTS ref ON rc.RDB$CONSTRAINT_NAME = ref.RDB$CONSTRAINT_NAME
                LEFT JOIN RDB$RELATION_CONSTRAINTS refc ON ref.RDB$CONST_NAME_UQ = refc.RDB$CONSTRAINT_NAME
                LEFT JOIN RDB$INDICES refi ON refc.RDB$INDEX_NAME = refi.RDB$INDEX_NAME
                LEFT JOIN RDB$INDEX_SEGMENTS refs ON refi.RDB$INDEX_NAME = refs.RDB$INDEX_NAME
            WHERE rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
              AND rc.RDB$RELATION_NAME = ?
            ORDER BY rc.RDB$CONSTRAINT_NAME, s.RDB$FIELD_POSITION;
            """
        results_fk = self.execute_query(query_fk, (table_name.upper(),))

        foreign_keys_dict: Dict[str, ForeignKeyMetadata] = {}
        fk_action_map = { # Mapeamento das regras de ação de FK do Firebird
            'CASCADE': 'CASCADE',
            'SET NULL': 'SET NULL',
            'SET DEFAULT': 'SET DEFAULT',
            'NO ACTION': 'NO ACTION',
            'RESTRICT': 'RESTRICT'
        }
        for row in results_fk:
            row_tuple = cast(Tuple[Any, ...], row)
            fk_name_raw = row_tuple[0]
            fk_name = str(fk_name_raw).strip() if fk_name_raw else "UNNAMED_FK"
            
            local_col = str(row_tuple[1]).strip()
            referenced_table = str(row_tuple[2]).strip()
            referenced_column = str(row_tuple[3]).strip()
            on_update_raw_val = str(row_tuple[4]).strip() if row_tuple[4] else ''
            on_delete_raw_val = str(row_tuple[5]).strip() if row_tuple[5] else ''

            on_update = fk_action_map.get(on_update_raw_val, 'UNKNOWN')
            on_delete = fk_action_map.get(on_delete_raw_val, 'UNKNOWN')

            if fk_name not in foreign_keys_dict:
                # Cria uma nova FK. Se houver múltiplas colunas para a mesma FK,
                # isso exigiria uma lista de colunas na ForeignKeyMetadata,
                # mas o modelo atual só tem `column_name`.
                # Para Firebird, uma constraint de FK pode envolver múltiplas colunas,
                # e a consulta retorna uma linha por segmento (coluna).
                # Para simplificar aqui, assumimos que uma FK tem uma única coluna para este mapeamento,
                # ou que a primeira coluna segmentada é a principal.
                foreign_keys_dict[fk_name] = ForeignKeyMetadata(
                    name=fk_name,
                    column_name=local_col, # Isto pode precisar ser ajustado se FKs multicampos forem importantes
                    referenced_table_name=referenced_table,
                    referenced_column_name=referenced_column, # Isto pode precisar ser ajustado se FKs multicampos forem importantes
                    on_update=on_update,
                    on_delete=on_delete
                )

        foreign_keys = list(foreign_keys_dict.values())

        query_indexes = """
            SELECT i.RDB$INDEX_NAME,
                    s.RDB$FIELD_NAME,
                    i.RDB$UNIQUE_FLAG,
                    rc.RDB$CONSTRAINT_TYPE
            FROM RDB$INDICES i
                JOIN RDB$INDEX_SEGMENTS s ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME
                LEFT JOIN RDB$RELATION_CONSTRAINTS rc ON i.RDB$INDEX_NAME = rc.RDB$INDEX_NAME
            WHERE i.RDB$RELATION_NAME = ?
            ORDER BY i.RDB$INDEX_NAME, s.RDB$FIELD_POSITION;
            """
        results_indexes = self.execute_query(query_indexes, (table_name.upper(),))

        for row in results_indexes:
            row_tuple = cast(Tuple[Any, ...], row)
            idx_name = str(row_tuple[0]).strip()
            col_name = str(row_tuple[1]).strip()
            is_unique = bool(row_tuple[2] == 1)
            constraint_type = str(row_tuple[3]).strip() if row_tuple[3] else None
            is_primary = (constraint_type == 'PRIMARY KEY')

            if idx_name not in indexes:
                indexes[idx_name] = IndexMetadata(
                    name=idx_name,
                    columns=[],
                    is_unique=is_unique,
                    is_primary=is_primary,
                    type='B-tree' # Firebird usa principalmente B-tree
                )
            indexes[idx_name].columns.append(col_name)

        row_count = None
        size_bytes = None
        try:
            count_query = f"SELECT COUNT(*) FROM \"{table_name.upper()}\""
            count_result = self.execute_query(count_query)
            if count_result and count_result[0] and isinstance(count_result[0], tuple):
                row_count = _safe_int_conversion(count_result[0][0])
        except Exception as e:
            logger.warning(f"Não foi possível obter o row_count para {table_name.upper()}: {e}")

        table_comment = None
        try:
            comment_query = f"""
                SELECT RDB$DESCRIPTION
                FROM RDB$RELATIONS
                WHERE RDB$RELATION_NAME = ?;
            """
            comment_result = self.execute_query(comment_query, (table_name.upper(),))
            if comment_result and comment_result[0] and isinstance(comment_result[0], tuple):
                table_comment = str(comment_result[0][0]).strip() if comment_result[0][0] else None
        except Exception as e:
            logger.warning(f"Não foi possível obter o comentário da tabela para {table_name.upper()}: {e}")


        return TableMetadata(
            name=table_name.upper(),
            columns=columns,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            indexes=list(indexes.values()),
            size_bytes=size_bytes, # Firebird não tem um INFORMATION_SCHEMA fácil para tamanho
            row_count=row_count,
            comment=table_comment
        )

# --- MySQLConnector e PostgreSQLConnector (permanecem inalterados, apenas repetidos para contexto) ---

class MySQLConnector(BaseDBConnector):
    """Conector para MySQL Database."""
    
    def get_placeholder(self) -> str:
        return "%s"
    
    def connect(self) -> Any:
        if self.connection is not None:
            logger.info("Conexão MySQL já estabelecida.")
            return self.connection

        if mysql_connector_mod is None or not mysql_connector_available:
            raise ImportError("MySQL driver não está instalado ou não pôde ser carregado.")

        # Acessa os dados de conexão via self.config, que é populado pelo __init__ da classe base
        host = self.config.get('host')
        database = self.config.get('database')
        user_val = self.config.get('user')
        password_val = self.config.get('password')
        port = int(self.config.get('port', 3306)) # Porta extraída explicitamente

        if not host:
            raise ConnectionError("Host MySQL não fornecido na configuração.")
        if not database:
            raise ConnectionError("Nome do banco de dados MySQL não fornecido na configuração.")
        if not user_val or not str(user_val).strip():
            raise ConnectionError("Nome de usuário MySQL não fornecido ou vazio após processamento da configuração.")
        if not password_val or not str(password_val).strip():
            raise ConnectionError("Senha MySQL não fornecida ou vazia após processamento da configuração. Verifique a criptografia.")
        
        final_user = str(user_val).strip()
        final_password = str(password_val).strip()
        # --- FIM VERIFICAÇÃO DE CREDENCIAIS ---

        try:
            self.connection = mysql_connector_mod.connect( # type: ignore [attr-defined]
                host=host,
                database=database,
                user=final_user,
                password=final_password,
                port=port, 
                # Removido 'host', 'database', 'user', 'password', 'port' do filtro,
                # pois já estão sendo passados acima explicitamente.
                **{k: v for k, v in self.config.items() if k not in ['host', 'database', 'user', 'password', 'port', 'db_type']}
            )
            if hasattr(self.connection, 'autocommit'):
                self.connection.autocommit = False # type: ignore [attr-defined]
            logger.info("Conectado ao banco de dados MySQL com autocommit=False.")
            return self.connection
        except Exception as e:
            logger.error(f"Erro de conexão MySQL: {e}")
            raise ConnectionError(f"Erro ao conectar ao MySQL: {str(e)}")

    def disconnect(self):
        if self.connection is not None:
            try:
                if hasattr(self.connection, 'is_connected') and not self.connection.is_connected(): # type: ignore [attr-defined]
                    logger.warning("Conexão MySQL já fechada ou inválida durante a desconexão.")
                    return

                self.connection.close() # type: ignore [attr-defined]
                self.connection = None
                logger.info("Desconectado do banco de dados MySQL.")
            except Exception as e:
                logger.error(f"Erro ao desconectar do MySQL: {e}")

    def _get_cursor(self) -> Any:
        """Retorna um cursor de dicionário para MySQL."""
        if self.connection is None:
            raise ConnectionError("Nenhuma conexão MySQL para obter cursor.")
        return self.connection.cursor(dictionary=True) # type: ignore [attr-defined]

    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """Implementação específica para MySQL."""
        table_info_query = """
            SELECT
                data_length + index_length AS total_bytes,
                table_rows,
                table_comment
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s;
        """
        table_info_results = self.execute_query(table_info_query, (self.config['database'], table_name))

        table_info: Dict[str, Any] = {'total_bytes': None, 'row_count': None, 'table_comment': None}
        if table_info_results and isinstance(table_info_results[0], dict):
            first_row = table_info_results[0]
            table_info['total_bytes'] = _safe_int_conversion(first_row.get('total_bytes'))
            table_info['row_count'] = _safe_int_conversion(first_row.get('table_rows'))
            table_info['table_comment'] = str(first_row.get('table_comment')) if first_row.get('table_comment') is not None else None

        columns_query = """
            SELECT
                COLUMN_NAME,
                COLUMN_TYPE AS DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                COLUMN_KEY,
                EXTRA,
                COLUMN_COMMENT
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ORDINAL_POSITION;
        """
        columns_results = self.execute_query(columns_query, (self.config['database'], table_name))

        if not columns_results:
            raise ValueError(f"Tabela '{table_name}' não encontrada ou sem colunas acessíveis no MySQL.")


        columns = []
        primary_keys = []
        for row in columns_results:
            if not isinstance(row, dict):
                raise TypeError("Os resultados da consulta de colunas MySQL não foram dicionários.")

            col_name = str(row['COLUMN_NAME'])
            col_type_str = str(row['DATA_TYPE'])
            is_nullable = str(row['IS_NULLABLE']) == 'YES'
            default_value = str(row['COLUMN_DEFAULT']) if row['COLUMN_DEFAULT'] is not None else None
            max_length = _safe_int_conversion(row['CHARACTER_MAXIMUM_LENGTH'])

            num_precision = _safe_int_conversion(row['NUMERIC_PRECISION'])
            num_scale = _safe_int_conversion(row['NUMERIC_SCALE'])

            comment = str(row['COLUMN_COMMENT']) if row['COLUMN_COMMENT'] is not None else None
            is_pk = 'PRI' in str(row.get('COLUMN_KEY', ''))
            is_unique_col = 'UNI' in str(row.get('COLUMN_KEY', '')) or ('PRIMARY' in str(row.get('EXTRA', '')) if row.get('EXTRA') else False)

            columns.append(ColumnMetadata(
                name=col_name,
                type=col_type_str,
                is_nullable=is_nullable,
                default_value=default_value,
                max_length=max_length,
                numeric_precision=num_precision,
                numeric_scale=num_scale,
                comment=comment,
                is_primary_key=is_pk,
                is_unique=is_unique_col
            ))
            if is_pk:
                primary_keys.append(col_name)

        fk_query = """
            SELECT
                kcu.CONSTRAINT_NAME,
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_NAME,
                kcu.REFERENCED_COLUMN_NAME,
                rc.UPDATE_RULE,
                rc.DELETE_RULE
            FROM information_schema.KEY_COLUMN_USAGE kcu
            JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
                AND kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            WHERE kcu.REFERENCED_TABLE_SCHEMA = %s
              AND kcu.TABLE_NAME = %s
              AND kcu.REFERENCED_TABLE_NAME IS NOT NULL;
        """
        fk_results = self.execute_query(fk_query, (self.config['database'], table_name))

        foreign_keys = []
        for row in fk_results:
            if not isinstance(row, dict):
                raise TypeError("Os resultados da consulta de chaves estrangeiras MySQL não foram dicionários.")
            foreign_keys.append(ForeignKeyMetadata(
                name=str(row['CONSTRAINT_NAME']),
                column_name=str(row['COLUMN_NAME']),
                referenced_table_name=str(row['REFERENCED_TABLE_NAME']),
                referenced_column_name=str(row['REFERENCED_COLUMN_NAME']),
                on_update=str(row['UPDATE_RULE']),
                on_delete=str(row['DELETE_RULE'])
            ))

        indexes_query = """
            SELECT
                INDEX_NAME,
                COLUMN_NAME,
                NON_UNIQUE,
                INDEX_TYPE
            FROM information_schema.statistics
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY INDEX_NAME, SEQ_IN_INDEX;
        """
        indexes_results = self.execute_query(indexes_query, (self.config['database'], table_name))

        indexes: Dict[str, IndexMetadata] = {}
        for row in indexes_results:
            if not isinstance(row, dict):
                raise TypeError("Os resultados da consulta de índices MySQL não foram dicionários.")
            idx_name = str(row['INDEX_NAME'])
            col_name = str(row['COLUMN_NAME'])
            is_unique = bool(row['NON_UNIQUE'] == 0) # 0 means unique
            idx_type = str(row['INDEX_TYPE'])

            if idx_name not in indexes:
                indexes[idx_name] = IndexMetadata(
                    name=idx_name,
                    columns=[],
                    is_unique=is_unique,
                    is_primary=(idx_name == 'PRIMARY'), # Primary key index is named 'PRIMARY'
                    type=idx_type
                )
            indexes[idx_name].columns.append(col_name)

        return TableMetadata(
            name=table_name,
            columns=columns,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            indexes=list(indexes.values()),
            size_bytes=table_info['total_bytes'],
            row_count=table_info['row_count'],
            comment=table_info['table_comment']
        )


class PostgreSQLConnector(BaseDBConnector):
    """Conector para PostgreSQL Database."""

    def get_placeholder(self) -> str:
        return "%s"

    def connect(self) -> Any:
        if self.connection is not None:
            logger.info("Conexão PostgreSQL já estabelecida.")
            return self.connection

        if psycopg2_mod is None or not psycopg2_available:
            raise ImportError("PostgreSQL driver não está instalado ou não pôde ser carregado.")

        # Acessa os dados de conexão via self.config, que é populado pelo __init__ da classe base
        host = self.config.get('host')
        dbname = self.config.get('database')
        user_val = self.config.get('user')
        password_val = self.config.get('password')
        port = int(self.config.get('port', 5432))

        if not host:
            raise ConnectionError("Host PostgreSQL não fornecido na configuração.")
        if not dbname:
            raise ConnectionError("Nome do banco de dados PostgreSQL não fornecido na configuração.")
        if not user_val or not str(user_val).strip():
            raise ConnectionError("Nome de usuário PostgreSQL não fornecido ou vazio após processamento da configuração.")
        if not password_val or not str(password_val).strip():
            raise ConnectionError("Senha PostgreSQL não fornecida ou vazia após processamento da configuração. Verifique a criptografia.")
        
        final_user = str(user_val).strip()
        final_password = str(password_val).strip()
        # --- FIM VERIFICAÇÃO DE CREDENCIAIS ---

        try:
            self.connection = psycopg2_mod.connect( # type: ignore [attr-defined]
                host=host,
                dbname=dbname,
                user=final_user,
                password=final_password,
                port=port,
                **{k: v for k, v in self.config.items() if k not in ['host', 'database', 'user', 'password', 'port', 'db_type']}
            )
            self.connection.autocommit = False # type: ignore [attr-defined]
            logger.info("Conectado ao banco de dados PostgreSQL com autocommit=False.")
            return self.connection
        except Exception as e:
            logger.error(f"Erro de conexão PostgreSQL: {e}")
            raise ConnectionError(f"Erro ao conectar ao PostgreSQL: {str(e)}")

    def disconnect(self):
        if self.connection is not None:
            try:
                if self.connection.closed: # type: ignore [attr-defined]
                    logger.warning("Conexão PostgreSQL já fechada ou inválida durante a desconexão.")
                    return

                self.connection.close() # type: ignore [attr-defined]
                self.connection = None
                logger.info("Desconectado do banco de dados PostgreSQL.")
            except Exception as e:
                logger.error(f"Erro ao desconectar do PostgreSQL: {e}")

    def _get_cursor(self) -> Any:
        """Retorna um cursor de dicionário para PostgreSQL."""
        if self.connection is None:
            raise ConnectionError("Nenhuma conexão PostgreSQL para obter cursor.")
        if psycopg2_mod is None or psycopg2_extras_mod is None or not psycopg2_extras_available:
            raise ImportError("psycopg2.extras (para DictCursor) não está disponível ou não pôde ser carregado.")
        return self.connection.cursor(cursor_factory=psycopg2_extras_mod.DictCursor) # type: ignore [attr-defined]

    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """Implementação específica para PostgreSQL."""
        schema = str(self.config.get('schema', 'public'))

        exact_table_name = self._get_exact_table_name(table_name, schema)
        if exact_table_name is None:
            raise ValueError(f"Tabela '{table_name}' não encontrada no esquema '{schema}'.")

        table_info = self._get_table_info(exact_table_name, schema)
        columns = self._get_columns(exact_table_name, schema)
        primary_keys = self._get_primary_keys(exact_table_name)
        foreign_keys = self._get_foreign_keys(exact_table_name)
        indexes = self._get_indexes(exact_table_name)

        return TableMetadata(
            name=exact_table_name,
            columns=columns,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            indexes=list(indexes.values()),
            size_bytes=table_info['total_bytes'],
            row_count=table_info['row_count'],
            comment=table_info['table_comment']
        )

    def _get_exact_table_name(self, table_name: str, schema: str) -> Optional[str]:
        """Obtém o nome exato da tabela com o case correto."""
        query = """
                    SELECT relname
                    FROM pg_class
                    WHERE relname = %s
                      AND relkind = 'r'
                      AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s);
                    """
        result = self.execute_query(query, (table_name, schema))
        if result and isinstance(result[0], dict):
            return str(result[0]['relname'])
        return None

    def _get_table_info(self, table_name: str, schema: str) -> Dict[str, Any]:
        """Obtém informações básicas da tabela."""
        query = """
                    SELECT pg_total_relation_size(CAST(%s AS regclass))   AS total_bytes,
                           obj_description(CAST(%s AS regclass), 'pg_class') AS table_comment,
                           reltuples                                  AS row_count
                    FROM pg_class
                    WHERE relname = %s
                      AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s);
                    """
        result = self.execute_query(query, (table_name, table_name, table_name, schema))
        if result and isinstance(result[0], dict):
            first_row = result[0]
            return {
                'total_bytes': _safe_int_conversion(first_row.get('total_bytes')),
                'table_comment': str(first_row.get('table_comment')) if first_row.get('table_comment') is not None else None,
                'row_count': _safe_int_conversion(first_row.get('reltuples')) # PostgreSQL usa reltuples para contagem de linhas
            }
        return {'total_bytes': None, 'table_comment': None, 'row_count': None}

    def _get_columns(self, table_name: str, schema: str) -> List[ColumnMetadata]:
        """Obtém metadados das colunas."""
        query = """
                    SELECT c.column_name,
                           c.data_type,
                           c.is_nullable,
                           c.column_default,
                           c.character_maximum_length,
                           c.numeric_precision,
                           c.numeric_scale,
                           pg_catalog.col_description(cc.oid, c.ordinal_position) as column_comment,
                           EXISTS (SELECT 1
                                   FROM pg_index i
                                        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
                                   WHERE i.indrelid = CAST(%s AS regclass) AND i.indisprimary AND a.attname = c.column_name) AS is_primary_key,
                           EXISTS (SELECT 1
                                   FROM pg_index i
                                        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
                                   WHERE i.indrelid = CAST(%s AS regclass) AND i.indisunique AND a.attname = c.column_name)   AS is_unique
                    FROM information_schema.columns c
                         JOIN pg_class cc ON cc.relname = c.table_name
                         JOIN pg_namespace n ON n.oid = cc.relnamespace
                    WHERE c.table_name = %s
                      AND n.nspname = %s
                    ORDER BY c.ordinal_position;
                    """
        results = self.execute_query(query, (table_name, table_name, table_name, schema))

        if not results:
            raise ValueError(f"Nenhuma coluna encontrada para a tabela '{table_name}' no esquema '{schema}'. A tabela pode não existir ou o usuário não tem permissões.")


        columns = []
        for row in results:
            if not isinstance(row, dict):
                raise TypeError("Os resultados da consulta de colunas PostgreSQL não foram dicionários.")
            col_name = str(row['column_name'])
            col_type_str = str(row['data_type'])
            is_nullable = str(row['is_nullable']) == 'YES'
            default_value = str(row['column_default']) if row['column_default'] is not None else None
            max_length = _safe_int_conversion(row['character_maximum_length'])

            num_precision = _safe_int_conversion(row['numeric_precision'])
            num_scale = _safe_int_conversion(row['numeric_scale'])

            comment = str(row['column_comment']) if row['column_comment'] is not None else None
            is_primary_key = bool(row['is_primary_key'])
            is_unique = bool(row['is_unique'])

            columns.append(ColumnMetadata(
                name=col_name,
                type=col_type_str,
                is_nullable=is_nullable,
                default_value=default_value,
                max_length=max_length,
                numeric_precision=num_precision,
                numeric_scale=num_scale,
                comment=comment,
                is_primary_key=is_primary_key,
                is_unique=is_unique
            ))
        return columns

    def _get_primary_keys(self, table_name: str) -> List[str]:
        """Obtém as chaves primárias da tabela."""
        query = """
                    SELECT a.attname
                    FROM pg_index i
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
                    WHERE i.indrelid = CAST(%s AS regclass) AND i.indisprimary;
                    """
        results = self.execute_query(query, (table_name,))
        return [str(row['attname']) for row in results if isinstance(row, dict)]

    def _get_foreign_keys(self, table_name: str) -> List[ForeignKeyMetadata]:
        """Obtém as chaves estrangeiras."""
        query = """
                    SELECT con.conname,
                           att.attname   AS column_name,
                           cl2.relname   AS referenced_table_name,
                           att2.attname  AS referenced_column_name,
                           con.confupdtype,
                           con.confdeltype
                    FROM pg_constraint con
                         JOIN pg_class cl ON con.conrelid = cl.oid
                         JOIN pg_class cl2 ON con.confrelid = cl2.oid
                         JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY (con.conkey)
                         JOIN pg_attribute att2 ON att2.attrelid = con.confrelid AND att2.attnum = ANY (con.confkey)
                    WHERE con.contype = 'f'
                      AND cl.relname = %s;
                    """
        results = self.execute_query(query, (table_name,))

        fk_actions = {
            'a': 'NO ACTION',
            'r': 'RESTRICT',
            'c': 'CASCADE',
            'n': 'SET NULL',
            'd': 'SET DEFAULT'
        }

        foreign_keys = []
        for row in results:
            if not isinstance(row, dict):
                raise TypeError("Os resultados da consulta de chaves estrangeiras PostgreSQL não foram dicionários.")
            on_update = fk_actions.get(chr(row['confupdtype']), 'UNKNOWN')
            on_delete = fk_actions.get(chr(row['confdeltype']), 'UNKNOWN')

            foreign_keys.append(ForeignKeyMetadata(
                name=str(row['conname']),
                column_name=str(row['column_name']),
                referenced_table_name=str(row['referenced_table_name']),
                referenced_column_name=str(row['referenced_column_name']),
                on_update=on_update,
                on_delete=on_delete
            ))
        return foreign_keys

    def _get_indexes(self, table_name: str) -> Dict[str, IndexMetadata]:
        """Obtém os índices da tabela."""
        query = """
                    SELECT i.relname   AS index_name,
                           a.attname   AS column_name,
                           ix.indisunique  AS is_unique,
                           ix.indisprimary AS is_primary,
                           am.amname   AS index_type
                    FROM pg_index ix
                         JOIN pg_class i ON i.oid = ix.indexrelid
                         JOIN pg_class t ON t.oid = ix.indrelid
                         JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (ix.indkey)
                         JOIN pg_am am ON am.oid = i.relam
                    WHERE t.relname = %s
                    ORDER BY i.relname, array_position(ix.indkey, a.attnum);
                    """
        results = self.execute_query(query, (table_name,))

        indexes: Dict[str, IndexMetadata] = {}
        for row in results:
            if not isinstance(row, dict):
                raise TypeError("Os resultados da consulta de índices PostgreSQL não foram dicionários.")
            idx_name = str(row['index_name'])
            col_name = str(row['column_name'])
            is_unique = bool(row['is_unique'])
            is_primary = bool(row['is_primary'])
            idx_type = str(row['index_type'])

            if idx_name not in indexes:
                indexes[idx_name] = IndexMetadata(
                    name=idx_name,
                    columns=[],
                    is_unique=is_unique,
                    is_primary=is_primary,
                    type=idx_type
                )
            indexes[idx_name].columns.append(col_name)
        return indexes

# --- Gerenciador de Conexão com Carregamento de Chave Secreta ---

class DBConnectionManager:
    """
    Gerencia a leitura de configurações de conexão de arquivos JSON,
    descriptografia e instanciação do conector de banco de dados apropriado.
    """
    _CONNECTOR_MAP = {
        "firebird": FirebirdConnector,
        "mysql": MySQLConnector,
        "postgresql": PostgreSQLConnector,
    }
    
    def __init__(self, config_file_path: Union[str, Path]):
        self.config_file_path = Path(config_file_path)
        self.raw_config: Dict[str, Any] = {}
        self.decrypted_config: Dict[str, Any] = {} # Será preenchido após carregamento e descriptografia
        self._load_config()
        self._decrypt_config() # Este método agora garante que 'decrypted_config' contenha valores prontos

    def _load_config(self):
        """Carrega a configuração do arquivo JSON."""
        if not self.config_file_path.exists():
            raise ConfigError(f"Arquivo de configuração não encontrado: {self.config_file_path}")
        if not self.config_file_path.is_file():
            raise ConfigError(f"Caminho fornecido não é um arquivo: {self.config_file_path}")

        # Verifica permissões do arquivo para segurança (opcional, mas boa prática)
        file_stat = self.config_file_path.stat()
        # st_mode & (stat.S_IRWXG | stat.S_IRWXO) verifica se qualquer permissão de grupo ou outros está ativa
        if os.name != 'nt' and bool(file_stat.st_mode & (stat.S_IRWXG | stat.S_IRWXO)): 
            logger.warning(f"⚠️ Aviso: Permissões amplas detectadas para {self.config_file_path}. Recomenda-se 0o600.")

        try:
            with open(self.config_file_path, 'r', encoding='utf-8') as f:
                self.raw_config = json.load(f)
            logger.info(f"Configuração carregada de {self.config_file_path}")
        except json.JSONDecodeError as e:
            raise ConfigError(f"Erro ao analisar o JSON do arquivo de configuração: {e}")
        except Exception as e:
            raise ConfigError(f"Erro ao ler o arquivo de configuração: {e}")

    def _decrypt_config(self):
        """
        Descriptografa credenciais sensíveis. Prioriza a obtenção da chave:
        1. Arquivo 'secret.key' no mesmo diretório do config.json (ou caminho explícito)
        2. Variável de ambiente 'DB_ENCRYPTION_KEY'
        3. Chave embutida no config.json (NÃO RECOMENDADO PARA PRODUÇÃO)

        Se um valor criptografado não puder ser descriptografado, ele será definido como None.
        """
        # Inicializa decrypted_config com uma cópia profunda dos dados brutos.
        # Isso garante que todos os campos (mesmo os não sensíveis ou não criptografados)
        # sejam transferidos, e que as modificações sejam isoladas.
        self.decrypted_config = copy.deepcopy(self.raw_config)
        encryption_key_b64: Optional[str] = None

        # Tenta obter o caminho do arquivo de chave de criptografia do config_file_path
        key_file_path_from_config = self.raw_config.get('key_file_path')
        secret_key_path_candidate = Path(key_file_path_from_config) if key_file_path_from_config else self.config_file_path.parent / "secret.key"
        
        # 1. Tenta carregar a chave do arquivo secret.key (candidato)
        if secret_key_path_candidate.exists() and secret_key_path_candidate.is_file():
            try:
                with open(secret_key_path_candidate, 'r', encoding='utf-8') as f:
                    encryption_key_b64 = f.read().strip()
                logger.info(f"Chave de criptografia carregada de '{secret_key_path_candidate}'.")
            except Exception as e:
                raise SecurityError(f"Erro ao ler o arquivo secret.key em '{secret_key_path_candidate}': {e}")
        
        # 2. Se não encontrou no arquivo, tenta da variável de ambiente
        if not encryption_key_b64:
            encryption_key_b64 = os.getenv("DB_ENCRYPTION_KEY")
            if encryption_key_b64:
                logger.info("Chave de criptografia obtida de variável de ambiente 'DB_ENCRYPTION_KEY'.")

        # 3. Se ainda não encontrou, tenta do próprio arquivo de configuração (fallback)
        if not encryption_key_b64:
            encryption_key_b64 = self.raw_config.get('encryption_key')
            if encryption_key_b64:
                 logger.warning("Chave de criptografia obtida do arquivo de configuração (não recomendado para produção).")

        if encryption_key_b64:
            try:
                f = Fernet(encryption_key_b64.encode())
                
                # Campos sensíveis a serem verificados e potencialmente descriptografados
                sensitive_keys = ['password', 'user', 'database', 'host'] 
                
                for key in sensitive_keys:
                    current_value = self.raw_config.get(key) # Obtém o valor original do raw_config
                    
                    if isinstance(current_value, str) and current_value.startswith('ENC:'):
                        try:
                            decoded_value = f.decrypt(current_value[4:].encode()).decode()
                            self.decrypted_config[key] = decoded_value # Armazena o valor descriptografado
                            logger.debug(f"Campo '{key}' descriptografado com sucesso.")
                        except InvalidToken:
                            logger.error(f"Token de criptografia inválido para o campo '{key}' ('{current_value[:20]}...'). Definindo como None.")
                            self.decrypted_config[key] = None # Define explicitamente como None em caso de falha de descriptografia
                        except Exception as e:
                            logger.error(f"Erro inesperado ao descriptografar campo '{key}' ('{current_value[:20]}...'): {e}. Definindo como None.")
                            self.decrypted_config[key] = None # Define explicitamente como None em caso de erro inesperado
                    # Se o valor não começa com 'ENC:', ele já foi copiado como plaintext por deepcopy,
                    # então não é necessário fazer nada para esses campos aqui.
            except Exception as e:
                raise SecurityError(f"Erro ao inicializar Fernet com a chave fornecida: {e}. Verifique se a chave é válida ou o formato Base64.")
        else:
            logger.warning("Nenhuma chave de criptografia fornecida. Credenciais não serão descriptografadas. Garanta que não há dados sensíveis em texto simples se esta não for a intenção.")

    def get_connector(self) -> IDBConnector:
        """
        Retorna uma instância do conector de banco de dados apropriado
        com base na configuração carregada.
        """
        db_type = self.decrypted_config.get('db_type', '').lower()
        connector_class = self._CONNECTOR_MAP.get(db_type)

        if not connector_class:
            raise ConfigError(f"Tipo de banco de dados '{db_type}' não suportado ou configurado incorretamente.")
        
        # Verifica se o driver está disponível para o tipo de banco de dados
        # Essas verificações são importantes para garantir que as dependências estão instaladas
        if db_type == "firebird" and not fdb_available:
            raise ImportError("Driver Firebird (firebird.driver) não está disponível. Por favor, instale-o.")
        if db_type == "mysql" and not mysql_connector_available:
            raise ImportError("Driver MySQL (mysql-connector-python) não está disponível. Por favor, instale-o.")
        if db_type == "postgresql" and not psycopg2_available:
            raise ImportError("Driver PostgreSQL (psycopg2-binary) não está disponível. Por favor, instale-o.")

        return connector_class(self.decrypted_config)