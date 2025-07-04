import psycopg2 # type: ignore
import logging
from typing import Optional, Any, List, Tuple, Dict, Union
from connectorDB import IDBConnector, DatabaseError

logger = logging.getLogger(__name__)

class PostgreSQLConnector(IDBConnector):
    """
    Implementação da interface IDBConnector para PostgreSQL.
    Utiliza a biblioteca `psycopg2`.
    """
    def __init__(self, host: str, port: int, database: str, user: str, password: str, **kwargs):
        super().__init__(host, port, database, user, password, **kwargs)
        self._connection: Optional[psycopg2.extensions.connection] = None
        self._cursor: Optional[psycopg2.extensions.cursor] = None
        self._placeholder = "%s" # PostgreSQL (psycopg2) usa '%s' como placeholder

    def connect(self):
        """Estabelece a conexão com o banco de dados PostgreSQL."""
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                dbname=self._database,
                user=self._user,
                password=self._password
            )
            # Desativa o autocommit para gerenciar transações manualmente
            self._connection.autocommit = False
            self._cursor = self._connection.cursor()
            logger.info("Conexão com PostgreSQL estabelecida.")
        except psycopg2.Error as e:
            logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
            raise DatabaseError(f"Erro ao conectar ao PostgreSQL: {e}") from e

    def disconnect(self):
        """Fecha a conexão com o banco de dados PostgreSQL."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Conexão com PostgreSQL fechada.")

    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Union[Tuple[Any, ...], Dict[str, Any]]]:
        """Executa uma consulta SELECT no PostgreSQL."""
        if not self._connection or not self._cursor:
            raise DatabaseError("Conexão com PostgreSQL não está ativa.")
        try:
            self._cursor.execute(query, params)
            return self._cursor.fetchall()
        except psycopg2.Error as e:
            self._connection.rollback()
            logger.error(f"Erro ao executar consulta PostgreSQL: {e} - Query: {query} - Params: {params}")
            raise DatabaseError(f"Erro ao executar consulta PostgreSQL: {e}") from e

    def execute_update(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> int:
        """Executa uma consulta de atualização no PostgreSQL."""
        if not self._connection or not self._cursor:
            raise DatabaseError("Conexão com PostgreSQL não está ativa.")
        try:
            self._cursor.execute(query, params)
            return self._cursor.rowcount
        except psycopg2.Error as e:
            self._connection.rollback()
            logger.error(f"Erro ao executar atualização PostgreSQL: {e} - Query: {query} - Params: {params}")
            raise DatabaseError(f"Erro ao executar atualização PostgreSQL: {e}") from e

    def get_last_insert_id(self) -> Optional[Any]:
        """
        Retorna o ID da última linha inserida para PostgreSQL.
        Psycopg2 não tem um atributo lastrowid genérico.
        Para obter o ID de uma inserção, é comum usar `RETURNING id_col` na query INSERT.
        Para manter a simplicidade e a compatibilidade com a interface, retornaremos None.
        Se necessário, a query INSERT pode ser modificada para incluir RETURNING e o resultado lido.
        """
        logger.warning("get_last_insert_id para PostgreSQL requer o uso de 'RETURNING' na query INSERT para obter o ID de forma confiável.")
        return None

    def start_transaction(self):
        """Inicia uma transação no PostgreSQL. Com autocommit=False, não é necessário um BEGIN explícito."""
        if self._connection:
            # Psycopg2, quando autocommit=False, já inicia uma transação implicitamente.
            # Um novo bloco de transação é iniciado após um commit ou rollback.
            logger.info("Iniciando transação PostgreSQL (autocommit=False).")
        else:
            raise DatabaseError("Conexão com PostgreSQL não está ativa para iniciar transação.")

    def commit_transaction(self):
        """Confirma a transação atual no PostgreSQL."""
        if self._connection:
            self._connection.commit()
            logger.info("Transação PostgreSQL confirmada.")
        else:
            raise DatabaseError("Conexão com PostgreSQL não está ativa para confirmar transação.")

    def rollback_transaction(self):
        """Desfaz a transação atual no PostgreSQL."""
        if self._connection:
            self._connection.rollback()
            logger.info("Transação PostgreSQL desfeita.")
        else:
            raise DatabaseError("Conexão com PostgreSQL não está ativa para desfazer transação.")

    def get_placeholder(self) -> str:
        """Retorna o placeholder de parâmetro para PostgreSQL."""
        return self._placeholder