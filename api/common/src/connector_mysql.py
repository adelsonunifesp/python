import mysql.connector # type: ignore
import logging
from typing import Optional, Any, List, Tuple, Dict, Union
from connectorDB import IDBConnector, DatabaseError

logger = logging.getLogger(__name__)

class MySQLConnector(IDBConnector):
    """
    Implementação da interface IDBConnector para MySQL.
    Utiliza a biblioteca `mysql-connector-python`.
    """
    def __init__(self, host: str, port: int, database: str, user: str, password: str, **kwargs):
        super().__init__(host, port, database, user, password, **kwargs)
        self._connection: Optional[mysql.connector.MySQLConnection] = None
        self._cursor: Optional[mysql.connector.cursor.MySQLCursor] = None
        self._placeholder = "%s" # MySQL usa '%s' como placeholder

    def connect(self):
        """Estabelece a conexão com o banco de dados MySQL."""
        try:
            self._connection = mysql.connector.connect(
                host=self._host,
                port=self._port,
                database=self._database,
                user=self._user,
                password=self._password,
                autocommit=False # Desativa autocommit para gerenciamento manual de transações
            )
            self._cursor = self._connection.cursor()
            logger.info("Conexão com MySQL estabelecida.")
        except mysql.connector.Error as e:
            logger.error(f"Erro ao conectar ao MySQL: {e}")
            raise DatabaseError(f"Erro ao conectar ao MySQL: {e}") from e

    def disconnect(self):
        """Fecha a conexão com o banco de dados MySQL."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Conexão com MySQL fechada.")

    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Union[Tuple[Any, ...], Dict[str, Any]]]:
        """Executa uma consulta SELECT no MySQL."""
        if not self._connection or not self._cursor:
            raise DatabaseError("Conexão com MySQL não está ativa.")
        try:
            self._cursor.execute(query, params)
            return self._cursor.fetchall()
        except mysql.connector.Error as e:
            self._connection.rollback()
            logger.error(f"Erro ao executar consulta MySQL: {e} - Query: {query} - Params: {params}")
            raise DatabaseError(f"Erro ao executar consulta MySQL: {e}") from e

    def execute_update(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> int:
        """Executa uma consulta de atualização no MySQL."""
        if not self._connection or not self._cursor:
            raise DatabaseError("Conexão com MySQL não está ativa.")
        try:
            self._cursor.execute(query, params)
            return self._cursor.rowcount
        except mysql.connector.Error as e:
            self._connection.rollback()
            logger.error(f"Erro ao executar atualização MySQL: {e} - Query: {query} - Params: {params}")
            raise DatabaseError(f"Erro ao executar atualização MySQL: {e}") from e

    def get_last_insert_id(self) -> Optional[Any]:
        """Retorna o ID da última linha inserida para MySQL."""
        if self._cursor:
            return self._cursor.lastrowid
        return None

    def start_transaction(self):
        """Inicia uma transação no MySQL. Com autocommit=False, não é necessário um BEGIN explícito."""
        if self._connection:
            # O MySQL Connector/Python gerencia transações automaticamente quando autocommit=False.
            # Um novo 'BEGIN' é iniciado implicitamente após um commit ou rollback.
            # Não há um método `begin()` explícito no driver para chamar.
            logger.info("Iniciando transação MySQL (autocommit=False).")
        else:
            raise DatabaseError("Conexão com MySQL não está ativa para iniciar transação.")

    def commit_transaction(self):
        """Confirma a transação atual no MySQL."""
        if self._connection:
            self._connection.commit()
            logger.info("Transação MySQL confirmada.")
        else:
            raise DatabaseError("Conexão com MySQL não está ativa para confirmar transação.")

    def rollback_transaction(self):
        """Desfaz a transação atual no MySQL."""
        if self._connection:
            self._connection.rollback()
            logger.info("Transação MySQL desfeita.")
        else:
            raise DatabaseError("Conexão com MySQL não está ativa para desfazer transação.")

    def get_placeholder(self) -> str:
        """Retorna o placeholder de parâmetro para MySQL."""
        return self._placeholder