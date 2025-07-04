import firebird.driver
import logging
from typing import Optional, Any, List, Tuple, Dict, Union
from connectorDB import IDBConnector, DatabaseError 

logger = logging.getLogger(__name__)

class FirebirdConnector(IDBConnector):
    """
    Implementação da interface IDBConnector para Firebird.
    Utiliza a biblioteca `firebird-driver`.
    """
    def __init__(self, host: str, port: int, database: str, user: str, password: str, charset: str = 'UTF8', **kwargs):
        # Chama o construtor da classe base para inicializar _host, _port, _database, _user, _password
        super().__init__(host=host, port=port, database=database, user=user, password=password, **kwargs)
        self._charset = charset
        self._connection: Optional[firebird.driver.Connection] = None
        self._cursor: Optional[firebird.driver.Cursor] = None
        self._placeholder = "?" # Firebird usa '?' como placeholder

    def connect(self):
        """Estabelece a conexão com o banco de dados Firebird."""
        try:
            # firebird-driver espera os parâmetros de conexão como argumentos nomeados
            self._connection = firebird.driver.connect(
                host=self._host,         # self._host agora é acessível após super().__init__
                port=self._port,         # self._port agora é acessível
                database=self._database, # self._database agora é acessível
                user=self._user,         # self._user agora é acessível
                password=self._password, # self._password agora é acessível
                charset=self._charset
            )
            # Para o aviso de tipo em execute_query, se você quiser retornar dicionários,
            # você precisaria de um cursor_factory. Por padrão, ele retorna tuplas.
            # self._cursor = self._connection.cursor(cursor_factory=firebird.driver.DictCursor)
            self._cursor = self._connection.cursor()
            logger.info("Conexão com Firebird estabelecida usando firebird-driver.")
        except firebird.driver.Error as e:
            logger.error(f"Erro ao conectar ao Firebird: {e}")
            raise DatabaseError(f"Erro ao conectar ao Firebird: {e}") from e

    def disconnect(self):
        """Fecha a conexão com o banco de dados Firebird."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Conexão com Firebird fechada.")

    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Tuple[Any, ...]]:
        """Executa uma consulta SELECT no Firebird."""
        if not self._connection or not self._cursor:
            raise DatabaseError("Conexão com Firebird não está ativa.")
        try:
            self._cursor.execute(query, params or ())
            # Alterado o tipo de retorno para ser mais específico, pois o padrão é tuplas.
            return self._cursor.fetchall()
        except firebird.driver.Error as e:
            # A rollback é segura mesmo se não houver transação ativa, mas é boa prática ter um contexto transacional
            if self._connection:
                self._connection.rollback()
            logger.error(f"Erro ao executar consulta Firebird: {e} - Query: {query} - Params: {params}")
            raise DatabaseError(f"Erro ao executar consulta Firebird: {e}") from e

    def execute_update(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> int:
        """Executa uma consulta de atualização no Firebird."""
        if not self._connection or not self._cursor:
            raise DatabaseError("Conexão com Firebird não está ativa.")
        try:
            self._cursor.execute(query, params or ())
            rows_affected = self._cursor.rowcount
            return rows_affected
        except firebird.driver.Error as e:
            if self._connection:
                self._connection.rollback()
            logger.error(f"Erro ao executar atualização Firebird: {e} - Query: {query} - Params: {params}")
            raise DatabaseError(f"Erro ao executar atualização Firebird: {e}") from e

    def get_last_insert_id(self) -> Optional[Any]:
        """
        Retorna o ID da última linha inserida para Firebird.
        Firebird geralmente usa triggers ou generators para IDs.
        Este método pode precisar ser adaptado dependendo de como o ID é gerado.
        Para uma abordagem genérica, pode-se usar uma query `SELECT GEN_ID(sequence_name, 0) FROM RDB$DATABASE`
        ou `RETURNING ID` na instrução INSERT.
        Por simplicidade, retornaremos None e o usuário deve buscar o ID após a inserção se necessário.
        """
        logger.warning("get_last_insert_id não é diretamente suportado de forma universal para Firebird sem informações adicionais sobre a geração de IDs. Considere usar `RETURNING <COLUNA_ID>` na sua instrução INSERT.")
        return None

    def start_transaction(self):
        """Inicia uma transação no Firebird."""
        if self._connection:
            # O firebird-driver gerencia transações automaticamente por padrão.
            # Cada execute() está em sua própria transação se não houver uma transação explícita.
            # Para controle explícito, você pode usar:
            # self._connection.transaction.begin()
            # No entanto, a forma mais comum é simplesmente confiar no commit/rollback da conexão.
            logger.info("Iniciando transação Firebird (gerenciamento implícito pelo driver ou explícito via commit/rollback).")
        else:
            raise DatabaseError("Conexão com Firebird não está ativa para iniciar transação.")

    def commit_transaction(self):
        """Confirma a transação atual no Firebird."""
        if self._connection:
            self._connection.commit()
            logger.info("Transação Firebird confirmada.")
        else:
            raise DatabaseError("Conexão com Firebird não está ativa para confirmar transação.")

    def rollback_transaction(self):
        """Desfaz a transação atual no Firebird."""
        if self._connection:
            self._connection.rollback()
            logger.info("Transação Firebird desfeita.")
        else:
            raise DatabaseError("Conexão com Firebird não está ativa para desfazer transação.")

    def get_placeholder(self) -> str:
        """Retorna o placeholder de parâmetro para Firebird."""
        return self._placeholder