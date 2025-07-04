from pathlib import Path
import sys
import logging
from typing import Optional, Any, Dict, List, Union, Tuple

# Configuração do caminho para importar módulos de 'common/src'
# Ajuste conforme a estrutura final do seu projeto
current_dir = Path(__file__).parent
common_src_dir = current_dir / "common" / "src"
if str(common_src_dir) not in sys.path:
    sys.path.insert(0, str(common_src_dir))

from connectorDB import IDBConnector, DatabaseError
from connector_manager import DBConnectionManager # Novo import para gerenciar conexões

# Configuração de logger para a classe CRUD
logger = logging.getLogger(__name__)

class CRUD:
    """
    Classe para operações CRUD (Create, Read, Update, Delete) em um banco de dados,
    projetada para ser agnóstica ao tipo de banco de dados através da interface IDBConnector.

    Esta classe recebe uma instância de IDBConnector em sua inicialização,
    permitindo que ela opere sobre qualquer conexão de banco de dados
    gerenciada pelo DBConnectionManager.
    """
    def __init__(self, connector: IDBConnector):
        """
        Inicializa a classe CRUD com uma instância de um conector de banco de dados.

        Args:
            connector (IDBConnector): Uma instância de uma classe que implementa a interface IDBConnector,
                                     fornecendo métodos para execução de consultas e atualizações.
        """
        if not isinstance(connector, IDBConnector):
            raise TypeError("O conector fornecido deve ser uma instância de IDBConnector.")
        self.connector = connector
        self.placeholder = self.connector.get_placeholder() # Obtém o placeholder do conector
        logger.info("Instância CRUD inicializada com sucesso.")

    def create(self, table_name: str, data: Dict[str, Any]) -> int:
        """
        Insere um novo registro na tabela especificada.

        Args:
            table_name (str): O nome da tabela onde o registro será inserido.
            data (Dict[str, Any]): Um dicionário onde as chaves são os nomes das colunas
                                     e os valores são os dados a serem inseridos.

        Returns:
            int: O número de linhas afetadas pela operação de inserção.
        """
        if not data:
            logger.warning("Dados vazios fornecidos para operação CREATE. Nenhuma inserção será feita.")
            return 0

        columns = ", ".join(data.keys())
        placeholders = ", ".join([self.placeholder] * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        params = tuple(data.values())

        try:
            rows_affected = self.connector.execute_update(query, params)
            logger.info(f"CREATE: {rows_affected} linha(s) afetada(s) na tabela '{table_name}'.")
            return rows_affected
        except DatabaseError as e:
            logger.error(f"Erro no CREATE para '{table_name}': {e}")
            raise # Re-lança a exceção após o log

    def read(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> List[Union[Tuple[Any, ...], Dict[str, Any]]]:
        """
        Lê registros da tabela com base em condições opcionais.

        Args:
            table_name (str): O nome da tabela da qual os registros serão lidos.
            conditions (Optional[Dict[str, Any]]): Um dicionário opcional onde as chaves são os nomes das colunas
                                                     e os valores são as condições para filtrar os registros.

        Returns:
            List[Union[Tuple[Any, ...], Dict[str, Any]]]: Uma lista de tuplas ou dicionários,
                                                            representando os registros encontrados.
        """
        query = f"SELECT * FROM {table_name}"
        params: Optional[Tuple[Any, ...]] = None
        
        if conditions:
            condition_clauses = []
            param_values = []
            for col, val in conditions.items():
                condition_clauses.append(f"{col} = {self.placeholder}")
                param_values.append(val)
            query += " WHERE " + " AND ".join(condition_clauses)
            params = tuple(param_values)
        
        try:
            results = self.connector.execute_query(query, params)
            logger.info(f"READ: {len(results)} registro(s) lido(s) da tabela '{table_name}'.")
            return results
        except DatabaseError as e:
            logger.error(f"Erro no READ para '{table_name}': {e}")
            raise

    def update(self, table_name: str, data: Dict[str, Any], conditions: Dict[str, Any]) -> int:
        """
        Atualiza registros na tabela com base em condições.

        Args:
            table_name (str): O nome da tabela a ser atualizada.
            data (Dict[str, Any]): Um dicionário com os nomes das colunas e os novos valores a serem definidos.
            conditions (Dict[str, Any]): Um dicionário com as condições para identificar os registros a serem atualizados.

        Returns:
            int: O número de linhas afetadas pela operação de atualização.
        """
        if not data:
            logger.warning("Dados vazios fornecidos para operação UPDATE. Nenhuma atualização será feita.")
            return 0
        if not conditions:
            raise ValueError("Condições devem ser fornecidas para a operação UPDATE para evitar atualizações em massa não intencionais.")

        set_clauses = []
        param_values = []
        for col, val in data.items():
            set_clauses.append(f"{col} = {self.placeholder}")
            param_values.append(val)

        condition_clauses = []
        for col, val in conditions.items():
            condition_clauses.append(f"{col} = {self.placeholder}")
            param_values.append(val) # Adiciona os valores das condições aos parâmetros

        query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {' AND '.join(condition_clauses)}"
        
        try:
            rows_affected = self.connector.execute_update(query, tuple(param_values))
            logger.info(f"UPDATE: {rows_affected} linha(s) afetada(s) na tabela '{table_name}'.")
            return rows_affected
        except DatabaseError as e:
            logger.error(f"Erro no UPDATE para '{table_name}': {e}")
            raise

    def delete(self, table_name: str, conditions: Dict[str, Any]) -> int:
        """
        Deleta registros da tabela com base em condições.

        Args:
            table_name (str): O nome da tabela da qual os registros serão deletados.
            conditions (Dict[str, Any]): Um dicionário com as condições para identificar os registros a serem deletados.

        Returns:
            int: O número de linhas afetadas pela operação de exclusão.
        """
        if not conditions:
            raise ValueError("Condições devem ser fornecidas para a operação DELETE para evitar exclusões em massa não intencionais.")

        condition_clauses = []
        param_values = []
        for col, val in conditions.items():
            condition_clauses.append(f"{col} = {self.placeholder}")
            param_values.append(val)
        
        query = f"DELETE FROM {table_name} WHERE {(' AND '.join(condition_clauses))}"
        
        try:
            rows_affected = self.connector.execute_update(query, tuple(param_values))
            logger.info(f"DELETE: {rows_affected} linha(s) afetada(s) na tabela '{table_name}'.")
            return rows_affected
        except DatabaseError as e:
            logger.error(f"Erro no DELETE para '{table_name}': {e}")
            raise

    def begin_transaction(self):
        """
        Inicia uma transação no banco de dados.
        Delega ao conector subjacente.
        """
        try:
            self.connector.start_transaction()
            logger.info("Transação iniciada via conector.")
        except DatabaseError as e:
            logger.error(f"Erro ao iniciar transação: {e}")
            raise

    def commit(self):
        """
        Confirma a transação atual no banco de dados.
        Delega ao conector subjacente.
        """
        try:
            self.connector.commit_transaction()
            logger.info("Transação confirmada via conector.")
        except DatabaseError as e:
            logger.error(f"Erro ao confirmar transação: {e}")
            raise

    def rollback(self):
        """
        Desfaz (reverte) a transação atual no banco de dados.
        Delega ao conector subjacente.
        """
        try:
            self.connector.rollback_transaction()
            logger.info("Transação desfeita (rollback) via conector.")
        except DatabaseError as e:
            logger.error(f"Erro ao desfazer transação: {e}")
            raise