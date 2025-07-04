import logging
from typing import Dict, Any, Type, Optional
from connectorDB import IDBConnector, DatabaseError

logger = logging.getLogger(__name__)

class DBConnectionManager:
    """
    Gerencia a criação e fornecimento de instâncias de conectores de banco de dados,
    abstraindo a escolha do SGBD específico.
    """

    _connectors: Dict[str, Type[IDBConnector]] = {} # Inicializa vazio

    # Adiciona os conectores ao dicionário somente se a importação for bem-sucedida
    # Isso é feito fora do __init__ ou de um método, para que seja executado uma vez na importação do módulo.
    try:
        from connector_firebird import FirebirdConnector
        _connectors["firebird"] = FirebirdConnector
    except ImportError:
        logging.warning("FirebirdConnector não pôde ser importado. Conexão Firebird não estará disponível.")

    try:
        from connector_mysql import MySQLConnector
        _connectors["mysql"] = MySQLConnector
    except ImportError:
        logging.warning("MySQLConnector não pôde ser importado. Conexão MySQL não estará disponível.")

    try:
        from connector_postgres import PostgreSQLConnector
        _connectors["postgres"] = PostgreSQLConnector
    except ImportError:
        logging.warning("PostgreSQLConnector não pôde ser importado. Conexão PostgreSQL não estará disponível.")


    @classmethod
    def register_connector(cls, db_type: str, connector_class: Type[IDBConnector]):
        """Registra um novo tipo de conector."""
        cls._connectors[db_type.lower()] = connector_class
        logger.info(f"Conector para '{db_type}' registrado com sucesso.")

    @classmethod
    def get_connector(cls, db_type: str, **db_config: Any) -> IDBConnector:
        """
        Retorna uma instância do conector de banco de dados solicitado.

        Args:
            db_type (str): O tipo de banco de dados ('firebird', 'mysql', 'postgres', etc.).
            **db_config: Argumentos de configuração para o conector (host, port, database, user, password, etc.).

        Returns:
            IDBConnector: Uma instância do conector de banco de dados.

        Raises:
            ValueError: Se o tipo de banco de dados não for suportado.
            DatabaseError: Se houver um problema ao conectar.
        """
        connector_class = cls._connectors.get(db_type.lower())
        
        # Agora, se connector_class for None, significa que ele não foi adicionado
        # ao dicionário, o que implica que a importação falhou ou o tipo não existe.
        if connector_class is None:
            raise ValueError(f"Tipo de banco de dados '{db_type}' não suportado ou conector não disponível. Conectores disponíveis: {list(cls._connectors.keys())}")
        
        try:
            connector = connector_class(**db_config)
            connector.connect()
            logger.info(f"Conexão com {db_type} estabelecida via DBConnectionManager.")
            return connector
        except Exception as e:
            logger.error(f"Falha ao obter e conectar o conector {db_type}: {e}")
            raise DatabaseError(f"Não foi possível conectar ao banco de dados {db_type}: {e}")

    @classmethod
    def close_connector(cls, connector: IDBConnector):
        """
        Fecha a conexão de um conector de banco de dados.
        """
        if connector:
            try:
                connector.disconnect()
                logger.info("Conexão do conector fechada via DBConnectionManager.")
            except Exception as e:
                logger.error(f"Erro ao fechar conexão do conector: {e}")