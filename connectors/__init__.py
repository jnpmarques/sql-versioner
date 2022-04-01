from connectors.IConnector import IConnector
from connectors.PostgresqlConnector import PostgresqlConnector


def get_connector(database_configs: dict) -> IConnector:
    "A static method to get a concrete product"
    connector_name = database_configs["connector"]

    connector: IConnector = None
    if connector_name == 'PostgresqlConnector':
        connector = PostgresqlConnector()

    if connector is not None:
        database_configs.pop("connector")
        connector.set_configs(database_configs)


    return connector
