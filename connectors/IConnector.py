from abc import ABCMeta, abstractmethod
from distutils.version import LooseVersion
from models.Migration import Migration


class IConnector(metaclass=ABCMeta):
    """A Class interface for a database connector class"""

    configs = {}

    def set_configs(self, database_conf: dict):
        self.configs = database_conf

    @abstractmethod
    def connect(self):
        return NotImplemented

    @abstractmethod
    def disconnect(self):
        return NotImplemented

    @abstractmethod
    def initialize_database(self):
        return NotImplemented

    @abstractmethod
    def get_last_migration(self) -> Migration:
        return NotImplemented

    @abstractmethod
    def get_migration_by_version(self, version: LooseVersion) -> Migration:
        return NotImplemented

    @abstractmethod
    def migrate(self, migration: [Migration]) -> (bool, Migration, str):
        return NotImplemented
