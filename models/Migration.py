from datetime import datetime
from distutils.version import LooseVersion


class Migration:
    def __init__(self, install_order: int = None, version: LooseVersion = None, description: str = None, script: str = None,
                 checksum: str = None, installed_on: datetime = None):
        self.install_order = install_order
        self.version = version
        self.description = description
        self.script = script
        self.checksum = checksum
        self.installed_on = installed_on

    def __repr__(self) -> str:
        return "Migration(" + (
                str(self.install_order) or "None") + ",'" + str(self.version) + ",'" + self.description + ",'" \
                    + self.checksum + ",'" + (
                       self.installed_on or "None") + "')"

    def __str__(self) -> str:
        return "Migration.version=" + (str(self.version) or "None")
