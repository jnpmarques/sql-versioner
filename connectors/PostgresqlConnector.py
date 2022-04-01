import psycopg2
from distutils.version import LooseVersion

from connectors.IConnector import IConnector
from models.Migration import Migration


class PostgresqlConnector(IConnector):
    """A Postgresql Connectior Class"""

    def __init__(self):
        self.name = "PostgresqlConnector"

    def connect(self):
        conn_str = ""
        configs = self.configs
        for key in self.configs:
            conn_str = "{previous} {key}={value}".format(
                previous=conn_str,
                key=key,
                value=self.configs[key]
            )
        if conn_str:
            self.conn = psycopg2.connect(conn_str)
        else:
            return ConnectionError

    def disconnect(self):
        self.conn.close()

    def _get_conn(self):
        if self.conn is None:
            print("Not connected. Trying to connect")
            self.connect()

        return self.conn

    def _version_table_exists(self) -> bool:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT EXISTS ( "
                        "SELECT FROM "
                        "    information_schema.tables "
                        "WHERE "
                        "    table_schema LIKE 'public' AND "
                        "    table_type LIKE 'BASE TABLE' AND "
                        "   table_name = 'sql_versioner' "
                        ")")
            row = cur.fetchone()
            return row[0]
        finally:
            cur.close()

    def initialize_database(self):
        if self._version_table_exists():
            print("Versioning table detected. Skipping initialization.")

        print("Initializing database..")
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "CREATE TABLE sql_versioner ( "
                "install_order serial PRIMARY KEY, "
                "version varchar(50) NOT NULL, "
                "description varchar(200) NOT NULL, "
                "script varchar(1000) NOT NULL, "
                "checksum varchar(32) NOT NULL, "
                "installed_on timestamp NOT NULL DEFAULT now() "
                ")"
            )
            conn.commit()
        finally:
            cur.close()

        print("Finished database initialization. Ready to migrate.")

    def _row_to_migration(self, row) -> Migration:
        install_order = row[0]
        version = LooseVersion(row[1])
        description = row[2]
        script = row[3]
        checksum = row[4]
        installed_on = row[5]
        return Migration(install_order=install_order,
                         version=version,
                         description=description,
                         script=script,
                         checksum=checksum,
                         installed_on=installed_on)

    def get_last_migration(self) -> Migration:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT install_order "
                "       ,version "
                "       ,description "
                "       ,script "
                "       ,checksum "
                "       ,installed_on "
                "FROM sql_versioner "
                "order by install_order desc "
                "LIMIT 1 "
            )
            if cur.rowcount == 0:
                return None

            row = cur.fetchone()
            return self._row_to_migration(row)
        finally:
            cur.close()

    def get_migration_by_version(self, version: LooseVersion) -> Migration or None:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT install_order "
                "       ,version "
                "       ,description "
                "       ,script "
                "       ,checksum "
                "       ,installed_on "
                "FROM sql_versioner "
                "WHERE version = '{}' ".format(str(version))
            )
            if cur.rowcount == 0:
                return None

            row = cur.fetchone()
            return self._row_to_migration(row)
        finally:
            cur.close()

    def _migrate_script(self, script) -> (bool, str):
        with open(script, 'r') as s:
            sql = s.read()
            if sql is None:
                return False, "Script is empty."
            conn = self._get_conn()
            cur = conn.cursor()
            try:
                cur.execute(sql)
                conn.commit()
                return True, None
            except Exception as err:
                return False, err
            finally:
                cur.close()

    def migrate(self, migration: [Migration]) -> (bool, Migration, str):
        res, err = self._migrate_script(migration.script)
        if not res:
            return res, None, err

        conn = self._get_conn()
        cur = conn.cursor()

        sql = "INSERT INTO sql_versioner (version, description, script, checksum) VALUES ('{}','{}','{}','{}') " \
              "returning install_order, version, description, script, checksum, installed_on" \
            .format(str(migration.version), migration.description, migration.script, migration.checksum)
        try:
            cur.execute(sql)
            row = cur.fetchone()
            mig = self._row_to_migration(row)
            conn.commit()
            return True, mig, None
        except Exception as err:
            return False, None, err
        finally:
            cur.close()
