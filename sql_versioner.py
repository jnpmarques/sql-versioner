#!/usr/bin/env python3

import click
import yaml
import os
import hashlib
from connectors import get_connector
from connectors.IConnector import IConnector
from models.Migration import Migration
from distutils.version import LooseVersion


def _create_checksum(file: str):
    md5 = hashlib.md5()
    with open(file, 'rb') as f:
        data = f.read()
        md5.update(data)
    return md5.hexdigest()


def _order_by_version(migrations: [Migration]):
    return sorted(migrations, key=lambda m: m.version)


def _path_to_migration(path: str, file_path: str) -> Migration:
    script = os.path.join(path, file_path)
    parts = file_path.lower().split("__")
    version = LooseVersion(parts[0].replace("v", "").replace("_", "."))
    description = parts[1].replace(".sql", "").replace("_", " ")
    checksum = _create_checksum(script)
    migration = Migration(version=version, description=description, script=script, checksum=checksum)
    return migration


def _list_migration_files(path: str) -> [Migration]:
    scripts = [script for script in os.listdir(path) if script.endswith(".sql") and script.lower().startswith("v")]
    avlb_migr = []
    for script in scripts:
        avlb_migr.append(_path_to_migration(path, script))
    avlb_migr = _order_by_version(avlb_migr)
    return avlb_migr


def _migrations_are_valid(connector: IConnector, avlb_migr: [Migration]) -> (bool, Migration, Migration):
    for mig in avlb_migr:
        bd_mig = connector.get_migration_by_version(mig.version)
        if bd_mig is None:
            continue
        if bd_mig.checksum != mig.checksum:
            return False, mig, bd_mig

    return True, None, None


def _clean_old_migrations(avlb_migr: [Migration], last_version: Migration):
    if last_version is None:
        return avlb_migr

    return [mig for mig in avlb_migr if mig.version > last_version.version]


def _migrate(connector: [IConnector], avlb_migr: [Migration]):
    last_migration = connector.get_last_migration()
    new_migs = _clean_old_migrations(avlb_migr, last_migration)
    for mig in new_migs:
        success, new_mig, err = connector.migrate(mig)
        if not success:
            print("Migration with version {} failed. Error: \n {}".format(str(mig.version), err))
            break


@click.command()
@click.option("-c", "--config", default="./config.yml", type=click.STRING)
@click.option("--initialize_migrations", is_flag=True)
@click.option("-m", "--migrate", is_flag=True)
def sqlversioner(config: str, initialize_migrations: bool, migrate: bool):
    configs = None
    with open(config) as file:
        try:
            configs = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)

    connector = get_connector(configs["database"])
    try:
        connector.connect()
        if initialize_migrations:
            connector.initialize_database()
            return

        migrations_config = configs["migrations"]
        if migrate:
            avlb_migr = _list_migration_files(migrations_config["path"])
            mig_validation = _migrations_are_valid(connector, avlb_migr)
            if not mig_validation[0]:
                print("Migration with version {} checksums does not match. BD CheckSum: {} -- New Checksum: {}"
                      .format(str(mig_validation[1].version), mig_validation[2].checksum, mig_validation[1].checksum))
                return
            mig_result = _migrate(connector, avlb_migr)

    finally:
        connector.disconnect()


if __name__ == '__main__':
    sqlversioner()
