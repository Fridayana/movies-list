import psycopg2
import logging
import inspect
from psycopg2.extras import DictCursor, execute_batch
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import os
import configparser


def read_configs(config_name="app.properties"):
    CONFIG = configparser.ConfigParser()
    root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
    config_file = os.path.join(root_path, config_name)

    CONFIG.read(config_file)
    config_file = os.path.join(root_path, config_name)
    if os.path.exists(config_file):
        CONFIG.read(config_file)

        return CONFIG
    else:
        logging.error(f"File config not found: {config_file}")
        raise Exception(f"File config not found: {config_file}")


def db_connect(db_string, schemas="", connect_timeout=30, config_name="app.properties", auto_commit=True):
    config = read_configs(config_name=config_name)
    if config.has_section(db_string):
        list_values = dict(config.items(db_string))
        if schemas == '':
            connection = psycopg2.connect(host=list_values['host'] if 'host' in list_values else '',
                                          database=list_values['database'] if 'database' in list_values else '',
                                          user=list_values['user'] if 'user' in list_values else '',
                                          password=list_values['password'] if 'password' in list_values else '',
                                          port=list_values['port'] if 'port' in list_values else 5432,
                                          connect_timeout=connect_timeout)
            connection.autocommit = True
        else:
            connection = psycopg2.connect(host=list_values['host'] if 'host' in list_values else '',
                                          database=list_values['database'] if 'database' in list_values else '',
                                          user=list_values['user'] if 'user' in list_values else '',
                                          password=list_values['password'] if 'password' in list_values else '',
                                          port=list_values['port'] if 'port' in list_values else 5432,
                                          options=f"-c search_path={schemas}",
                                          connect_timeout=connect_timeout)
        if auto_commit:
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection
    else:
        logging.error(f"There is no section of: {db_string} found in app.properties")
        raise Exception(f"There is no section of: {db_string} found in app.properties")


def db_insert(db_curr, sql, params: tuple = None, list_data=None, page_size=10000):
    try:
        sql = f"--Data {inspect.stack()[1][1]} {inspect.stack()[1][3]}\n{sql}"
        logging.info(f"SQL: {sql}")
        logging.info(f"params: {params}")
        # Insert large volume, execute batch every 10k(default) of data
        if list_data and not params:
            execute_batch(db_curr, sql, list_data, page_size=page_size)
        # Only insert one line of data at the time
        elif not list_data and params:
            db_curr.execute(sql, params)
        else:
            pass
    except Exception as error:
        logging.error(f"db_insert() {repr(error)}")
        raise Exception(repr(error))
