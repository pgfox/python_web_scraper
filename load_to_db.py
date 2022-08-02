"""
Used to load Zurich Film Festival data into Maria DB. It looks for 'db_user' and 'db_password' in environment variables
first. If not found it will prompt the user to enter username and password. It also asks the user, if it should replace
the data if it exists already in the DB.

DB_NAME and DB_HOST_NAME and ZFF_TABLE_NAME are set as constants in the module

"""
import os
import sys
from sqlalchemy import create_engine
import pandas as pd
import logging
import getpass
import pymysql

import movie_cleaner as mc

DB_NAME = "my_db_name"
DB_HOST_NAME = "127.0.0.1"
ZFF_STAGE_TABLE = "zff_movies_stage"
ZFF_SRC_TABLE = "zff_movies_src"
ZFF_SRC_DIRTY_TABLE = "zff_movies_src_dirty"

UPDATE_REPLACE = "replace"
UPDATE_FAIL = "fail"

VALID_RESPONSE = ["Y", 'y', "N", 'n']


# initialise a logger for the module.
def init_logger():
    my_logger = logging.getLogger("load_to_db")
    my_logger.setLevel(logging.DEBUG)

    # log to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(name)s — %(message)s"))

    # log to file
    file_handler = logging.FileHandler('./logs/load_to_db.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(name)s — %(message)s"))

    my_logger.addHandler(console_handler)
    my_logger.addHandler(file_handler)
    return my_logger


LOG = init_logger()


def create_db_connection_str() -> str:
    """
    retrieves password and username for DB - first tries looking up 'db_user' and 'db_password' from env and it
    not found prompts user for input.

    :return: url that is used for db connection
    """
    # try to get username and password from the env
    username = os.environ.get("db_user")
    password = os.environ.get("db_password")

    if username is None:
        username = input("Enter DB username:")

    if password is None:
        password = getpass.getpass(prompt="Enter DB password:")

    return "mysql+pymysql://" + username + ":" + password + "@" + DB_HOST_NAME + "/" + DB_NAME


def get_update_behaviour() -> str:
    """
    prompts the user - do they want to replace existing data in DB or replace it.
    :return :string
    """
    user_input = input("Replace if table already exists (Y/N)?")

    while user_input not in VALID_RESPONSE:
        user_input = input("Please enter Y or N:")

    if user_input.lower() == "y":
        return UPDATE_REPLACE
    else:
        return UPDATE_FAIL


def load_file_to_db(db_connection, file_name: str, table_name: str, update_behaviour=UPDATE_FAIL) -> None:
    """
    load data from csv file and write to db

    :param db_connection: engine created by sqlalchemy
    :param file_name: name of file to write to DB
    :param table_name: name of table in DB to populate
    :param update_behaviour: should the existing data be overwriten 'replace' or not overwritten 'fail' . defdult
    value is 'fail' (do not replace)
    :return:
    """
    zff_movie_df = pd.read_csv(file_name)

    LOG.info(zff_movie_df.info())
    LOG.info(f"loaded data from {file_name} to {table_name}")

    try:
        zff_movie_df.to_sql(table_name, con=db_connection, if_exists=update_behaviour)
    except ValueError as ex:
        LOG.info(f"writing dataframe to DB resulted in following exception: {ex}")


def load_to_db() -> None:
    """
    reads the data from _stage.csv and loads it into the DB
    :return: None
    """
    db_connection_str = create_db_connection_str()
    db_connection = create_engine(db_connection_str)

    update_behaviour = get_update_behaviour()
    try:
        #load each file
        load_file_to_db(db_connection, mc.STAGE_FILENAME, ZFF_STAGE_TABLE, update_behaviour)
        load_file_to_db(db_connection, mc.SRC_FILENAME, ZFF_SRC_TABLE, update_behaviour)
        load_file_to_db(db_connection, mc.SRC_DIRTY_FILENAME, ZFF_SRC_DIRTY_TABLE, update_behaviour)

    finally:
        # make sure connections are closed
        db_connection.dispose()


def read_from_db() -> None:
    """
    reads the zff data from DB and displays the head and dataframe info
    :return: None
    """
    db_connection_str = create_db_connection_str()
    db_connection = create_engine(db_connection_str)

    try:
        db_zff_movies_df = pd.read_sql('SELECT * FROM ' + ZFF_STAGE_TABLE, con=db_connection)

        LOG.info(f"head of dataframe read in \n{db_zff_movies_df.head(n=10)}")
        LOG.info(f"info on dataframe \n{db_zff_movies_df.info()}")
    finally:
        # make sure connections are closed
        db_connection.dispose()


if __name__ == '__main__':
    load_to_db()
    # read_from_db()
