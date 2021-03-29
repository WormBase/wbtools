import configparser
import os


def read_config_file(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config


def read_db_config():
    return read_config_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "local_config", "db.cfg"))


def read_tpc_config():
    return read_config_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "local_config",
                                         "textpresso.cfg"))


def read_tazendra_config():
    return read_config_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "local_config",
                                         "tazendra.cfg"))
