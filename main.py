import logging
import os
import traceback
import threading
import time
from configparser import ConfigParser
from configobj import ConfigObj
from connector_plc import connector_factory
import config as conf

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

API_ALARMS_HOST = os.getenv('API_ALARMS_HOST', '192.168.75.105')
API_ALARMS_PORT = os.getenv('API_ALARMS_PORT', '5000')
DEBUG = True if os.getenv('DEBUG', '') == 'True' else False

SLEEP_TIME = 2  # TODO Calculate this better

lock = threading.Lock()


def process_db_loop(db):
    while True:
        try:
            conf.plc.process_db(db)
        except Exception as e:
            logging.exception(e)
        finally:
            logging.info(F"Sleeping now")
            time.sleep(db['freq'])


def main():
    # !!!!!!! CONFIGURAR FINS/UDP CON CONVERSIÓN AUTOMÁTICA (DINÁMICA), DIRECCIÓN IP DE DESTINO SE CAMBIA
    # DINÁMICAMENTE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    config = ConfigObj('plc_info.ini')
    clear_dbs = []

    for plc_tag in config:
        plc_info = config[plc_tag]

        if not plc_info.as_bool('enabled'):
            continue

        conf.plc = connector_factory(plc_info)

        while conf.plc.plc is None:
            try:
                logging.info(f"Attempting to connect to PLC")
                conf.plc.connect()
            except:
                traceback.print_exc()
                time.sleep(10)
                continue
        logging.info(f"Connection to {plc_info['address']} is ready to start")

        process_db_loop(conf.plc.DBs[0])


if __name__ == "__main__":
    main()
