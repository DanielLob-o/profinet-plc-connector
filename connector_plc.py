import struct
import threading

import requests

from postgres import *

import snap7.client
from snap7.util import *
import os

API_ALARMS_HOST = os.getenv('API_ALARMS_HOST', '192.168.76.103')
API_ALARMS_PORT = os.getenv('API_ALARMS_PORT', '5000')
DEBUG = True if os.getenv('DEBUG', '') == 'True' else False
SOURCE_NODE = os.getenv('SOURCE_NODE', 1)


offsets = {"Bool": 2, "Int": 2, "Real": 4, "Dint": 4, "String": 256, "Char": 1, "Word": 2, "DWord": 4}
lock = threading.Lock()


def check_alarms(db_data, last_alarms):
    insert_alarms = False
    for alarms in db_data.items():
        try:
            if alarms[1] == last_alarms[alarms[0]]:
                pass
            else:
                insert_alarms = True
        except:
            insert_alarms = True
            continue
    return insert_alarms


def connector_factory(plc_info):
    plc_type = plc_info.pop('type')

    if plc_type.lower() == 'siemens':
        return ProfinetConnector({'address': plc_info['address'], 'rack': int(plc_info['rack']), 'slot': int(plc_info['slot'])})
    if plc_type.lower() == 'omron':
        return FINSConnector({'address': plc_info['address']})


class ProfinetConnector:
    # Freq in seconds
    if DEBUG:
        DBs = [{"name": "DB199_DBBasedeDatos", "num": 199, "freq": 15}]
    else:
        DBs = [{"name": "DB199_DBBasedeDatos", "num": 199, "freq": 15}]  # TODO Specify num and freq


    def __init__(self, plc_info):
        self.PLC_INFO = plc_info
        self.plc = None

    def read_db(self, db_num, length, dbitems):
        data = self.plc.read_area(snap7.types.Areas.DB, db_num, 0, length)
        lectura = {}
        for item in dbitems:
            value = None
            offset = int(item['offset'])
            if item['type'] == 'Real':
                value = get_real(data, offset)
            if item['type'] == 'Bool':
                bit = round(item['offset'] % 1 * 10)
                value = get_bool(data, offset, bit)
            if item['type'] == 'Int':
                value = get_int(data, offset)
            if item['type'] == 'Dint':
                value = get_dint(data, offset)
            if item['type'] == 'Char':
                # value = get_byte(data, offset)
                value = get_string(data, offset, 1)
            if item['type'] == 'String':
                value = get_string(data, offset, 255)
            if item['type'] == 'Word':
                value = get_word(data, offset)
            if item['type'] == 'DWord':
                value = get_dword(data, offset)
            lectura[item['metric_id']] = value
        return lectura

    def process_db(self, db):
        # Connect to the PLC if connection got closed
        with lock:
            try:
                if not self.get_connected():
                    logging.warning(f"Attempting to reconnect to PLC ({db['name']})")
                    self.connect()
                    logging.info(f"Reconnected to PLC ({db['name']})")
            except:
                time.sleep(10)
                return

        insert = True
        enables = {}
        try:
            # Read Enable Db

            # If enable variable of Db is false, do not read this DB
            if DEBUG or True:
                logging.info(f"Trying to get db size")
                items_db = read_db_desc("dms_plcs_info", type='siemens')
                size_db = self.get_db_size(items_db)
                logging.info(f"db size correctly read now getting data")
                logging.info(f"Reading DB {db['name']}")
                db_data = self.read_db(db['num'], size_db, items_db)

                logging.info(f"Inserting data to DB: metric_numeric_data")

                insert_db_data(db['name'], db_data, items_db)
                logging.info(f"Insert ({db['name']})")

        except Exception as e:
            if self.plc and self.get_connected():
                self.disconnect()
                logging.warning(f"Disconnected from PLC ({db['name']})")
            raise

    def write_var(self, variable, value):
        return False  # TODO Implemen

    def connect(self):
        self.plc = None
        try:
            self.plc = snap7.client.Client()
            self.plc.connect(**self.PLC_INFO)
        except Exception as e:
            if self.plc:
                self.plc.disconnect()
            raise

    def disconnect(self):
        self.plc.disconnect()

    def get_connected(self):
        return self.plc.get_connected()

    def get_db_size(self, array):
        seq, length = [float(x['offset']) for x in array], [x['type'] for x in array]
        idx = seq.index(max(seq))
        full_byte = 1
        for data in length[len(length) - 8:len(length)]:
            if data != 'Bool':
                full_byte = 0
        lastByte = int(str(max(seq)).split('.')[0]) + (offsets[length[idx]]) - full_byte
        return lastByte


class FINSConnector:
    """
    Connector for reading data from Omron PLCs using the FINS protocol.

    Note: data in an Omron PLC is not organized in DBs, but we treat it as so for consistency.
    """
    # Freq in seconds
    if DEBUG:
        DBs = [{"name": "db_alarmas", "num": 105, "freq": 2}, {"name": "db_comunicaciones", "num": 107, "freq": 5}]
    else:
        DBs = [{"name": "db_alarmas", "num": 105, "freq": 2}, {"name": "db_comunicaciones", "num": 107, "freq": 5},
               {"name": "db_contadores_desgastes", "num": 108, "freq": 3600},
               {"name": "db_general_graficar", "num": 104, "freq": 2},
               {"name": "db_general_visualizar", "num": 109, "freq": 5},
               {"name": "db_servos_graficar", "num": 106, "freq": 10},
               {"name": "db_servos_visualizar", "num": 110, "freq": 5},
               {"name": "db_alarmas_custom", "num": 111, "freq": 60}]  # TODO Specify num and freq

    # DBs = [{"name": "db_servos_visualizar", "num": 110, "freq": 5}]
    enable_DB = {"num": 111, "size": 2, "items": [{"variable": "enable_alarmas", "offset": 'D25700.00', "type": "Bool"},
                                                  {"variable": "enable_comunicaciones", "offset": 'D25700.01', "type": "Bool"},
                                                  {"variable": "enable_contadores_desgastes", "offset": 'D25700.02',
                                                   "type": "Bool"},
                                                  {"variable": "enable_general_graficar", "offset": 'D25700.03',
                                                   "type": "Bool"},
                                                  {"variable": "enable_general_visualizar", "offset": 'D25700.04',
                                                   "type": "Bool"},
                                                  {"variable": "enable_servos_graficar", "offset": 'D25700.05', "type": "Bool"},
                                                  {"variable": "enable_servos_visualizar", "offset": 'D25700.06',
                                                   "type": "Bool"},
                                                  {"variable": "enable_alarmas_custom", "offset": 'D25700.00',
                                                   "type": "Bool"}]}

    def __init__(self, PLC_INFO):
        self.PLC_INFO = PLC_INFO
        self.plc = None
        self._connected = False

    def connect(self):
        self.plc = fins.udp.UDPFinsConnection()
        self.plc.connect(self.PLC_INFO['address'])
        self.plc.dest_node_add = int(self.PLC_INFO['address'].split('.')[-1])
        self.plc.srce_node_add = int(SOURCE_NODE)
        self._connected = True

    def read_db(self, dbitems):
        memory_area_codes = []
        memory_area_addresses = []

        ma = fins.FinsPLCMemoryAreas()
        format_string = '>'
        headers_type_bytes = []

        for item in dbitems:
            if not item['type'] or not item['offset']:
                continue

            skip_char = 1
            num_codes = 2 if item['type'].upper() in ['DINT', 'REAL', 'UDINT'] else 1
            if 'STRING' in item['type'].upper():
                size = re.search('string\((\\d*)\)', item['type'].upper(), re.IGNORECASE).group(1)
                num_codes = int(size) // 2 if size else 1


            # Memory codes
            if item['type'].upper() == 'BOOL':
                if item['offset'][0].isdigit():
                    memory_area_codes.append(ma.CIO_BIT)
                    skip_char = 0
                if item['offset'][0:2] == 'DM':
                    memory_area_codes.append(ma.DATA_MEMORY_BIT)
                    skip_char = 2
                if item['offset'][0] == 'D':
                    memory_area_codes.append(ma.DATA_MEMORY_BIT)
                if item['offset'][0] == 'W':
                    memory_area_codes.append(ma.WORK_BIT)
                if item['offset'][0] == 'H':
                    memory_area_codes.append(ma.HOLDING_BIT)
                if item['offset'][0] == 'A':
                    memory_area_codes.append(ma.AUXILIARY_BIT)
                if item['offset'][0] == 'T':
                    memory_area_codes.append(ma.TIMER_FLAG)
                if item['offset'][0] == 'C':
                    memory_area_codes.append(ma.COUNTER_FLAG)
            else:
                for i in range(0, num_codes):
                    if item['offset'][0].isdigit():
                        memory_area_codes.append(ma.CIO_WORD)
                        skip_char = 0
                    if item['offset'][0] == 'D':
                        memory_area_codes.append(ma.DATA_MEMORY_WORD)
                    if item['offset'][0] == 'W':
                        memory_area_codes.append(ma.WORK_WORD)
                    if item['offset'][0] == 'H':
                        memory_area_codes.append(ma.HOLDING_WORD)
                    if item['offset'][0] == 'A':
                        memory_area_codes.append(ma.AUXILIARY_WORD)
                    if item['offset'][0] == 'T':
                        memory_area_codes.append(ma.TIMER_WORD)
                    if item['offset'][0] == 'C':
                        memory_area_codes.append(ma.COUNTER_WORD)

            # Get address in bytes
            address = list(map(lambda x: int(x), item['offset'][skip_char:].split('.')))
            if len(address) == 1:
                address.append(0)
            address_format = '>hb'
            if item['type'].upper() in ['DINT', 'REAL', 'UDINT']:
                memory_area_addresses.append(bytearray(struct.pack(address_format, *address)))
                address[0] = address[0] + 1
                memory_area_addresses.append(bytearray(struct.pack(address_format, *address)))
            elif 'STRING' in item['type'].upper():
                size = re.search('string\((\\d*)\)', item['type'].upper(), re.IGNORECASE).group(1)
                for i in range(int(size) // 2):
                    memory_area_addresses.append(bytearray(struct.pack(address_format, *address)))
                    address[0] = address[0] + 1
            else:
                #a = bytearray(struct.pack(address_format, *address))
                memory_area_addresses.append(bytearray(struct.pack(address_format, *address)))

            # Quedan algunos tipos que no sé si hace falta contemplar: Data Register, Index Register,
            # Task Flag, EM Area, Clock Pulses, Condition Flags, etc.

            # TODO Rellenar esto. UInt del PLC es H (unsigned short)
            # Data type formatting
            if item['type'].upper() == 'BOOL':  # Initial x corresponds to Memory Area Code pad byte
                format_string += '?'
                headers_type_bytes.append(1)
            if item['type'].upper() == 'DINT':
                #format_string += 'xh'
                format_string += 'i'
                headers_type_bytes.append(2)
                headers_type_bytes.append(2)
            if item['type'].upper() == 'UDINT':
                #format_string += 'xh'
                format_string += 'I'
                headers_type_bytes.append(2)
                headers_type_bytes.append(2)
            if item['type'].upper() == 'INT':
                format_string += 'h'
                headers_type_bytes.append(2)
            if item['type'].upper() == 'UINT_BCD':
                format_string += 'H'
                headers_type_bytes.append(2)
            if item['type'].upper() == 'REAL':
                #format_string += 'xe'
                format_string += 'f'
                headers_type_bytes.append(2)
                headers_type_bytes.append(2)
            if 'STRING' in item['type'].upper():  # TODO Check string length?
                size = re.search('string\((\\d*)\)', item['type'].upper(), re.IGNORECASE).group(1)
                if not size:
                    size = '2'
                format_string += f'{size}s'
                for i in range(int(size)//2):
                    headers_type_bytes.append(2)
            if item['type'].upper() == '???':
                format_string += ''

        data_bytes = bytes()
        chunk_size = 300
        for i in range(0, len(memory_area_addresses), chunk_size):
            address_batch = memory_area_addresses[i:i + chunk_size]
            codes_batch = memory_area_codes[i:i + chunk_size]

            response = self.plc.multiple_memory_area_read(codes_batch, address_batch)

            # Look for x01, x04 on bytes 11-12 (request type "multiple area read")
            # Look for x00, x00 on bytes 13-14 (response code OK)
            # Actual data in bytes 16-17, 19-20, 22-23, (...)
            assert self.validate_read_packet(response)

            data_bytes += response[14:]  # Remove headers
            # data_bytes += self.extract_read_data(response)
        data_bytes = bytearray(data_bytes)
        # for i in range(len(data_bytes)):
        #     if i+1 == 1 or (i+1)%3 == 0:
        #         data_bytes.pop(i)
        j = 0
        for i in headers_type_bytes:
            data_bytes.pop(j)
            j = j + i
        data_list = []
        for item in dbitems:
            if item['type'].upper() == 'BOOL':
                data_list.append(struct.unpack('>?', data_bytes[0].to_bytes(1,'big'))[0])
                data_bytes.pop(0)
            if item['type'].upper() == 'DINT':
                order_bytes = data_bytes[2].to_bytes(1,'big') + data_bytes[3].to_bytes(1,'big') + data_bytes[0].to_bytes(1,'big') + data_bytes[1].to_bytes(1,'big')
                data_list.append(struct.unpack('>i', order_bytes)[0])
                data_bytes.pop(0)
                data_bytes.pop(0)
                data_bytes.pop(0)
                data_bytes.pop(0)
            if item['type'].upper() == 'UDINT':
                order_bytes = data_bytes[2].to_bytes(1,'big') + data_bytes[3].to_bytes(1,'big') + data_bytes[0].to_bytes(1,'big') + data_bytes[1].to_bytes(1,'big')
                data_list.append(struct.unpack('>I', order_bytes)[0])
                data_bytes.pop(0)
                data_bytes.pop(0)
                data_bytes.pop(0)
                data_bytes.pop(0)
            if item['type'].upper() == 'INT':
                data_list.append(struct.unpack('>h', data_bytes[0].to_bytes(1,'big')+data_bytes[1].to_bytes(1,'big'))[0])
                data_bytes.pop(0)
                data_bytes.pop(0)
            if item['type'].upper() == 'UINT_BCD':
                dec = struct.unpack('>H', data_bytes[0].to_bytes(1, 'big') + data_bytes[1].to_bytes(1, 'big'))[0]
                data_list.append(int(hex(dec)[2:]))
                data_bytes.pop(0)
                data_bytes.pop(0)
            if item['type'].upper() == 'REAL':
                order_bytes = data_bytes[2].to_bytes(1,'big') + data_bytes[3].to_bytes(1,'big') + data_bytes[0].to_bytes(1,'big') + data_bytes[1].to_bytes(1,'big')
                data_list.append(struct.unpack('>f', order_bytes)[0])
                data_bytes.pop(0)
                data_bytes.pop(0)
                data_bytes.pop(0)
                data_bytes.pop(0)
            if 'STRING' in item['type'].upper():
                size = re.search('string\((\\d*)\)', item['type'].upper(), re.IGNORECASE).group(1)
                if not size:
                    size = '2'
                data_string = bytes()
                # data_list.append(struct.unpack(f'>{size}s', data_bytes[0].to_bytes(1,'big')+data_bytes[1].to_bytes(1,'big'))[0])
                for i in range(int(size)):
                    data_string += struct.unpack(f'>1s', data_bytes[0].to_bytes(1,'big'))[0]
                    data_bytes.pop(0)
                data_list.append(data_string)
        #data_list = struct.unpack(format_string, data_bytes)


        lectura = {}
        for item, data in zip(dbitems, data_list):  # TODO Test
            if type(data) == bytes:
                lectura[item['variable']] = data.decode('utf-8')
            else:
                lectura[item['variable']] = data

        return lectura

    def process_db(self, db):
        insert = True
        enables = {}
        try:
            # Read Enable Db
            with lock:
                enables = self.read_db(self.enable_DB['items'])

            # If enable variable of Db is false, do not read this DB
            if DEBUG or enables['enable_' + db['name'].replace('db_', '')]:
                items_db = read_db_desc(db['name'], type='omron')

                with lock:
                    logging.info(f"Reading DB {db['name']}")
                    db_data = self.read_db(items_db)

                if db['name'] == 'db_alarmas':
                    last_alarms = get_last_alarms(items_db)
                    if last_alarms:
                        insert = check_alarms(db_data, last_alarms)

                if insert:
                    create_db_table(db['name'], items_db)
                    insert_db_data(db['name'], db_data, items_db)
                    logging.info(f"Insert ({db['name']})")
                insert = True

                if db['name'] == 'db_contadores_desgastes':
                    if get_new_set_points():
                        try:
                            logging.info('Trying to update Grafana Counters')
                            requests.post(url=f'http://{API_ALARMS_HOST}:{API_ALARMS_PORT}/update_counters')
                            logging.info('Updated Grafana Counters')
                        except Exception as e:
                            logging.exception(e)

                if db['name'] == 'db_alarmas_custom':  # TODO Test
                    if get_new_custom_set_points():
                        try:
                            requests.post(url=f'http://{API_ALARMS_HOST}:{API_ALARMS_PORT}/update_custom_alarms')
                            logging.info('Updated Grafana Custom Alarms')
                        except Exception as e:
                            logging.exception(e)
        except Exception:
            raise

    def write_var(self, variable, value):
        try:
            offset = read_offset('db_contadores_desgastes', variable, type='omron')
            if not offset:
                return False
            logging.info(f'The offset of the variable is: {offset}')

            ma = fins.FinsPLCMemoryAreas()

            if offset[0].isdigit():
                memory_area_code = ma.CIO_WORD
            elif offset[0] == 'D':
                memory_area_code = ma.DATA_MEMORY_WORD
            elif offset[0] == 'W':
                memory_area_code = ma.WORK_WORD
            elif offset[0] == 'H':
                memory_area_code = ma.HOLDING_WORD
            elif offset[0] == 'A':
                memory_area_code = ma.AUXILIARY_WORD
            elif offset[0] == 'T':
                memory_area_code = ma.TIMER_WORD
            elif offset[0] == 'C':
                memory_area_code = ma.COUNTER_WORD
            else:
                return False

            # Get address in bytes
            address = list(map(lambda x: int(x), offset[1:].split('.')))
            if len(address) == 1:
                address.append(0)
            address_format = '>hb'
            arr = bytearray(struct.pack(address_format, *address))

            logging.info(f'Writing new value')
            self.plc.memory_area_write(memory_area_code, arr, value, 1)
            return True
        except:
            traceback.print_exc()
            raise

    def get_connected(self):
        return self._connected

    @staticmethod
    def validate_read_packet(packet):
        if not packet:
            return False
        if packet[10:12] != b'\x01\x04':  # Incorrect request type
            return False
        if packet[12:14] != b'\x00\x00':  # Bad response code
            return False
        return True

    @staticmethod
    def extract_read_data(packet):
        data = bytes()
        for i in range(15, len(packet), 3):
            value = packet[i:i + 2]
            data += value

        return data
