import struct

import psycopg2
from pydantic import PostgresDsn
from lora_common.config import DBSettings

class LoraDB:

    def __init__(self, credentials: PostgresDsn):
        self.credentials = credentials

    def get_device_list(self) -> tuple:
        try:

            with psycopg2.connect(self.credentials) as conn:
                with conn.cursor() as cursor:
                    query = "SELECT deveui FROM devices"
                    cursor.execute(query)
                    records = cursor.fetchall()

            data = tuple(elem[0] for elem in records if elem[0] != '')
            return data

        except Exception as e:
            print(e)

    def get_last_record(self, device_id: str):
        try:
            with psycopg2.connect(self.credentials) as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * from rawdata WHERE type = 'CONF_UP' and deveui = %s ORDER BY time DESC"
                    cursor.execute(query, (device_id,))
                    data = cursor.fetchone()
                    return data

        except Exception as e:
            print(e)

    def get_last_service_package(self, device_id: str) -> dict:
        try:
            with psycopg2.connect(self.credentials) as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * from rawdata WHERE type = 'CONF_UP' and deveui = %s and data LIKE E'\x02%%' ORDER BY time DESC LIMIT 1"
                    cursor.execute(query, (device_id,))
                    record = cursor.fetchone()[0]
                    package_type, vcc, vcc_p,\
                        unsent_messages, error_code, _, _, rs = struct.unpack("!bfbbbbbh", record[0:12])
                    service_message = {
                        'package_type': package_type,
                        'vcc': vcc,
                        'vcc_p': vcc_p,
                        'unsent_messages': unsent_messages,
                        'error_code': error_code,
                        'rs': rs
                    }
                    return service_message

        except Exception as e:
            print(e)

    def update_service_data(self, device_id, last_package_time):
        try:
            with psycopg2.connect(self.credentials) as conn:
                with conn.cursor() as cursor:
                    query = "SELECT * from rawdata WHERE type = 'CONF_UP' and deveui = %s and data LIKE E'\x02%%' ORDER BY time DESC LIMIT 1"
                    cursor.execute(query, (device_id,))
                    record = cursor.fetchone()[0]
                    package_type, vcc, vcc_p, \
                        unsent_messages, error_code, _, _, rs = struct.unpack("!bfbbbbbh", record[0:12])
                    service_message = {
                        'package_type': package_type,
                        'vcc': vcc,
                        'vcc_p': vcc_p,
                        'unsent_messages': unsent_messages,
                        'error_code': error_code,
                        'rs': rs
                    }
                    return service_message

        except Exception as e:
            print(e)


if __name__ == '__main__':
    db_config = DBSettings()
    lora_db = LoraDB(credentials=db_config.pg_lora)

    lora_db.update_service_data()

