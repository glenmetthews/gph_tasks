import struct

import psycopg2
import pytz as pytz
from pydantic import PostgresDsn
from lora_common.config import DBSettings

from datetime import datetime


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
                    query = "SELECT data, time from rawdata WHERE type = 'CONF_UP' and deveui = %s and data LIKE E'\x02%%' ORDER BY time DESC LIMIT 1"
                    cursor.execute(query, (device_id,))
                    record = cursor.fetchone()
                    package_type, vcc, vcc_p,\
                        unsent_messages, error_code, _, _, rs = struct.unpack("!bfbbbbbh", record[0][0:12])
                    service_message = {
                        'time': record[1],
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

    def update_service_data(self, device_id, last_package_time: str = 0):

        try:
            with psycopg2.connect(self.credentials) as conn:
                with conn.cursor() as cursor:
                    query = "SELECT data, time from rawdata WHERE type = 'CONF_UP' and deveui = %s and data LIKE E'\x02%%' and time > %s ORDER BY time DESC LIMIT 5"
                    cursor.execute(query, (device_id, int(last_package_time)))
                    record = cursor.fetchone()
                    if record is not None:
                        package_type, vcc, vcc_p, \
                            unsent_messages, error_code, _, _, rs = struct.unpack("!bfbbbbbh", record[0][0:12])
                        service_message = {
                            'time': record[1],
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

    def get_all_data_package_of_device(self, device_id):
        try:
            with psycopg2.connect(self.credentials) as conn:
                with conn.cursor() as cursor:
                    query = "SELECT data from rawdata WHERE type = 'CONF_UP' and deveui = %s ORDER BY time DESC"
                    cursor.execute(query, (device_id, ))
                    record = cursor.fetchall()

                    data_list = []
                    for row in record:

                        if row[0][0].hex() == '01':
                            if row[0][1].hex() == '01':
                                time_value = datetime.utcfromtimestamp(int(row[0][2:6].hex(), 16))
                                print(row.hex())
                                print(struct.unpack("!bbiffbb", row))
                                print(time_value)

                            if row[0][1].hex() == '02':
                                time_value = datetime.utcfromtimestamp(int(row[0][2:6].hex(), 16))
                                print(row.hex())
                                print(struct.unpack("!bbiffbbiffbb", row))
                                print(time_value)

                        if row[0][0].hex() == '06':
                            _, _, time, displacement, temperature, _, _ = struct.unpack("!bbiffbb", row[0][0:16])
                            data = {
                                'device_id': device_id,
                                'time': datetime.utcfromtimestamp(time),
                                'displacement': displacement,
                                'temperature': temperature
                            }
                        data_list.append(data)

                    return data_list

        except Exception as e:
            print(e)

    def get_last_disp_data(self, device_id):
        try:
            with psycopg2.connect(self.credentials) as conn:
                with conn.cursor() as cursor:
                    query = "SELECT data from rawdata WHERE type = 'CONF_UP' and deveui = %s and data LIKE E'\x06%%' ORDER BY time DESC LIMIT 1"
                    cursor.execute(query, (device_id,))
                    record = cursor.fetchone()[0]

                    _, _, time, displacement, temperature, _, _ = struct.unpack("!bbiffbb", record[0:16])
                    data = {
                        'device_id': device_id,
                        'time': time,
                        'displacement': displacement,
                        'temperature': temperature,
                        'replacement': 'Трещиномер'
                    }
                    return data

        except Exception as e:
            print(e)

if __name__ == '__main__':
    db_config = DBSettings()
    lora_db = LoraDB(credentials=db_config.pg_lora)

    data = lora_db.get_last_disp_data('0729331405303640')
    print(data['time'])
    timezone = pytz.timezone('Europe/Moscow')
    time_value = datetime.fromtimestamp(data['time'])
    time_with_tz = timezone.localize(time_value)
    print(time_with_tz)
    print(datetime.fromtimestamp(data['time']))
    # import csv
    #
    # with open('from_null.csv', 'a', newline='') as csvfile:
    #     spamwriter = csv.writer(csvfile, delimiter=';')
    #     spamwriter.writerow(['time', 'displacement', 'temp'])
    #
    #     for row in data:
    #
    #         spamwriter.writerow([row['time'], row['displacement'] - data[-1]['displacement'], row['temperature']])

    #print(lora_db.get_last_service_package('0729331405303640'))



