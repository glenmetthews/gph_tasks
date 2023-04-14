import psycopg2
from pydantic import PostgresDsn
from lora_common.config import DBSettings

from datetime import datetime

class GrafanaDB:

    def __init__(self, credentials: PostgresDsn):
        self.credentials = credentials

    def set_service_data(
            self,
            device_id: str,
            time: datetime,
            vcc: float,
            vcc_p: str,
            error_code: int,
            rs: float
    ) -> None:
        try:
            with psycopg2.connect(self.credentials) as conn:
                with conn.cursor() as cursor:
                    query = 'INSERT INTO "service_data" (time, device_id, vcc, vcc_p, error_code, rs) ' \
                            'VALUES (%s, %s, %s, %s, %s, %s)'
                    cursor.execute(query, (time, device_id, vcc, vcc_p, error_code, rs))
                    conn.commit()

        except Exception as e:
            print(e)

    def update_service_data(
            self,
            device_id: str,
            time: datetime,
            vcc: float,
            vcc_p: str,
            error_code: int,
            rs: float
    ) -> None:
        try:
            with psycopg2.connect(self.credentials) as conn:
                with conn.cursor() as cursor:
                    query = 'UPDATE "service_data" SET time = %s, vcc = %s, vcc_p = %s, error_code = %s, rs= %s WHERE device_id = %s'
                    cursor.execute(query, (time, vcc, vcc_p, error_code, rs, device_id))
                    conn.commit()

        except Exception as e:
            print(e)

    def write_disp_to_db(
            self,
            device_id,
            time,
            value,
            temp,
            replacement
    ):
        try:
            with psycopg2.connect(self.credentials) as conn:
                with conn.cursor() as cursor:
                    query = 'INSERT INTO "disp_data" (time, device_id, value, temp, replacement) ' \
                            'VALUES (%s, %s, %s, %s, %s)'
                    cursor.execute(query, (time, device_id, value, temp, replacement))
                    conn.commit()

        except Exception as e:
            print(e)